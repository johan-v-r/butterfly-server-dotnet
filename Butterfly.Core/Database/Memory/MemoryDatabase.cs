/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Transactions;
using NLog;

using Dict = System.Collections.Generic.Dictionary<string, object>;

namespace Butterfly.Core.Database.Memory {

    /// <inheritdoc/>
    public class MemoryDatabase : BaseDatabase {

        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        public MemoryDatabase() : base(null) {
        }

        protected override Task LoadSchemaAsync() {
            return Task.FromResult(0);
        }

        protected override Task<Table> LoadTableSchemaAsync(string tableName) {
            return null;
        }
        protected readonly HashSet<MemoryTable> ChangedTables = new HashSet<MemoryTable>();

        protected override BaseTransaction CreateTransaction() {
            return new MemoryTransaction(this);
        }

        protected override Task<Dict[]> DoSelectRowsAsync(string executableSql, Dict executableParams, int limit) {
            SelectStatement executableStatement = new SelectStatement(this, executableSql);
            if (executableStatement.StatementFromRefs.Length > 1) throw new Exception("MemoryTable does not support joins");
            if (!(executableStatement.StatementFromRefs[0].table is MemoryTable memoryTable)) throw new Exception("Table is not a MemoryTable");

            string[] fieldNames = string.IsNullOrEmpty(executableStatement.selectClause) || executableStatement.selectClause=="*" ? memoryTable.DataTable.Columns.Cast<DataColumn>().Select(x => x.ColumnName).ToArray() : executableStatement.selectClause.Split(',').Select(x => x.Trim()).ToArray();
            if (fieldNames.Any(x => x.Contains(' '))) throw new Exception("MemoryTable does not support field aliases");

            string evaluatedWhereClause = EvaluateWhereClause(executableStatement.whereClause, executableParams, executableStatement.StatementFromRefs);
            DataRow[] dataRows = memoryTable.DataTable.Select(evaluatedWhereClause, null, DataViewRowState.OriginalRows);
            List<Dict> rows = new List<Dict>();
            foreach (var dataRow in dataRows) {
                Dict row = new Dict();
                foreach (var fieldName in fieldNames) {
                    row[fieldName] = dataRow[fieldName, DataRowVersion.Original];
                }
                rows.Add(row);
                if (limit > 0 && rows.Count >= limit) break;
            }
            return Task.FromResult(rows.ToArray());
        }

        protected override Task<Dict[]> DoQueryRowsAsync(string storedProcedureName, Dict vars = null) {
            throw new NotImplementedException();
        }

        protected static readonly Regex SIMPLE_REPLACE = new Regex(@"(?<tableAliasWithDot>\w+\.)?(?<fieldName>\w+)\s*(?<op>=|<>|!=|>|<)\s*(?<param>\@\w+)");
        protected static readonly Regex IN_REPLACE = new Regex(@"(?<tableAliasWithDot>\w+\.)?(?<fieldName>\w+)\s+(?<op>IN|NOT\s+IN)\s+\((?<param>[^\)]+)\)", RegexOptions.IgnoreCase);

        public override bool CanJoin => false;
        public override bool CanFieldAlias => false;

        public static string EvaluateWhereClause(string whereClause, Dict sqlParams, StatementFromRef[] tableRefs) {
            string newWhereClause = whereClause;
            newWhereClause = EvaluateWhereClauseReplace(newWhereClause, SIMPLE_REPLACE, sqlParams, tableRefs, op => {
                if (op == "!=") return "<>";
                else return op;
            });
            newWhereClause = EvaluateWhereClauseReplace(newWhereClause, IN_REPLACE, sqlParams, tableRefs);
            return newWhereClause;
        }

        protected static string EvaluateWhereClauseReplace(string sql, Regex regex, Dict sqlParams, StatementFromRef[] tableRefs, Func<string, string> remapOp = null) {
            StringBuilder sb = new StringBuilder();
            int lastIndex = 0;
            foreach (Match match in regex.Matches(sql)) {
                sb.Append(sql.Substring(lastIndex, match.Groups["op"].Index - lastIndex));
                string op = match.Groups["op"].Value;
                sb.Append(remapOp !=null ? remapOp(op) : op);
                lastIndex = match.Groups["op"].Index + match.Groups["op"].Length;

                sb.Append(sql.Substring(lastIndex, match.Groups["param"].Index - lastIndex));

                // Get table ref
                string tableAlias = match.Groups["tableAliasWithDot"].Value.Replace(".", "");
                StatementFromRef tableRef;
                if (string.IsNullOrEmpty(tableAlias)) {
                    if (tableRefs.Length > 1) throw new Exception("SELECT statements with more than one table reference must use table aliases for all where clause fields");
                    tableRef = tableRefs[0];
                }
                else {
                    tableRef = Array.Find(tableRefs, x => x.tableAlias==tableAlias);
                }

                // Get field defs
                string fieldName = match.Groups["fieldName"].Value;
                if (fieldName.Equals("NOT", StringComparison.OrdinalIgnoreCase)) {
                    lastIndex = match.Groups["param"].Index;
                }
                else {
                    TableFieldDef fieldDef = tableRef.table.FindFieldDef(fieldName);

                    // Get evaluated value
                    var paramNames = match.Groups["param"].Value.Split(',').Select(x => x.Replace("@", "").Trim());
                    bool isFirst = true;
                    foreach (var paramName in paramNames) {
                        object replacementValue = sqlParams[paramName];
                        string evaluatedValue;
                        if (fieldDef.type == typeof(string)) {
                            evaluatedValue = $"'{replacementValue}'";
                        }
                        else if (fieldDef.type == typeof(DateTime)) {
                            evaluatedValue = $"#{replacementValue}#";
                        }
                        else {
                            evaluatedValue = $"{replacementValue}";
                        }
                        if (isFirst) isFirst = false;
                        else sb.Append(',');
                        sb.Append(evaluatedValue);
                    }
                    lastIndex = match.Groups["param"].Index + match.Groups["param"].Length;
                }
            }
            sb.Append(sql.Substring(lastIndex));
            return sb.ToString();
        }

        protected override Task<int> DoUpdateAsync(string executableSql, Dict executableParams) {
            UpdateStatement executableStatement = new UpdateStatement(this, executableSql);
            if (!(executableStatement.StatementFromRefs[0].table is MemoryTable memoryTable)) throw new Exception("Table is not a MemoryTable");

            (var whereIndex, var setRefs, var whereRefs) = executableStatement.GetWhereIndexSetRefsAndWhereRefs(this, executableParams);
            var fieldValues = BaseStatement.RemapStatementParamsToFieldValues(executableParams, setRefs);

            string evaluatedWhereClause = EvaluateWhereClause(executableStatement.whereClause, executableParams, executableStatement.StatementFromRefs);
            var dataRows = memoryTable.DataTable.Select(evaluatedWhereClause);
            int count = 0;
            foreach (var dataRow in dataRows) {
                bool changed = false;

                foreach (var fieldValue in fieldValues) {
                    if (dataRow[fieldValue.Key] != fieldValue.Value) {
                        dataRow[fieldValue.Key] = fieldValue.Value;
                        changed = true;
                    }
                }
                
                if (changed) count++;
            }

            AddChangeTable(memoryTable);
            return Task.FromResult(count);
        }

        protected override Task<Func<object>> DoInsertAsync(string executableSql, Dict executableParams, bool ignoreIfDuplicate)
        {
            InsertStatement executableStatement = new InsertStatement(this, executableSql);
            var memoryTable = executableStatement.StatementFromRefs[0].table as MemoryTable;

            var insertRefs = executableStatement.GetInsertRefs(executableParams);
            var fieldValues = BaseStatement.RemapStatementParamsToFieldValues(executableParams, insertRefs);

            var dataRow = memoryTable.DataTable.NewRow();
            foreach (var nameValuePair in fieldValues)
            {
                dataRow[nameValuePair.Key] = nameValuePair.Value;
            }
            memoryTable.DataTable.Rows.Add(dataRow);

            AddChangeTable(memoryTable);

            if (memoryTable.AutoIncrementFieldName == null)
            {
                return Task.FromResult<Func<object>>(null);
            }
            else
            {
                return Task.FromResult<Func<object>>(() => dataRow[memoryTable.AutoIncrementFieldName]);
            }
        }

        protected override Task<int> DoDeleteAsync(string executableSql, Dict executableParams) {
            DeleteStatement executableStatement = new DeleteStatement(this, executableSql);
            var memoryTable = executableStatement.StatementFromRefs[0].table as MemoryTable;

            string evaluatedWhereClause = MemoryDatabase.EvaluateWhereClause(executableStatement.whereClause, executableParams, executableStatement.StatementFromRefs);
            var dataRows = memoryTable.DataTable.Select(evaluatedWhereClause);
            foreach (var dataRow in dataRows) {
                dataRow.Delete();
            }

            AddChangeTable(memoryTable);
            return Task.FromResult(dataRows.Length);
        }

        private void AddChangeTable(MemoryTable table)
        {
            if (Transaction.Current == null) {
                // no transaction, so accept and complete
                table.DataTable.AcceptChanges();
                return;
            }

            ChangedTables.Add(table);

            // only add the event the first time
            if (ChangedTables.Count == 1)
                Transaction.Current.TransactionCompleted += Current_TransactionCompleted;
        }

        private void Current_TransactionCompleted(object sender, TransactionEventArgs e)
        {
            foreach (var changedTable in ChangedTables) {
                if (e.Transaction.TransactionInformation.Status == TransactionStatus.Committed)
                    changedTable.DataTable.AcceptChanges();
                else
                    changedTable.DataTable.RejectChanges();
            }
        }

        protected override Task DoTruncateAsync(string tableName)
        {
            if (!TableByName.TryGetValue(tableName, out Table table)) throw new Exception($"Invalid table name '{tableName}'");
            if (!(table is MemoryTable memoryTable)) throw new Exception($"Invalid table type {table.GetType()}");
            memoryTable.DataTable.Clear();
            return Task.FromResult(0);
        }
    }
}
