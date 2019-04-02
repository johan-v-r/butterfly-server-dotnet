﻿/* Any copyright is dedicated to the Public Domain.
 * http://creativecommons.org/publicdomain/zero/1.0/ */

using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Reflection;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Butterfly.Core.Database;
using Butterfly.Core.Database.Dynamic;
using Butterfly.Core.Database.Event;

using Dict = System.Collections.Generic.Dictionary<string, object>;
using System.Transactions;

namespace Butterfly.Core.Test {
    [TestClass]
    public class DynamicUnitTest {
        public static async Task TestDatabase(IDatabase database, Assembly assembly = null, string resourceFile = "Butterfly.Core.Test.db.sql") {
            await database.CreateFromResourceFileAsync(assembly ?? Assembly.GetExecutingAssembly(), resourceFile);
            database.SetDefaultValue("id", tableName => Guid.NewGuid().ToString(), "employee");
            database.SetDefaultValue("created_at", tableName => DateTime.Now);
            database.SetDefaultValue("updated_at", tableName => DateTime.Now);

            await DatabaseUnitTest.TruncateData(database);
            (object salesDepartmentId, object hrDepartmentId, object customerServiceDepartmentId) = await DatabaseUnitTest.InsertBasicData(database);
            await TestInsertUpdateDeleteEvents(database, salesDepartmentId, "SELECT * FROM employee", "name", "Joe Sales, Jr", 5, 1, 1, 1);
            await TestInsertUpdateDeleteEvents(database, salesDepartmentId, "SELECT id, name FROM employee", "department_id", -1, 5, 1, 0, 1);
            if (database.CanJoin) {
                await TestInsertUpdateDeleteEvents(database, salesDepartmentId, "SELECT e.id, e.name FROM employee e INNER JOIN department d ON e.department_id=d.id ORDER BY e.name", "name", "Joe Sales, Sr", 5, 1, 1, 1, new string[] { "id" });
                await TestInsertUpdateDeleteEvents(database, salesDepartmentId, "SELECT ec.employee_id, ec.contact_type, ec.contact_data, e.name FROM employee_contact ec INNER JOIN employee e ON ec.employee_id=e.id", "name", "Joe Sales, Sr", 8, 0, 0, 0, new string[] { "employee_id", "contact_type" });
                await TestMinimalSelects(database);
            }
        }

        public static async Task TestInsertUpdateDeleteEvents(IDatabase database, object salesDepartmentId, string selectSourceSql, string updateField, object updateValue, int initialCount, int insertCount, int updateCount, int deleteCount, string[] keyFieldNames = null) {
            List<DataEventTransaction> dataEventTransactionCollector = new List<DataEventTransaction>();
            using (DynamicViewSet dynamicViewSet = new DynamicViewSet(database, listener: dataEventTransaction => {
                dataEventTransactionCollector.Add(dataEventTransaction);
            })) {
                dataEventTransactionCollector.Clear();
                DynamicView employeeDynamicView = dynamicViewSet.CreateDynamicView(selectSourceSql, name: "xyz", keyFieldNames: keyFieldNames);
                await dynamicViewSet.StartAsync();
                Assert.AreEqual(1, dataEventTransactionCollector.Count);
                Assert.AreEqual(initialCount, dataEventTransactionCollector[0].dataEvents.Length);
                Assert.AreEqual(DataEventType.InitialBegin, dataEventTransactionCollector[0].dataEvents[0].dataEventType);
                Assert.AreEqual(DataEventType.Initial, dataEventTransactionCollector[0].dataEvents[1].dataEventType);
                Assert.AreEqual(DataEventType.Initial, dataEventTransactionCollector[0].dataEvents[2].dataEventType);
                Assert.AreEqual(DataEventType.Initial, dataEventTransactionCollector[0].dataEvents[3].dataEventType);
                Assert.AreEqual(DataEventType.InitialEnd, dataEventTransactionCollector[0].dataEvents[dataEventTransactionCollector[0].dataEvents.Length-1].dataEventType);

                // Confirm that an insert event is created
                dataEventTransactionCollector.Clear();
                string joeSalesEmployeeId;
                using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)) {
                    // Add Joe Sales employee
                    joeSalesEmployeeId = await database.InsertAsync<string>("employee", new {
                        name = "Joe Sales",
                        department_id = salesDepartmentId,
                    });
                    transaction.Complete();
                }
                await Task.Delay(50);
                Assert.AreEqual(insertCount, dataEventTransactionCollector.Count);
                if (insertCount > 0) {
                    Assert.AreEqual(1, dataEventTransactionCollector[0].dataEvents.Length);
                    Assert.AreEqual(DataEventType.Insert, dataEventTransactionCollector[0].dataEvents[0].dataEventType);
                }

                // Confirm that an update event is created
                dataEventTransactionCollector.Clear();
                using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)) {
                    // Update Joe Sales employee
                    await database.UpdateAsync($"UPDATE employee SET {updateField}=@{updateField} WHERE id=@id", new Dict {
                        ["id"] = joeSalesEmployeeId,
                        [updateField] = updateValue
                    });
                    transaction.Complete();
                }
                await Task.Delay(50);
                Assert.AreEqual(updateCount, dataEventTransactionCollector.Count);
                if (updateCount > 0) {
                    Assert.AreEqual(1, dataEventTransactionCollector[0].dataEvents.Length);
                    Assert.AreEqual(DataEventType.Update, dataEventTransactionCollector[0].dataEvents[0].dataEventType);
                    Assert.AreEqual(updateValue, (dataEventTransactionCollector[0].dataEvents[0] as RecordDataEvent).record[updateField]);
                }

                // Confirm that a delete event is created
                dataEventTransactionCollector.Clear();
                using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)) {
                    // Delete Joe Sales employee
                    await database.DeleteAsync("DELETE FROM employee WHERE id=@id", new {
                        id = joeSalesEmployeeId,
                    });
                    transaction.Complete();
                }
                await Task.Delay(50);
                Assert.AreEqual(deleteCount, dataEventTransactionCollector.Count);
                if (deleteCount > 0) {
                    Assert.AreEqual(1, dataEventTransactionCollector[0].dataEvents.Length);
                    Assert.AreEqual(DataEventType.Delete, dataEventTransactionCollector[0].dataEvents[0].dataEventType);
                }
            }
        }

        public static async Task TestMinimalSelects(IDatabase database) {
            await DatabaseUnitTest.TruncateData(database);
            using (DynamicViewSet dynamicViewSet = new DynamicViewSet(database, listener: dataEventTransaction => {
            })) {
                DynamicView departmentDynamicView = dynamicViewSet.CreateDynamicView("department");
                DynamicView employeeDynamicView = dynamicViewSet.CreateDynamicView("employee");
                await dynamicViewSet.StartAsync();
                await Task.Delay(50);

                int preSelectCount = database.SelectCount;
                await database.InsertAndCommitAsync<string>("employee", new {
                    name = "Joe Sales",
                    department_id = 1,
                });
                await Task.Delay(50);

                // Should require doing two SELECTs to handle this INSERT (before and after SELECT)
                Assert.AreEqual(preSelectCount + 1, database.SelectCount);
            }
        }
    }
}
