﻿/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NLog;

using Butterfly.Core.Database;
using Butterfly.Core.Util;
using Butterfly.Core.Util.Field;
using Butterfly.Core.WebApi;

using Dict = System.Collections.Generic.Dictionary<string, object>;
using System.Transactions;

namespace Butterfly.Core.Notify {

    public enum NotifyMessageType {
        Email,
        PhoneText,
    }

    public class NotifyManager {

        protected static readonly Logger logger = LogManager.GetCurrentClassLogger();

        protected readonly IDatabase database;

        protected readonly NotifyMessageEngine emailNotifyMessageEngine;
        protected readonly NotifyMessageEngine phoneNotifyMessageEngine;

        protected readonly string notifyMessageTableName;
        protected readonly string notifyVerifyTableName;
        protected readonly int verifyCodeExpiresSeconds;

        protected readonly NotifyMessage verifyEmailNotifyMessage;
        protected readonly NotifyMessage verifyPhoneNotifyMessage;
        protected readonly string verifyCodeFormat;

        protected readonly static EmailFieldValidator EMAIL_FIELD_VALIDATOR = new EmailFieldValidator("email", false, true);
        protected readonly static PhoneFieldValidator PHONE_FIELD_VALIDATOR = new PhoneFieldValidator("phone", false);

        protected readonly static Random RANDOM = new Random();

        public NotifyManager(IDatabase database, INotifyMessageSender emailNotifyMessageSender = null, INotifyMessageSender phoneNotifyMessageSender = null, string notifyMessageTableName = "notify_message", string notifyVerifyTableName = "notify_verify", int verifyCodeExpiresSeconds = 3600, string verifyEmailFile = null, string verifyPhoneTextFile = null, string verifyCodeFormat = "###-###") {
            this.database = database;
            this.emailNotifyMessageEngine = emailNotifyMessageSender == null ? null : new NotifyMessageEngine(NotifyMessageType.Email, emailNotifyMessageSender, database, notifyMessageTableName);
            this.phoneNotifyMessageEngine = phoneNotifyMessageSender == null ? null : new NotifyMessageEngine(NotifyMessageType.PhoneText, phoneNotifyMessageSender, database, notifyMessageTableName);
            this.notifyMessageTableName = notifyMessageTableName;
            this.notifyVerifyTableName = notifyVerifyTableName;
            this.verifyCodeExpiresSeconds = verifyCodeExpiresSeconds;

            this.verifyEmailNotifyMessage = verifyEmailFile!=null ? NotifyMessage.ParseFile(verifyEmailFile) : null;
            this.verifyPhoneNotifyMessage = verifyPhoneTextFile!=null ? NotifyMessage.ParseFile(verifyPhoneTextFile) : null;
            this.verifyCodeFormat = verifyCodeFormat;
        }

        public void Start() {
            this.emailNotifyMessageEngine?.Start();
            this.phoneNotifyMessageEngine?.Start();
        }

        public void Stop() {
            this.emailNotifyMessageEngine?.Stop();
            this.phoneNotifyMessageEngine?.Stop();
        }

        public async Task SendVerifyCodeAsync(string contact) {
            logger.Debug($"SendVerifyCodeAsync():contact={contact}");

            // Scrub contact
            string scrubbedContact = Validate(contact);
            logger.Debug($"SendVerifyCodeAsync():scrubbedContact={scrubbedContact}");

            // Generate code and expires at
            int digits = this.verifyCodeFormat.Count(x => x=='#');
            int min = (int)Math.Pow(10, digits - 1);
            int max = (int)Math.Pow(10, digits) - 1;
            int code = RANDOM.Next(0, max - min) + min;
            logger.Debug($"SendVerifyCodeAsync():digits={digits},min={min},max={max},code={code}");
            DateTime expiresAt = DateTime.Now.AddSeconds(this.verifyCodeExpiresSeconds);

            // Insert/update database
            string id = await this.database.SelectValueAsync<string>($"SELECT id FROM {this.notifyVerifyTableName}", new {
                contact = scrubbedContact
            });

            using (var transaction = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled)) {
                if (id == null) {
                    await database.InsertAsync<string>(this.notifyVerifyTableName, new {
                        contact = scrubbedContact,
                        verify_code = code,
                        expires_at = expiresAt,
                    });
                }
                else {
                    await database.UpdateAsync(this.notifyVerifyTableName, new {
                        id,
                        verify_code = code,
                        expires_at = expiresAt,
                    });
                }

                // Send notify message
                var notifyMessageType = DetectNotifyMessageType(scrubbedContact);
                NotifyMessage notifyMessage = null;
                switch (notifyMessageType) {
                    case NotifyMessageType.Email:
                        if (this.verifyEmailNotifyMessage == null) throw new Exception("Server must be configured with verify email notify message");
                        notifyMessage = this.verifyEmailNotifyMessage;
                        break;
                    case NotifyMessageType.PhoneText:
                        if (this.verifyPhoneNotifyMessage == null) throw new Exception("Server must be configured with verify phone text notify message");
                        notifyMessage = this.verifyPhoneNotifyMessage;
                        break;
                }
                var evaluatedNotifyMessage = notifyMessage.Evaluate(new {
                    contact = scrubbedContact,
                    code = code.ToString(this.verifyCodeFormat)
                });
                await this.Queue(evaluatedNotifyMessage);

                transaction.Complete();
            }
        }

        public async Task<string> VerifyAsync(string contact, int code) {
            logger.Debug($"VerifyAsync():contact={contact},code={code}");
            string scrubbedContact = Validate(contact);
            Dict result = await this.database.SelectRowAsync($"SELECT verify_code, expires_at FROM {this.notifyVerifyTableName}", new {
                contact = scrubbedContact
            });
            int verifyCode = result.GetAs("verify_code", -1);
            if (code == -1 || code!=verifyCode) throw new Exception("Invalid contact and/or verify code");

            int expiresAtUnix = result.GetAs("expires_at", -1);
            if (DateTimeX.FromUnixTimestamp(expiresAtUnix) < DateTime.Now) throw new Exception("Expired verify code");

            return scrubbedContact;
        }

        protected static string Validate(string value) {
            logger.Debug($"Validate():value={value}");
            if (value.Contains("@")) {
                return EMAIL_FIELD_VALIDATOR.Validate(value);
            }
            else {
                return PHONE_FIELD_VALIDATOR.Validate(value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="notifyMessages"></param>
        /// <returns></returns>
        public async Task Queue(params NotifyMessage[] notifyMessages) {
            bool emailQueued = false;
            bool phoneTextQueued = false;
            foreach (var notifyMessage in notifyMessages) {
                if (notifyMessage == null) continue;

                if (string.IsNullOrEmpty(notifyMessage.from)) throw new Exception("From address cannot be blank");
                string scrubbedFrom = Validate(notifyMessage.from);

                if (string.IsNullOrEmpty(notifyMessage.to)) throw new Exception("To address cannot be blank");
                string scrubbedTo = Validate(notifyMessage.to);

                NotifyMessageType notifyMessageType = this.DetectNotifyMessageType(scrubbedTo);
                switch (notifyMessageType) {
                    case NotifyMessageType.Email:
                        if (this.emailNotifyMessageEngine == null) throw new Exception("No email message sender configured");
                        await this.emailNotifyMessageEngine.Queue(scrubbedFrom, scrubbedTo, notifyMessage.subject, notifyMessage.bodyText, notifyMessage.bodyHtml, notifyMessage.priority, notifyMessage.extraData);
                        emailQueued = true;
                        break;
                    case NotifyMessageType.PhoneText:
                        if (this.phoneNotifyMessageEngine == null) throw new Exception("No phone text message sender configured");
                        await this.phoneNotifyMessageEngine.Queue(scrubbedFrom, scrubbedTo, notifyMessage.subject, notifyMessage.bodyText, notifyMessage.bodyHtml, notifyMessage.priority, notifyMessage.extraData);
                        phoneTextQueued = true;
                        break;
                }
            }
            
            if (Transaction.Current == null) {
                // notify immediately
                SendNotifications(emailQueued, phoneTextQueued);
            }
            else {
                // execution wrapped in transaction, wait for commit
                Transaction.Current.TransactionCompleted += (object sender, TransactionEventArgs e) => {
                    if (e.Transaction.TransactionInformation.Status == TransactionStatus.Committed) {
                        SendNotifications(emailQueued, phoneTextQueued);
                    }
                };
            }
        }

        private void SendNotifications(bool emailQueued, bool phoneTextQueued) {
            if (emailQueued) this.emailNotifyMessageEngine.Pulse();
            if (phoneTextQueued) this.phoneNotifyMessageEngine.Pulse();
        }

        private void Current_TransactionCompleted(object sender, TransactionEventArgs e) {
            throw new NotImplementedException();
        }

        protected NotifyMessageType DetectNotifyMessageType(string to) {
            if (string.IsNullOrWhiteSpace(to)) throw new Exception("Invalid contact '" + to + "'");
            else if (to.Contains("@")) return NotifyMessageType.Email;
            else if (to.StartsWith("+")) return NotifyMessageType.PhoneText;
            else throw new Exception("Invalid to '" + to + "'");
        }

        protected class NotifyMessageEngine {
            protected readonly NotifyMessageType notifyMessageType;
            protected readonly INotifyMessageSender notifyMessageSender;
            protected readonly IDatabase database;
            protected readonly string notifyMessageTableName;

            protected CancellationTokenSource cancellationTokenSource = null;

            public NotifyMessageEngine(NotifyMessageType notifyMessageType, INotifyMessageSender notifyMessageSender, IDatabase database, string notifyMessageTableName) {
                this.notifyMessageType = notifyMessageType;
                this.notifyMessageSender = notifyMessageSender;
                this.database = database;
                this.notifyMessageTableName = notifyMessageTableName;
            }

            protected bool started;

            public void Start() {
                if (!this.started) {
                    Task task = this.Run();
                }
            }

            public void Stop() {
                this.started = false;
                this.Pulse();
            }

            public async Task Queue(string from, string to, string subject, string bodyText, string bodyHtml, byte priority, Dict extraData) {
                logger.Debug($"Queue():type={this.notifyMessageType},priority={priority},from={from},to={to},subject={subject}");
                string extraJson = extraData != null && extraData.Count > 0 ? JsonUtil.Serialize(extraData) : null;
                await database.InsertAsync<string>(this.notifyMessageTableName, new {
                    message_type = (byte)this.notifyMessageType,
                    message_priority = priority,
                    message_from = from,
                    message_to = to,
                    message_subject = subject,
                    message_body_text = bodyText,
                    message_body_html = bodyHtml,
                    extra_json = extraJson,
                });
            }

            public void Pulse() {
                this.cancellationTokenSource?.Cancel();
            }

            async Task Run() {
                this.started = true;
                while (this.started) {
                    DateTime start = DateTime.Now;

                    Dict message = await this.database.SelectRowAsync(
                        @"SELECT *
                        FROM " + this.notifyMessageTableName + @"
                        WHERE message_type=@messageType AND sent_at IS NULL
                        ORDER BY message_priority DESC, created_at", new {
                            messageType = (byte)this.notifyMessageType
                        });
                    if (message == null) {
                        logger.Trace("Run():Waiting indefinitely");
                        try {
                            this.cancellationTokenSource = new CancellationTokenSource();
                            await Task.Delay(60000, cancellationTokenSource.Token);
                        }
                        catch (TaskCanceledException) {
                            this.cancellationTokenSource = null;
                        }
                        logger.Trace("Run():Waking up");
                    }
                    else {
                        NotifyMessageType notifyMessageType = message.GetAs("type", NotifyMessageType.Email);
                        string sentMessageId = null;
                        string error = null;
                        try {
                            string from = message.GetAs("message_from", (string)null);
                            string to = message.GetAs("message_to", (string)null);
                            string subject = message.GetAs("message_subject", (string)null);
                            string bodyText = message.GetAs("message_body_text", (string)null);
                            string bodyHtml = message.GetAs("message_body_html", (string)null);
                            logger.Debug($"Run():Sending message to {to}");

                            sentMessageId = await this.notifyMessageSender.SendAsync(from, to, subject, bodyText, bodyHtml);
                        }
                        catch (Exception e) {
                            error = e.Message;
                            logger.Error(e);
                        }

                        var id = message.GetAs("id", (string)null);
                        await this.database.UpdateAndCommitAsync(this.notifyMessageTableName, new {
                            id,
                            sent_at = DateTime.Now,
                            sent_message_id = sentMessageId,
                            send_error = error,
                        });

                        int totalMillis = (int)(this.notifyMessageSender.CanSendNextAt - DateTime.Now).TotalMilliseconds;
                        if (totalMillis>0) {
                            logger.Trace("Run():Sleeping for " + totalMillis + "ms");
                            await Task.Delay(totalMillis);
                        }
                    }

                }
            }
        }
    }
}
