using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Messaging;
using System.Net.Mail;
using Eurofins.Milk.FunctionalService.Helper;

namespace DqciFileDelivery
{

    public class sendEmails
    {

        public static bool FileExists(string fileName)
        {
            //make sure we don't access directories outside of our store for security reasons
            string file = null;
            if (!string.IsNullOrEmpty(fileName))
            {
                var dir = fileName.Substring(0, fileName.LastIndexOf("\\") + 1);

                var filename = fileName.Substring(fileName.LastIndexOf("\\") + 1);

                file = Directory.GetFiles(dir, filename, SearchOption.TopDirectoryOnly)
                    .FirstOrDefault();
            }

            return file != null;
        }

        public static FileStream Open(string fileName)
        {
            return File.Open(fileName,
                FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        }

        public static string GetFileName(string fullPath)
        {
            return fullPath.Substring(fullPath.LastIndexOf("\\") + 1);
        }

        public bool WriteMailMessageToQueue(CustomerDetails customer, string filename, string fromMailAddress,
            string queuePath)
        {
            if (customer == null)
            {
                return false;
            }

            bool queueStatus = false;
            try
            {
                using (MailMessage mail = new MailMessage())
                {
                    string[] toEmails = customer.emailList.Split(',');

                    foreach (string email in toEmails)
                    {
                        if (!string.IsNullOrWhiteSpace(email))
                            mail.To.Add(new MailAddress(email));
                    }

                    if (FileExists(filename))
                        mail.Attachments.Add(new Attachment(Open(filename), GetFileName(filename)));

                    mail.Subject = "Eurofins DQCI M2M Report";
                    mail.Sender = new MailAddress(fromMailAddress);
                    mail.From = new MailAddress(fromMailAddress);
                    mail.Body = "This is an automated Report Delivery. Please find your report attached";

                    SerializeableMailMessage serializedMail = new SerializeableMailMessage(mail);
                    Message queueMessage = new Message();
                    queueMessage.Body = serializedMail;
                    queueMessage.Recoverable = true;

                    queueMessage.Formatter = new BinaryMessageFormatter();


                    MessageQueue queue = new MessageQueue(queuePath);
                    queue.Send(queueMessage);
                    queueStatus = true;

                }
            }
            catch (Exception exception)
            {

                //if (retryCount-- > 0)
                //    WriteMailMessageToQueue(transactionInfo, linkToFile, fileFormat, retryCount);

                //string errDet = ExceptionFormatter.GetFormattedExceptionDetails(exception);
                //_error.Error(errDet, exception);
            }
            return queueStatus;
        }

        public bool SendQueueMails(string queuePath, string SMTPServer, int SMTPPortNumber, string SMTPUserName,
            string SMTPPassword, ref string fileAttchementname)
        {

            try
            {
                MessageQueue queue = new MessageQueue(queuePath);

                SMTPHelper smtpHelper = new SMTPHelper();
                smtpHelper.Host = SMTPServer; // _configHelper.GetConfigValue("SMTPServer");
                smtpHelper.Port = SMTPPortNumber; //Convert.ToInt32(_configHelper.GetConfigValue("SMTPPortNumber"));
                smtpHelper.UserName = SMTPUserName; //_configHelper.GetConfigValue("SMTPUserName");
                smtpHelper.Password = SMTPPassword; //_configHelper.GetConfigValue("SMTPPassword");

                Message msg = queue.Receive(TimeSpan.Zero);

                if (msg == null)
                {
                    return false;
                }

                msg.Formatter = new BinaryMessageFormatter();
                if (msg.Body is SerializeableMailMessage)
                {

                    SerializeableMailMessage mail = (SerializeableMailMessage) msg.Body;
                    try
                    {
                        MailMessage objMsg = mail.GetMailMessage();

                        string attachments =
                            objMsg.Attachments.AsEnumerable().Select(a => a.Name).FirstOrDefault();

                        smtpHelper.Send(objMsg);
                        fileAttchementname = attachments;

                    }
                    catch (Exception ex)
                    {
                        return false;
                    }
                }

            }
            catch (MessageQueueException mqe)
            {
                if (mqe.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                    return false;

                return false;
            }
            return true;
        }


        public bool WriteToTransaction(CustomerDetails customer, string connString, string fileName, string status)
        {
            List<CustomerDetails> custmers = new List<CustomerDetails>();
            custmers.Add(customer);
            DataTable dtCustomers = helperClass.ConvertToDataTable(custmers);

            System.Data.DataColumn newColumn = new System.Data.DataColumn("FileNameToSend", typeof (System.String));
            newColumn.DefaultValue = GetFileName(fileName.ToUpper());
            dtCustomers.Columns.Add(newColumn);


            System.Data.DataColumn ActionNameColumn = new System.Data.DataColumn("ActionName", typeof (System.String));
            ActionNameColumn.DefaultValue = status;
            dtCustomers.Columns.Add(ActionNameColumn);

            System.Data.DataColumn dateColumn = new System.Data.DataColumn("LastUpdatedDate", typeof (System.DateTime));
            dateColumn.DefaultValue = DateTime.UtcNow;
            dtCustomers.Columns.Add(dateColumn);

            InsertDataToTable(dtCustomers, connString);
            return true;
        }

        private bool InsertDataToTable(DataTable entityDataTable, string connString)
        {
            using (var cnnSqlConnection = new SqlConnection(connString))
            {
                cnnSqlConnection.Open();
                using (SqlBulkCopy bulkcopy = new SqlBulkCopy(cnnSqlConnection))
                {
                    // bulkcopy.BulkCopyTimeout = 600;
                    bulkcopy.DestinationTableName = "ReportTransactions";
                    foreach (DataColumn col in entityDataTable.Columns)
                    {
                        bulkcopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                    bulkcopy.WriteToServer(entityDataTable);
                }
            }
            return true;
        }

        public bool UpdateMailSent(string connString, string fileName)
        {
            bool result = true;

            using (var cnnSqlConnection = new SqlConnection(connString))
            {
                using (var command = new SqlCommand())
                {
                    command.Connection = cnnSqlConnection;
                    cnnSqlConnection.Open();

                    command.CommandText =
                        @"UPDATE RT SET RT.ActionName='Mail Sent' FROM ReportTransactions RT WHERE RT.FileNameToSend = '" +
                        fileName.ToUpper() + "'  AND RT.ActionName='Queued'";
                    command.ExecuteNonQuery();

                }
            }

            return result;
        }
    }
}