using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Transactions;

namespace DqciFileDelivery
{
    internal class Program
    {
        private static void WriteLine(string text, string filePath)
        {
            Logger.WriteLine(text + Environment.NewLine, filePath);
        }

        private static void Main(string[] args)
        {
            // Read the files from folder
            string connString = ConfigurationManager.ConnectionStrings["eLimsMilkM2MReports"].ConnectionString;
            string fromMailAddress = ConfigurationManager.AppSettings.Get("FromEmail");
            string mailQueue = ConfigurationManager.AppSettings.Get("EmailQueue");
            string SMTPServer = ConfigurationManager.AppSettings.Get("SMTPServer");
            int Port = int.Parse(ConfigurationManager.AppSettings.Get("SMTPPortNumber"));
            string UserName = ConfigurationManager.AppSettings.Get("SMTPUserName");
            string Password = ConfigurationManager.AppSettings.Get("SMTPPassword");
            string mailSentFolders = ConfigurationManager.AppSettings.Get("MailSent");
            string logFilePath = ConfigurationManager.AppSettings.Get("LogPath");

            string todaysDate = DateTime.Now.ToString("yyyyMMdd");
            string LogFile = string.Empty;

            string input = "1";
            if (args != null && args.Any())
                input = args[0];

            if (input == "0")
            {
                try
                {
                    todaysDate = todaysDate + ".txt";

                    LogFile = Path.Combine(logFilePath, todaysDate);
                    Logger.WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss fff"));


                    CustomerSendFiles obj = new CustomerSendFiles();

                    // get Customer details from Db // // List of Customers TO Sen Files

                    WriteLine("Read Customer Files", LogFile);
                    List<CustomerFiles> cusromerFileDetails = obj.ReadCustomerFiles();
                    WriteLine("Read Customer Files completed", LogFile);

                    if (cusromerFileDetails.Count <= 0)
                    {
                        WriteLine("No files to process", LogFile);

                        return;
                    }

                    WriteLine("Get Customer Detailses", LogFile);
                    List<CustomerDetails> customerDetailses = obj.GetCustomerDetailses(cusromerFileDetails, connString);
                    WriteLine("Get Customer Detailses Completed", LogFile);
                    if (customerDetailses.Count <= 0)
                    {
                        WriteLine("No Customer to process", LogFile);
                        return;
                    }

                    // Process Each File
                    foreach (CustomerFiles oneFile in cusromerFileDetails)
                    {
                        //get Customer delivery method

                        WriteLine("Processing File " + oneFile.FilePath, LogFile);
                        CustomerDetails oneCustomer =
                            customerDetailses.FirstOrDefault(
                                c =>
                                    String.Equals(c.CustomerCode, oneFile.CustomerCode,
                                        StringComparison.CurrentCultureIgnoreCase));

                        if (oneCustomer == null)
                        {
                            WriteLine("NO Customer for this file " + oneFile.FilePath, LogFile);
                            continue;
                        }

                        WriteLine("Processing Customer " + oneCustomer.CustomerCode + " for file " + oneFile.FilePath,
                            LogFile);
                        switch (oneCustomer.Mode.ToUpper())
                        {

                            case "EMAIL":
                            {
                                WriteLine(
                                    "Processing Customer to Send mail " + oneCustomer.CustomerCode + " for file " +
                                    oneFile, LogFile);

                                //Write to transaction log
                                //--------------------------------------------------------------------------------------------------------------------------------------------
                                using (
                                    TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew,
                                        new TransactionOptions
                                        {
                                            IsolationLevel = IsolationLevel.ReadCommitted
                                        }))
                                {
                                    SendEmails mailToSend = new SendEmails();

                                    // Write to DB
                                    WriteLine("Write To DB Transaction table " + oneCustomer.CustomerCode, LogFile);

                                    oneCustomer.CustomerSpecificFileName = RenameReadyToSendFiles.RenameFiles(
                                        oneCustomer, SendEmails.GetFileName(oneFile.FilePath));
                                    bool isinserted = mailToSend.WriteToTransaction(oneCustomer, connString,
                                        oneFile.FilePath, "Queued");

                                    if (isinserted)
                                    {
                                        WriteLine("Send To Queue " + oneCustomer.CustomerCode, LogFile);
                                        bool mailSent = mailToSend.WriteMailMessageToQueue(oneCustomer, oneFile.FilePath,
                                            fromMailAddress, mailQueue);
                                        if (mailSent)
                                        {
                                            // Move file to Sent folder
                                            WriteLine(
                                                "Move file to sent folder from " + oneFile.FilePath + " to " +
                                                mailSentFolders, LogFile);
                                            MoveFile(oneFile.FilePath, mailSentFolders);
                                            scope.Complete();
                                        }
                                        else
                                        {
                                            WriteLine(
                                                "Error in move file to sent folder from " + oneFile.FilePath + " to " +
                                                mailSentFolders, LogFile);
                                        }
                                    }

                                }

                                //--------------------------------------------------------------------------------------------------------------------------------------------

                                break;
                            }

                            case "FTP":
                            {
                                //Write to transaction log
                                //--------------------------------------------------------------------------------------------------------------------------------------------
                                using (
                                    TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew,
                                        new TransactionOptions
                                        {
                                            IsolationLevel = IsolationLevel.ReadCommitted
                                        }))
                                {
                                    SendEmails mailToSend = new SendEmails();
                                    SendFtp sendToFtp = new SendFtp();

                                    WriteLine(
                                        "Send TO FTP Customer " + oneCustomer.CustomerCode + " ftp:  " +
                                        oneCustomer.FTPURL + " file: " + oneFile.FilePath, LogFile);
                                    string mailSent = sendToFtp.SendFileToFtp(oneCustomer, oneFile.FilePath);

                                    if (mailSent == "File sent to FTP")
                                    {
                                        WriteLine(
                                            "Send TO FTP Customer " + oneCustomer.CustomerCode + " ftp:  " +
                                            oneCustomer.FTPURL + " file: " + oneFile.FilePath + " Status: " +
                                            mailSent, LogFile);

                                        // Write to DB
                                        WriteLine("Write To DB Transaction table " + oneCustomer.CustomerCode,
                                            LogFile);

                                        bool isinserted = mailToSend.WriteToTransaction(oneCustomer, connString,
                                            oneFile.FilePath, mailSent);

                                        // Move file to Sent folder
                                        WriteLine(
                                            "Move file to sent folder from " + oneFile.FilePath + " to " +
                                            mailSentFolders + " write to Db status " + isinserted, LogFile);
                                        MoveFile(oneFile.FilePath, mailSentFolders);
                                    }
                                    else
                                    {
                                        WriteLine(
                                            "Send TO FTP Customer " + oneCustomer.CustomerCode + " ftp:  " +
                                            oneCustomer.FTPURL + " file: " + oneFile.FilePath + " Status: " +
                                            mailSent, LogFile);
                                    }
                                    scope.Complete();
                                }

                                break;
                            }

                            default:
                                break;

                        }

                    }
                }
                catch (Exception ex)
                {

                    WriteLine("Send to Queue Error: " + ex.Message, LogFile);
                }
                finally
                {
                    WriteLine(
                        "---------------------------------------------------------------------------------------------------",
                        LogFile);
                }

            }
            else
            {
                try
                {
                    //as both if and else conditions runs in sequential based on the arg values we pass, better add a little delay so that we will not miss any queue items.
                    Thread.Sleep(3000);

                    todaysDate = todaysDate + "-1.txt";
                    LogFile = Path.Combine(logFilePath, todaysDate);
                    WriteLine(DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss fff"), LogFile);

                    SendEmails mailToSend = new SendEmails();
                    bool queNotEmpty = true;
                    do
                    {
                        string attachementFileName = string.Empty;
                        queNotEmpty = mailToSend.SendQueueMails(mailQueue, SMTPServer, Port, UserName, Password,
                            ref attachementFileName);

                        if (queNotEmpty)
                        {
                            WriteLine("Sent file " + attachementFileName, LogFile);

                            // Update the Report transaction for file sent
                            mailToSend.UpdateMailSent(connString, attachementFileName);

                        }
                        else
                        {
                            WriteLine("No file to Sent ", LogFile);
                        }

                    } while (queNotEmpty);


                }
                catch (Exception ex)
                {

                    WriteLine("Send to mail Error: " + ex.Message, LogFile);
                }
                finally
                {
                    WriteLine(
                        "---------------------------------------------------------------------------------------------------",
                        LogFile);
                }

            }

        }

        private static void MoveFile(string sourcFile, string destinationFolder)
        {
            string fileName = sourcFile.Substring(sourcFile.LastIndexOf("\\") + 1);

            if (fileName != null)
            {
                string fileWithAbsolutePath = Path.Combine(destinationFolder, fileName);

                // Ensure that the target does not exist.
                if (File.Exists(fileWithAbsolutePath))
                    File.Delete(fileWithAbsolutePath);
                File.Move(sourcFile, fileWithAbsolutePath);
            }
        }

    }
}