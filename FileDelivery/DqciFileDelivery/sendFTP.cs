using System;

namespace DqciFileDelivery
{
    public class SendFtp
    {
        public static string GetFileName(string fullPath)
        {
            return fullPath.Substring(fullPath.LastIndexOf("\\") + 1);
        }

        public string SendFileToFtp(CustomerDetails customer, string filename)
        {
            string hostIP = customer.FTPURL;
            string userName = customer.FTPUserName;
            string passWord = customer.FTPPassword;

            string status = string.Empty;
            string fileNameOnly = GetFileName(filename);

            customer.CustomerSpecificFileName = RenameReadyToSendFiles.RenameFiles(customer, fileNameOnly);

            try
            {
                FTPClient ftpClient = new FTPClient();

                status = "Assign FTP credentials " + hostIP;
                ftpClient.SetUserNameAndPassword(hostIP, userName, passWord);

                status = "FTP credentials are assigned " + hostIP;
                if (!ftpClient.CheckDirectoryExists())
                {
                    status = "FTP directory not  Exists " + hostIP;
                    if (!ftpClient.CreateHostDirectory())
                    {
                        //even if above 2 checks failed and OverRidePreFtpChecks = true proceed to upload.
                        if (!customer.OverRidePreFTPChecks)
                        {
                            status = "FTP could not Create Host Directory " + hostIP;
                            return status;
                        }
                    }
                }
                status = "Path Success " + hostIP;

                if (ftpClient.CheckIfFileExists(customer.CustomerSpecificFileName))
                {
                    status = "File exists " + hostIP + " file name " + customer.CustomerSpecificFileName;

                    return status;
                }

                status = ftpClient.Upload(customer.CustomerSpecificFileName, filename);

                if (String.IsNullOrEmpty(status))
                {
                    status = "File sent to FTP";
                }
            }
            catch (Exception ex)
            {
                Logger.WriteException(ex);
                status = status + ex.Message;
            }

            return status;
        }
    }
}