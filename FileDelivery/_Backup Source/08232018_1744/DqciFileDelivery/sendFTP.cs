using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DqciFileDelivery
{
    public class sendFTP
    {

        public static string GetFileName(string fullPath)
        {
            return fullPath.Substring(fullPath.LastIndexOf("\\") + 1);
        }
        public string  SendFileToFtp(CustomerDetails customer, string filename)
        {
            string hostIP = customer.FTPURL; 
            string userName=customer.FTPUserName;
            string passWord = customer.FTPPassword;

            string status = string.Empty;
            string fileNameOnly = GetFileName(filename);

            try
            {
                FTPClient _ftpClient = new FTPClient();

                status = "Assign FTP credentials " + hostIP;
                _ftpClient.SetUserNameAndPassword(hostIP, userName, passWord);

                status = "FTP credentials are assigned " + hostIP;
                if (!_ftpClient.CheckDirectoryExists())
                {
                    status = "FTP directory not  Exists " + hostIP;
                    if (!_ftpClient.CreateHostDirectory())
                    {
                        status = "FTP could not Create Host Directory " + hostIP;
                        return status;
                    }
                }
                status = "Path Success " + hostIP;

                if (_ftpClient.CheckIfFileExists(fileNameOnly))
                {
                    status = "File exists " + hostIP + " file name " + fileNameOnly;

                    return status;
                }

                status = _ftpClient.Upload(fileNameOnly, filename);

                if (String.IsNullOrEmpty(status))
                {
                    status = "File sent to FTP";
                }
            }
            catch (Exception ex)
            {

                status = status + ex.Message;
            }


            return status;
        }
    }
}
