using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DqciFileDelivery
{
    public class CustomerDetails
    {
        public string CustomerCode { get; set; }
        public string Mode { get; set; }
        public string emailList { get; set; }
        public string FTPURL { get; set; }
        public string FTPType { get; set; }
        public string FTPUserName { get; set; }
        public string FTPPassword { get; set; }
        public string SendFileName { get; set; }
        public bool IsFullName { get; set; }

    }

    public class CustomerFiles
    {
        public string CustomerCode { get; set; }
        public string FilePath { get; set; }
    }

    public class CustomerSendFiles
    {
        public List<CustomerFiles> ReadCustomerFiles()
        {
            // Read the files from folder
            string fileLocations = ConfigurationManager.AppSettings.Get("FileLocations");
            
            List<CustomerFiles> listOfCustomers = new List<CustomerFiles>();
            foreach (string sourcFile in Directory.EnumerateFiles(fileLocations, "*.*"))
            {
                string fileName = sendEmails.GetFileName(sourcFile);
                string[] words = fileName.Split('-');

                if (words.Length > 1)
                {
                    if (listOfCustomers.FirstOrDefault(c => String.Equals(c.CustomerCode, words[0], StringComparison.CurrentCultureIgnoreCase))==null)
                    {
                        listOfCustomers.Add(new CustomerFiles
                        {
                            CustomerCode = words[0].ToUpper(),
                            FilePath = sourcFile
                        });
                    }
                }

            }
            return listOfCustomers;
        }

        public List<CustomerDetails> GetCustomerDetailses(List<CustomerFiles> customersFiles, string _connectionString)
        {
            List<CustomerDetails> listOfCustomerDetails=new List<CustomerDetails>();

            DataTable dtCustomers = helperClass.ConvertToDataTable(customersFiles);

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand())
                {
                    command.Connection = connection;
                    command.CommandType = CommandType.Text;

                    command.CommandText = @"CREATE TABLE #CusomersFiles (CustomerCode nvarchar(250), FilePath nvarchar(2500))";
                    command.ExecuteNonQuery();

                    using (SqlBulkCopy sbc = new SqlBulkCopy(connection))
                    {
                        sbc.BulkCopyTimeout = 60;
                        sbc.DestinationTableName = "#CusomersFiles";
                        sbc.WriteToServer(dtCustomers);
                        sbc.Close();
                    }

                    command.CommandText = string.Format(@"
                                                        SELECT DISTINCT
                                                        CD.CustomerCode,
                                                        DeliveryMode,
                                                        eMails,
                                                        FTPUrl,
                                                        FTPUserName,
                                                        FTPPassword,
                                                        FTPType,
                                                        SendFileName,
                                                        IsFullName
                                                        FROM #CusomersFiles CF
                                                        JOIN CustomerDetails CD WITH (NOLOCK) ON CD.CustomerCode=CF.CustomerCode
                                                        ");

                    command.CommandTimeout = 120;

                    using (var reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {

                            int CustomerCodeOrdinal = reader.GetOrdinal("CustomerCode");
                            int DeliveryModeOrdinal = reader.GetOrdinal("DeliveryMode");
                            int eMailsOrdinal = reader.GetOrdinal("eMails");
                            int FTPUrlOrdinal = reader.GetOrdinal("FTPUrl");
                            int FTPUserNameOrdinal = reader.GetOrdinal("FTPUserName");
                            int FTPPasswordOrdinal = reader.GetOrdinal("FTPPassword");
                            int FTPTypeOrdinal = reader.GetOrdinal("FTPType");
                            int SendFileNameOrdinal = reader.GetOrdinal("SendFileName");
                            int IsFullNameOrdinal = reader.GetOrdinal("IsFullName");

                            while (reader.Read())
                            {
                                listOfCustomerDetails.Add(new CustomerDetails
                                {
                                   
                                    CustomerCode =reader.SafeGetString(CustomerCodeOrdinal),
                                    Mode = reader.SafeGetString(DeliveryModeOrdinal),
                                    emailList = reader.SafeGetString(eMailsOrdinal),
                                    FTPURL = reader.SafeGetString(FTPUrlOrdinal),
                                    FTPUserName = reader.SafeGetString(FTPUserNameOrdinal),
                                    FTPPassword = reader.SafeGetString(FTPPasswordOrdinal),
                                    FTPType = reader.SafeGetString(FTPTypeOrdinal),
                                    SendFileName = reader.SafeGetString(SendFileNameOrdinal),
                                    IsFullName = bool.Parse(reader.SafeGetString(IsFullNameOrdinal))
                                });

                            }
                        }
                    }
                }
            }

            return listOfCustomerDetails;
        }

       
    }

    public static class DataReaderExtensions
    {
        /// <summary>
        /// Get string with null check 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="colIndex">index</param>
        /// <returns></returns>
        public static string SafeGetString(this SqlDataReader reader, int colIndex)
        {
            if (colIndex >= 0 && !reader.IsDBNull(colIndex))
                return reader.GetString(colIndex);
            else
                return string.Empty;
        }
    }
}
