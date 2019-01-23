using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace DqciFileDelivery
{
    class RenameReadyToSendFiles
    {
        
        public static string RenameFiles(CustomerDetails customer, string fileName)
        {
            if (!customer.IsFullName & customer.SendFileName != null)
            {
                switch (customer.CustomerCode)
                {
                    case "AUS023973":
                        fileName = string.Format("{0}{1}{2}{3}{4}", ConfigurationManager.AppSettings.Get("FileLocations"), customer.SendFileName, "_", 
                            FormatDate("ddMMyyyy_HHmmss"), ".CSV");
                        break;
                }
            }
            else if(customer.IsFullName & customer.SendFileName != null)
            {
                switch (customer.CustomerCode)
                {
                    case "A00292829IAV":
                        fileName = string.Format("{0}{1}", ConfigurationManager.AppSettings.Get("FileLocations"), customer.SendFileName);
                        break;
                }
            }

            return fileName;
        }

        public static string FormatDate(string dateString)
        {
            string date = DateTime.Now.ToString(dateString);

            return date;
        }
    }
}
