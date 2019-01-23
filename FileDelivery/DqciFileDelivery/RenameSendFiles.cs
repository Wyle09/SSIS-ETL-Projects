using System;

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
                    case "AUS023973": // DFA Mounds View Producers.
                        fileName = string.Format("{0}{1}{2}{3}", customer.SendFileName, "_", FormatDate("ddMMyyyy_HHmmss"), ".CSV");
                        break;

                    case "A00564332QXD": // DFA East Producers.
                        fileName = string.Format("{0}{1}{2}{3}", customer.SendFileName, "_", FormatDate("ddMMyyyy_HHmmss"), ".CSV");
                        break;

                    case "A00525594BOD": // Davisco Producers.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "A00292720XU0": // Agropur.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "CFA00525594BOD2": // Davisco Laken Loads.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "CFA00525594BOD3": // Davisco Lesueur Loads.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "A00525589AR1": // Saputo.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "AUS022004": // Deans.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "A00381487UEP": // NDP East.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "AUS001375": // AMPI.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "A003322065Z9": // Grande Cheese.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;
                        
                    case "A00099666": // Foremost.
                        fileName = string.Format("{0}{1}", customer.SendFileName, FormatDate("yyMMddHHmm"));
                        break;

                    case "A00148736": // Stickney Hills.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;
                        
                    case "A00525590TP8": // Horizon Organic.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("yyyyMMdd.HH"), ".CSV");
                        break;

                    case "A00525584UMA": // Crop (Organic Valley producers).
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "CFA00525584UMA1": // Crop (Organic Valley loads).
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;
                        
                    case "A00525586ERF": // NFO.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "AUS022732": // Schroeder loads.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;

                    case "A00525581RJR": // NFO.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;
                        
                    case "CFA00525589AR1Loads": // NFO.
                        fileName = string.Format("{0}{1}{2}", customer.SendFileName, FormatDate("MMddHHmm"), ".CSV");
                        break;
                }
            }
            else if(customer.IsFullName & customer.SendFileName != null)
            {
                switch (customer.CustomerCode)
                {
                    case "A00292829IAV": // LOL.
                        fileName = string.Format("{0}", customer.SendFileName);
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
