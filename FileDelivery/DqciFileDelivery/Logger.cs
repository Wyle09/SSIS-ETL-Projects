using System;
using System.Collections;
using System.Configuration;
using System.IO;
using System.Text;
using System.Threading;

namespace DqciFileDelivery
{
    public static class Logger
    {
        private static readonly string LogFile = string.Empty;
        private static readonly string LogFilePath = ConfigurationManager.AppSettings.Get("LogPath");


        static Logger()
        {
            string todaysDate = DateTime.Now.ToString("yyyyMMdd") +".txt";
            LogFile = Path.Combine(LogFilePath, todaysDate);
        }

        public static void WriteLine(string text, string filePath)
        {
            File.AppendAllText(filePath, text + Environment.NewLine);
        }

        public static void WriteLine(string text)
        {
            WriteLine(text,LogFile);
        }

        public static void WriteException(Exception ex)
        {
            string errorMessage = GetFormattedExceptionDetails(ex);
            WriteLine(errorMessage);
        }

        public static string GetFormattedExceptionDetails(Exception ex)
        {
            // Initialize the string builder and append the exception details in the same
            var strBuilder = new StringBuilder();
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "Message: {0}{1}", ex.Message,
                Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "Type: {0}{1}", ex.GetType(),
                Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "HelpLink: {0}{1}", ex.HelpLink,
                Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "Source: {0}{1}", ex.Source,
                Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "TargetSite: {0}{1}", ex.TargetSite,
                Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "InnerException Message: {0}{1}",
                ex.InnerException != null ? ex.InnerException.Message : string.Empty, Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "InnerException Stack trace: {0}{1}",
                ex.InnerException != null ? ex.InnerException.StackTrace : string.Empty, Environment.NewLine);
            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "Data:{0}", Environment.NewLine);

            // Append the each and every dictionary entry from data into string builder
            foreach (DictionaryEntry dictionaryEntry in ex.Data)
            {
                strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "\t{0} : {1}{2}", dictionaryEntry.Key,
                    dictionaryEntry.Value, Environment.NewLine);
            }

            strBuilder.AppendFormat(Thread.CurrentThread.CurrentCulture, "StackTrace: {0}{1}", ex.StackTrace,
                Environment.NewLine);

            // Return the formatted string
            return strBuilder.ToString();
        }
    }
}