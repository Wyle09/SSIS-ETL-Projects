using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace DqciFileDelivery
{
    
    public class SMTPHelper 
    {
        private SmtpClient _smtpClient
        {
            get
            {
                return new SmtpClient
                {
                    UseDefaultCredentials = false,
                    Credentials = new NetworkCredential(UserName, Password),
                    Host = Host,
                    Port = Port,
                    DeliveryMethod = SmtpDeliveryMethod.Network
                };
            }
        }

        private string _host;
        private string _userName;
        private string _password;
        private int _port;

        /// <summary>
        /// Sends the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Send(MailMessage message)
        {
            _smtpClient.Send(message);
        }

        public string Host
        {
            get
            {
                return _host;
            }
            set
            {
                _host = value;
            }
        }

        public string UserName
        {
            get
            {
                return _userName;
            }
            set
            {
                _userName = value;
            }
        }

        public string Password
        {
            get
            {
                return _password;
            }
            set
            {
                _password = value;
            }
        }

        public int Port
        {
            get
            {
                return _port;
            }
            set
            {
                _port = value;
            }
        }
    }
}
