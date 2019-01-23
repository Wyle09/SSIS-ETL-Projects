using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;
using System.Threading.Tasks;

namespace Eurofins.Milk.FunctionalService.Helper
{
    [Serializable]
    public class SerializeableMailMessage
    {
        private Boolean IsBodyHtml { get; set; }

        private String Body { get; set; }

        private SerializeableMailAddress From { get; set; }

        private SerializeableMailAddress Sender { get; set; }

        private String Subject { get; set; }

        private Encoding BodyEncoding { get; set; }

        private Encoding SubjectEncoding { get; set; }

        private DeliveryNotificationOptions DeliveryNotificationOptions { get; set; }

        private SerializeableCollection Headers { get; set; }

        private MailPriority Priority { get; set; }

        private IList<SerializeableMailAddress> To { get; set; }

        private IList<SerializeableMailAddress> CC { get; set; }

        private IList<SerializeableMailAddress> Bcc { get; set; }

        private IList<SerializeableMailAddress> ReplyToList { get; set; }

        private IList<SerializeableAlternateView> AlternateViews { get; set; }

        private IList<SerializeableAttachment> Attachments { get; set; }

        public SerializeableMailMessage(MailMessage mailMessage)
        {
            To = new List<SerializeableMailAddress>();
            CC = new List<SerializeableMailAddress>();
            Bcc = new List<SerializeableMailAddress>();
            ReplyToList = new List<SerializeableMailAddress>();

            AlternateViews = new List<SerializeableAlternateView>();

            Attachments = new List<SerializeableAttachment>();

            IsBodyHtml = mailMessage.IsBodyHtml;
            Body = mailMessage.Body;
            Subject = mailMessage.Subject;
            From = new SerializeableMailAddress(mailMessage.From);

            foreach (MailAddress ma in mailMessage.To)
            {
                To.Add(new SerializeableMailAddress(ma));
            }

            foreach (MailAddress ma in mailMessage.CC)
            {
                CC.Add(new SerializeableMailAddress(ma));
            }

            foreach (MailAddress ma in mailMessage.Bcc)
            {
                Bcc.Add(new SerializeableMailAddress(ma));
            }

            Attachments = new List<SerializeableAttachment>();
            foreach (Attachment att in mailMessage.Attachments)
            {
                Attachments.Add(new SerializeableAttachment(att));
            }

            BodyEncoding = mailMessage.BodyEncoding;

            DeliveryNotificationOptions = mailMessage.DeliveryNotificationOptions;
            Headers = new SerializeableCollection(mailMessage.Headers);
            Priority = mailMessage.Priority;

            foreach (MailAddress ma in mailMessage.ReplyToList)
            {
                ReplyToList.Add(new SerializeableMailAddress(ma));
            }

            if (mailMessage.Sender != null)
            {
                Sender = new SerializeableMailAddress(mailMessage.Sender);
            }

            SubjectEncoding = mailMessage.SubjectEncoding;

            foreach (AlternateView av in mailMessage.AlternateViews)
            {
                AlternateViews.Add(new SerializeableAlternateView(av));
            }
        }

        public MailMessage GetMailMessage()
        {
            var mailMessage = new MailMessage()
            {
                IsBodyHtml = IsBodyHtml,
                Body = Body,
                Subject = Subject,
                BodyEncoding = BodyEncoding,
                DeliveryNotificationOptions = DeliveryNotificationOptions,
                Priority = Priority,
                SubjectEncoding = SubjectEncoding,
            };

            if (From != null)
            {
                mailMessage.From = From.GetMailAddress();
            }

            foreach (var mailAddress in To)
            {
                mailMessage.To.Add(mailAddress.GetMailAddress());
            }

            foreach (var mailAddress in CC)
            {
                mailMessage.CC.Add(mailAddress.GetMailAddress());
            }

            foreach (var mailAddress in Bcc)
            {
                mailMessage.Bcc.Add(mailAddress.GetMailAddress());
            }

            foreach (var attachment in Attachments)
            {
                mailMessage.Attachments.Add(attachment.GetAttachment());
            }

            Headers.CopyTo(mailMessage.Headers);

            foreach (var mailAddress in ReplyToList)
            {
                mailMessage.ReplyToList.Add(mailAddress.GetMailAddress());
            }

            if (Sender != null)
            {
                mailMessage.Sender = Sender.GetMailAddress();
            }

            foreach (var alternateView in AlternateViews)
            {
                mailMessage.AlternateViews.Add(alternateView.GetAlternateView());
            }

            return mailMessage;
        }
    }

    [Serializable]
    public class SerializeableAlternateView
    {
        Uri BaseUri;
        String ContentId;
        Stream ContentStream;
        SerializeableContentType ContentType;
        IList<SerializeableLinkedResource> LinkedResources = new List<SerializeableLinkedResource>();
        TransferEncoding TransferEncoding;

        public SerializeableAlternateView(AlternateView alternativeView)
        {
            BaseUri = alternativeView.BaseUri;
            ContentId = alternativeView.ContentId;
            ContentType = new SerializeableContentType(alternativeView.ContentType);
            TransferEncoding = alternativeView.TransferEncoding;

            if (alternativeView.ContentStream != null)
            {
                byte[] bytes = new byte[alternativeView.ContentStream.Length];
                alternativeView.ContentStream.Read(bytes, 0, bytes.Length);
                ContentStream = new MemoryStream(bytes);
            }

            foreach (var lr in alternativeView.LinkedResources)
            {
                LinkedResources.Add(new SerializeableLinkedResource(lr));
            }
        }

        public AlternateView GetAlternateView()
        {
            var sav = new AlternateView(ContentStream)
            {
                BaseUri = BaseUri,
                ContentId = ContentId,
                ContentType = ContentType.GetContentType(),
                TransferEncoding = TransferEncoding,
            };

            foreach (var linkedResource in LinkedResources)
            {
                sav.LinkedResources.Add(linkedResource.GetLinkedResource());
            }

            return sav;
        }
    }

    [Serializable]
    public class SerializeableAttachment
    {
        String ContentId;
        SerializeableContentDisposition ContentDisposition;
        SerializeableContentType ContentType;
        Stream ContentStream;
        System.Net.Mime.TransferEncoding TransferEncoding;
        String Name;
        Encoding NameEncoding;

        public SerializeableAttachment(Attachment attachment)
        {
            ContentId = attachment.ContentId;
            ContentDisposition = new SerializeableContentDisposition(attachment.ContentDisposition);
            ContentType = new SerializeableContentType(attachment.ContentType);
            Name = attachment.Name;
            TransferEncoding = attachment.TransferEncoding;
            NameEncoding = attachment.NameEncoding;

            if (attachment.ContentStream != null)
            {
                byte[] bytes = new byte[attachment.ContentStream.Length];
                attachment.ContentStream.Read(bytes, 0, bytes.Length);

                ContentStream = new MemoryStream(bytes);
            }
        }

        public Attachment GetAttachment()
        {
            var attachment = new Attachment(ContentStream, Name)
            {
                ContentId = ContentId,
                ContentType = ContentType.GetContentType(),
                Name = Name,
                TransferEncoding = TransferEncoding,
                NameEncoding = NameEncoding,
            };

            ContentDisposition.CopyTo(attachment.ContentDisposition);

            return attachment;
        }
    }

    [Serializable]
    public class SerializeableCollection
    {
        IDictionary<string, string> Collection = new Dictionary<string, string>();

        public SerializeableCollection() { }

        public SerializeableCollection(NameValueCollection coll)
        {
            foreach (string key in coll.Keys)
            {
                Collection.Add(key, coll[key]);
            }
        }

        public SerializeableCollection(StringDictionary coll)
        {
            foreach (string key in coll.Keys)
            {
                Collection.Add(key, coll[key]);
            }
        }

        public void CopyTo(NameValueCollection scol)
        {
            foreach (String key in Collection.Keys)
            {
                scol.Add(key, this.Collection[key]);
            }
        }

        public void CopyTo(StringDictionary scol)
        {
            foreach (string key in Collection.Keys)
            {
                if (scol.ContainsKey(key))
                {
                    scol[key] = Collection[key];
                }
                else
                {
                    scol.Add(key, Collection[key]);
                }
            }
        }
    }

    [Serializable]
    public class SerializeableContentDisposition
    {
        DateTime CreationDate;
        String DispositionType;
        String FileName;
        Boolean Inline;
        DateTime ModificationDate;
        SerializeableCollection Parameters;
        DateTime ReadDate;
        long Size;

        public SerializeableContentDisposition(ContentDisposition contentDisposition)
        {
            CreationDate = contentDisposition.CreationDate;
            DispositionType = contentDisposition.DispositionType;
            FileName = contentDisposition.FileName;
            Inline = contentDisposition.Inline;
            ModificationDate = contentDisposition.ModificationDate;
            Parameters = new SerializeableCollection(contentDisposition.Parameters);
            ReadDate = contentDisposition.ReadDate;
            Size = contentDisposition.Size;
        }

        public void CopyTo(ContentDisposition contentDisposition)
        {
            contentDisposition.CreationDate = CreationDate;
            contentDisposition.DispositionType = DispositionType;
            contentDisposition.FileName = FileName;
            contentDisposition.Inline = Inline;
            contentDisposition.ModificationDate = ModificationDate;
            contentDisposition.ReadDate = ReadDate;
            contentDisposition.Size = Size;

            Parameters.CopyTo(contentDisposition.Parameters);
        }
    }

    [Serializable]
    internal class SerializeableContentType
    {
        String Boundary;
        String CharSet;
        String MediaType;
        String Name;
        SerializeableCollection Parameters;

        public SerializeableContentType(ContentType contentType)
        {
            Boundary = contentType.Boundary;
            CharSet = contentType.CharSet;
            MediaType = contentType.MediaType;
            Name = contentType.Name;
            Parameters = new SerializeableCollection(contentType.Parameters);
        }

        public ContentType GetContentType()
        {
            var sct = new ContentType()
            {
                Boundary = Boundary,
                CharSet = CharSet,
                MediaType = MediaType,
                Name = Name,
            };

            Parameters.CopyTo(sct.Parameters);

            return sct;
        }
    }

    [Serializable]
    public class SerializeableLinkedResource
    {
        String ContentId;
        Uri ContentLink;
        Stream ContentStream;
        SerializeableContentType ContentType;
        TransferEncoding TransferEncoding;

        public SerializeableLinkedResource(LinkedResource linkedResource)
        {
            ContentId = linkedResource.ContentId;
            ContentLink = linkedResource.ContentLink;
            ContentType = new SerializeableContentType(linkedResource.ContentType);
            TransferEncoding = linkedResource.TransferEncoding;

            if (linkedResource.ContentStream != null)
            {
                var bytes = new byte[linkedResource.ContentStream.Length];
                linkedResource.ContentStream.Read(bytes, 0, bytes.Length);
                ContentStream = new MemoryStream(bytes);
            }
        }

        public LinkedResource GetLinkedResource()
        {
            return new LinkedResource(ContentStream)
            {
                ContentId = ContentId,
                ContentLink = ContentLink,
                ContentType = ContentType.GetContentType(),
                TransferEncoding = TransferEncoding
            };
        }
    }

    [Serializable]
    public class SerializeableMailAddress
    {
        String User;
        String Host;
        String Address;
        String DisplayName;

        public SerializeableMailAddress(MailAddress address)
        {
            User = address.User;
            Host = address.Host;
            Address = address.Address;
            DisplayName = address.DisplayName;
        }

        public MailAddress GetMailAddress()
        {
            return new MailAddress(Address, DisplayName);
        }
    }

    public static class MailExtensions
    {
        public static SerializeableMailMessage ToSerializeableMailMessage(this MailMessage mailMessage)
        {
            return new SerializeableMailMessage(mailMessage);
        }
    }
}
