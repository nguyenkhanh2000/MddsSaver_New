using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMUSchema : BaseMessageSchema
    {
        // --- Payload (Disclosure Specific) ---
        public const string SecurityExchange      = "securityexchange";
        public const string Symbol                = "symbol";
        public const string SymbolName            = "symbolname";
        public const string DisclosureId          = "disclosureid";
        public const string TotalMsgNo            = "totalmsgno";
        public const string CurrentMsgNo          = "currentmsgno";
        public const string LanquageCategory      = "lanquagecategory";
        public const string DataCategory          = "datacategory";
        public const string PublicInformationDate = "publicinformationdate";
        public const string TransmissionDate      = "transmissiondate";
        public const string ProcessType           = "processtype";
        public const string Headline              = "headline";
        public const string Body                  = "body";
    }
}
