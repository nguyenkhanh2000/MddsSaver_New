using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMJSchema : BaseMessageSchema
    {
        // --- Payload (Drv Product Event Specific) ---
        public const string ProductId                 = "productid";
        public const string EventKindCode             = "eventkindcode";
        public const string EventOccurrenceReasonCode = "eventoccurrencereasoncode";
        public const string EventStartDate            = "eventstartdate";
        public const string EventEndDate              = "eventenddate";
    }
}
