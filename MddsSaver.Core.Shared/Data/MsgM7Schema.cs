using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM7Schema : BaseMessageSchema
    {
        // --- Payload (Security Info Specific) ---
        public const string Symbol          = "symbol";
        public const string ReferencePrice  = "referenceprice";
        public const string HighLimitPrice  = "highlimitprice";
        public const string LowLimitPrice   = "lowlimitprice";
        public const string EvaluationPrice = "evaluationprice";
        public const string HgstOrderPrice  = "hgstorderprice";
        public const string LwstOrderPrice  = "lwstorderprice";
        public const string ListedShares    = "listedshares";
        public const string ExClassType     = "exclasstype";
    }
}
