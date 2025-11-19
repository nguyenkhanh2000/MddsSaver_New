using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMXSchema : BaseMessageSchema
    {
        // --- Payload (Price Limit Expansion Specific) ---
        public const string Symbol         = "symbol";
        public const string HighLimitPrice = "highlimitprice";
        public const string LowLimitPrice  = "lowlimitprice";
        public const string PleUpLmtStep   = "pleuplmtstep";
        public const string PleLwLmtStep   = "plelwlmtstep";
    }
}
