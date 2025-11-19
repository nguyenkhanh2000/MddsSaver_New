using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMASchema : BaseMessageSchema
    {
        // --- Payload (Open Interest Specific) ---
        public const string Symbol          = "symbol";
        public const string TradeDate       = "tradedate";
        public const string OpenInterestQty = "openinterestqty";
        public const string SettlementPrice = "settlementprice";
    }
}
