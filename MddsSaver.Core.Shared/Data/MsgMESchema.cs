using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMESchema : BaseMessageSchema
    {
        // --- Payload (Deem Trade Price Specific) ---
        public const string Symbol             = "symbol";
        public const string ExpectedTradePx    = "expectedtradepx";
        public const string ExpectedTradeQty   = "expectedtradeqty";
        public const string ExpectedTradeYield = "expectedtradeyield";
    }
}
