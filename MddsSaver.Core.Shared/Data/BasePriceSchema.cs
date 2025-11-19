using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class BasePriceSchema : BaseMessageSchema
    {
        // --- Các trường chung cho Price (x) và PriceRecovery (w) ---
        public const string TradingSessionId  = "tradingsessionid";
        public const string Symbol            = "symbol";

        // --- Stats ---
        public const string TotalVolumeTraded = "totalvolumetraded";
        public const string GrossTradeAmt     = "grosstradeamt";
        public const string SellTotOrderQty   = "selltotorderqty";
        public const string BuyTotOrderQty    = "buytotorderqty";
        public const string SellValidOrderCnt = "sellvalidordercnt";
        public const string BuyValidOrderCnt  = "buyvalidordercnt";

        // --- Looped Column Prefixes & Suffixes ---
        public const string BpPrefix          = "bp"; // Buy Price
        public const string BqPrefix          = "bq"; // Buy Quantity
        public const string SpPrefix          = "sp"; // Sell Price
        public const string SqPrefix          = "sq"; // Sell Quantity

        public const string Suffix_Noo        = "_noo";
        public const string Suffix_Mdey       = "_mdey";
        public const string Suffix_Mdemms     = "_mdemms";
        public const string Suffix_Mdepno     = "_mdepno";
    }
}
