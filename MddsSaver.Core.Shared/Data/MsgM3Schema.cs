using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM3Schema : BaseMessageSchema
    {
        // --- Payload (Investor/Symbol Specific) ---
        public const string Symbol          = "symbol";
        public const string InvestCode      = "investcode";
        public const string SellVolume      = "sellvolume";
        public const string SellTradeAmount = "selltradeamount";
        public const string BuyVolume       = "buyvolume";
        public const string BuyTradedAmount = "buytradedamount";
    }
}
