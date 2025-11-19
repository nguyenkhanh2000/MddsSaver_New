using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM4Schema : BaseMessageSchema
    {
        // --- Payload (Top N Members Specific) ---
        public const string Symbol          = "symbol";
        public const string TotNumReports   = "totnumreports";
        public const string SellRankSeq     = "sellrankseq";
        public const string SellMemberNo    = "sellmemberno";
        public const string SellVolume      = "sellvolume";
        public const string SellTradeAmount = "selltradeamount";
        public const string BuyRankSeq      = "buyrankseq";
        public const string BuyMemberNo     = "buymemberno";
        public const string BuyVolume       = "buyvolume";
        public const string BuyTradedAmount = "buytradedamount";
    }
}
