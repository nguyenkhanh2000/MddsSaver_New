using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM2Schema : BaseMessageSchema
    {
        // --- Payload (Investor/Industry Specific) ---
        public const string TransactTime           = "transacttime";
        public const string MarketIndexClass       = "marketindexclass";
        public const string IndexsTypeCode         = "indexstypecode";
        public const string Currency               = "currency";
        public const string InvestCode             = "investcode";
        public const string SellVolume             = "sellvolume";
        public const string SellTradeAmount        = "selltradeamount";
        public const string BuyVolume              = "buyvolume";
        public const string BuyTradedAmount        = "buytradedamount";
        public const string BondClassificationCode = "bondclassc";
        public const string SecurityGroupId        = "securitygid";
    }
}
