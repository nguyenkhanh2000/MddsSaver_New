using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMTSchema : BaseMessageSchema
    {
        // --- Payload (Foreign Trading Result Specific) ---
        public const string Symbol               = "symbol";
        public const string TradingSessionId     = "tradingsessionid";
        public const string TransactTime         = "transacttime";
        public const string FornInvestTypeCode   = "forninvesttypecode";
        public const string SellVolume           = "sellvolume";
        public const string SellTradeAmount      = "selltradeamount";
        public const string BuyVolume            = "buyvolume";
        public const string BuyTradedAmount      = "buytradedamount";
        public const string SellVolumeTotal      = "sellvolumetotal";
        public const string SellTradeAmountTotal = "selltradeamounttotal";
        public const string BuyVolumeTotal       = "buyvolumetotal";
        public const string BuyTradedAmountTotal = "buytradedamounttotal";
    }
}
