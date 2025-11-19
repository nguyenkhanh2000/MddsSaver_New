using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMWSchema : BaseMessageSchema
    {
        // --- Payload (Random End Specific) ---
        public const string Symbol                    = "symbol";
        public const string TransactTime              = "transacttime";
        public const string ReApplyClassification     = "reapplyclassification";
        public const string ReTentativeExecutionPrice = "retentativeexecutionprice";
        public const string ReEstimatedHighestPrice   = "reestimatedhighestprice";
        public const string ReEHighestPriceDisparater = "reehighestpricedisparater";
        public const string ReEstimatedLowestPrice    = "reestimatedlowestprice";
        public const string ReELowestPriceDisparater  = "reelowestpricedisparater";
        public const string LatestPrice               = "latestprice";
        public const string LatestPriceDisparateRatio = "latestpricedisparateratio";
        public const string RandomEndReleaseTime      = "randomendreleasetime";
    }
}
