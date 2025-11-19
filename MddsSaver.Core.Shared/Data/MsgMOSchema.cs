using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMOSchema : BaseMessageSchema
    {
        // --- Payload (ETF Tracking Error Specific) ---
        public const string Symbol         = "symbol";
        public const string TradeDate      = "tradedate";
        public const string TrackingError  = "trackingerror";
        public const string DisparateRatio = "disparateratio";
    }
}
