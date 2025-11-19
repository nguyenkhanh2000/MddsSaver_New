using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMQSchema : BaseMessageSchema
    {
        // --- Payload (Top N Symbols Price Specific) ---
        public const string TotNumReports = "totnumreports";
        public const string Rank          = "rank";
        public const string Symbol        = "symbol";
        public const string MDEntryPx     = "mdentrypx";
    }
}
