using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMPSchema : BaseMessageSchema
    {
        // --- Payload (Top N Symbols Specific) ---
        public const string TotNumReports = "totnumreports";
        public const string Rank          = "rank";
        public const string Symbol        = "symbol";
        public const string MDEntrySize   = "mdentrysize";
    }
}
