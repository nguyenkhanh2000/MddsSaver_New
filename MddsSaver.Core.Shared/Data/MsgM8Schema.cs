using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM8Schema : BaseMessageSchema
    {
        // --- Payload (Closing Info Specific) ---
        public const string Symbol                = "symbol";
        public const string SymbolCloseInfoPx     = "symbolcloseinfopx";
        public const string SymbolCloseInfoYield  = "symbolcloseinfoyield";
        public const string SymbolCloseInfoPxType = "symbolcloseinfopxtype";
    }
}
