using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgWSchema : BasePriceSchema
    {
        // --- Các trường giá duy nhất của Msg_W ---
        public const string OpnPx             = "opnpx";
        public const string TrdSessnHighPx    = "trdsessnhighpx";
        public const string TrdSessnLowPx     = "trdsessnlowpx";
        public const string SymbolCloseInfoPx = "symbolcloseinfopx";
        public const string OpnPxYld          = "opnpxyld";
        public const string TrdSessnHighPxYld = "trdsessnhighpxyld";
        public const string TrdSessnLowPxYld  = "trdsessnlowpxyld";
        public const string ClsPxYld          = "clspxyld";
    }
}
