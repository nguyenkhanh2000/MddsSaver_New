using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMFSchema : BaseMessageSchema
    {
        // --- Payload (Foreigner Limit Specific) ---
        public const string Symbol                 = "symbol";
        public const string ForeignerBuyPosblQty   = "foreignerbuyposblqty";
        public const string ForeignerOrderLimitQty = "foreignerorderlimitqty";
    }
}
