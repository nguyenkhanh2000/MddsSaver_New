using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMHSchema : BaseMessageSchema
    {
        // --- Payload (Market Maker Specific) ---
        public const string MarketMakerContractCode = "marketmakercontractcode";
        public const string MemberNo                = "memberno";
    }
}
