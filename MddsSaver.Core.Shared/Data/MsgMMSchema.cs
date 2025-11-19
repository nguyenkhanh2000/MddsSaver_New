using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMMSchema : BaseMessageSchema
    {
        // --- Payload (ETF iNAV Specific) ---
        public const string Symbol       = "symbol";
        public const string TransactTime = "transacttime";
        public const string INAVValue    = "inavvalue";
    }
}
