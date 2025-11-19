using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgFSchema : BaseMessageSchema
    {
        // --- Payload (Security Status Specific) ---
        public const string TscProdGrpId      = "tscprodgrpid";
        public const string BoardEvtId        = "boardevtid";
        public const string SessOpenCloseCode = "sessopenclosecode";
        public const string Symbol            = "symbol";
        public const string TradingSessionId  = "tradingsessionid";
        public const string HaltRsnCode       = "haltrsncode";
        public const string ProductId         = "productid";
    }
}
