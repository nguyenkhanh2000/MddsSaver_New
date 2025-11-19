using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMLSchema : BaseMessageSchema
    {
        // --- Payload (Index Constituents Specific) ---
        public const string MarketIndexClass = "marketindexclass";
        public const string IndexsTypeCode   = "indexstypecode";
        public const string Currency         = "currency";
        public const string IdxName          = "idxname";
        public const string IdxEnglishName   = "idxenglishname";
        public const string TotalMsgNo       = "totalmsgno";
        public const string CurrentMsgNo     = "currentmsgno";
        public const string Symbol           = "symbol";
    }
}
