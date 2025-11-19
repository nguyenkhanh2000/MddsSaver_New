using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgMDSchema : BaseMessageSchema
    {
        // --- Payload (Volatility Interruption Specific) ---
        public const string Symbol                  = "symbol";
        public const string VITypeCode              = "vitypecode";
        public const string VIKindCode              = "vikindcode";
        public const string StaticVIBasePrice       = "staticvibaseprice";
        public const string DynamicVIBasePrice      = "dynamicvibaseprice";
        public const string VIPrice                 = "viprice";
        public const string StaticVIDispartiyRatio  = "staticvidispartiyratio";
        public const string DynamicVIDispartiyRatio = "dynamicvidispartiyratio";
    }
}
