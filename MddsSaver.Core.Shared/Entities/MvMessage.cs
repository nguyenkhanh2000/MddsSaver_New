using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Entities
{
    public class MvMessage
    {
        public string MsgType { get; set; } = "MV";
        public string RawMessage { get; set; }
    }
}
