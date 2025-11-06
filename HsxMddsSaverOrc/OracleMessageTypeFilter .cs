using MddsSaver.Core.Shared.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HsxMddsSaverOrc
{
    public sealed class OracleMessageTypeFilter : IMessageTypeFilter
    {
        public bool Accept(string msgType) => true;
    }
}
