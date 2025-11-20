using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HnxMddsSaverRD
{
    public sealed class RedisMessageTypeFilter : IMessageTypeFilter
    {
        private static readonly HashSet<string> _allowed = new() { EPrice.__MSG_TYPE, ESecurityDefinition.__MSG_TYPE };
        public bool Accept(string msgType) => msgType != null && _allowed.Contains(msgType);
    }
}
