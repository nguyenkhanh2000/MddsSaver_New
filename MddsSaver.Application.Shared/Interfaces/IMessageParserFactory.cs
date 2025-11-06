using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Application.Shared.Interfaces
{
    public interface IMessageParserFactory
    {
        Task<object> Parse(string rawMessage, string msgType);
        string GetMsgType(string rawMessage);
    }
}
