using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class BaseMessageSchema
    {
        // Header
        public const string BeginString  = "beginstring";
        public const string BodyLength   = "bodylength";
        public const string MsgType      = "msgtype";
        public const string SenderCompId = "sendercompid";
        public const string TargetCompId = "targetcompid";
        public const string MsgSeqNum    = "msgseqnum";
        public const string SendingTime  = "sendingtime";
        public const string MarketId     = "marketid";
        public const string BoardId      = "boardid";

        // Footer
        public const string Checksum     = "checksum";
        public const string CreateTime   = "createtime";
    }
}
