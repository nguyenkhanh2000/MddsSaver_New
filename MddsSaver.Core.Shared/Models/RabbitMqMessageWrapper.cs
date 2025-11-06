using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class RabbitMqMessageWrapper
    {
        public ulong DeliveryTag { get; set; }
        public object ParsedMessage { get; set; }
    }
}
