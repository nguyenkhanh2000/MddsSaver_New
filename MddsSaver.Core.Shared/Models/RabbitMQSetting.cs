using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class RabbitMQSetting
    {
        public string HostName      { get; set; }
        public int    Port          { get; set; }
        public string Username      { get; set; }
        public string Password      { get; set; }
        public string QueueName     { get; set; }
        public string ExchangeName  { get; set; }
        public string RoutingKey    { get; set; }
        public int    BatchSize     { get; set; }
        public int    TimeDelay     { get; set; }
        public ushort PrefetchCount { get; set; }

        public static RabbitMQSetting MapValue(IConfiguration configuration)
        {
            return configuration.Get<RabbitMQSetting>();
        }
    }
}
