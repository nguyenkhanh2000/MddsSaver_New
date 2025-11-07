using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class RedisSettings
    {
        public string Endpoint_Fox250 { get; set; }
        public string Endpoint_Fox251 { get; set; }
        public string Endpoint_Sentinel { get; set; }
        public string KeyAppName_Consume { get; set; }
        public string KeyAppName_Proc    { get; set; }
        public int DatabaseNumber_Fox { get; set; }
        public int DatabaseNumber_Sentinel {  get; set; }   
        public static RedisSettings MapValue(IConfiguration configuration)
        {
            return configuration.Get<RedisSettings>();
        }
    }
}
