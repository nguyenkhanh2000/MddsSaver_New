using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class ServiceSetting
    {
        public string Name      { get; set; }
        public string Version   { get; set; }
        public string Namespace { get; set; }

        public static ServiceSetting MapValue(IConfiguration configuration)
        {
            return configuration.Get<ServiceSetting>();
        }
    }
}
