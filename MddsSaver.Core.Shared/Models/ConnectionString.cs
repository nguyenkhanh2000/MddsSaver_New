using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class ConnectionString
    {
        public string Oracle {  get; set; }
        public static ConnectionString MapValue(IConfiguration configuration)
        {
            return configuration.Get<ConnectionString>();
        }
    }
}
