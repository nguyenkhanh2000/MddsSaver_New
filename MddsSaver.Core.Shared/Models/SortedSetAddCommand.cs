using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class SortedSetAddCommand : RedisCommand
    {
        public string Key { get; set; }
        public string Member { get; set; }
        public double Score { get; set; }
    }
}
