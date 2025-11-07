using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class UpdateSortedSetByScoreCommand : RedisCommand
    {
        public string KeyVal { get; set; }
        public string KeyVol { get; set; }
        public double Score { get; set; }
        public string Value { get; set; }
    }
}
