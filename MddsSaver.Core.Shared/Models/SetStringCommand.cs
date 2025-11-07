using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class SetStringCommand : RedisCommand
    {
        public string Key {  get; set; }
        public string Value { get; set; }
        public int Period { get; set; } 
    }
}
