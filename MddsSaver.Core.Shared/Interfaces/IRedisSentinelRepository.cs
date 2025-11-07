using MddsSaver.Core.Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Interfaces
{
    public interface IRedisSentinelRepository
    {
        //Task<bool> SaveAsync(string key, string data, TimeSpan? expiry = null);
        Task<bool> ExecuteBatchAsync(IEnumerable<RedisCommand> commands);
    }
}
