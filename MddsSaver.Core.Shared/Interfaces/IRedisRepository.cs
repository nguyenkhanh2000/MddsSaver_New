using MddsSaver.Core.Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Interfaces
{
    public interface IRedisRepository
    {
        Task<bool> ExecuteBatchAsync(IEnumerable<RedisCommand> commands);
        Task<bool> SaveAsync(string key, string data, TimeSpan? expiry = null);
        //Task<T> GetAsync<T>(string key);
        Task<bool> DeleteAsync(string key);
        Task<long> PublishAsync(string channel, string message);
        Task<bool> UpdateSortedSetByScoreAsync(string keyVal, string keyVol, double score, string value);
        Task<bool> SortedSetAddAsync(string key, string member, double score);
        string GetString(string key);
    }
}
