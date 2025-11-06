using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Interfaces
{
    public interface IRedisSentinelRepository
    {
        Task<bool> SaveAsync<T>(string key, T data, TimeSpan? expiry = null);
        //Task<T> GetAsync<T>(string key);
        //Task<bool> DeleteAsync(string key);
        //Task<long> PublishAsync(string channel, string message);
    }
}
