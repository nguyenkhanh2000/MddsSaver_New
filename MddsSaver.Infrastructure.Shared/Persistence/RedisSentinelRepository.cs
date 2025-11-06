using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;

namespace MddsSaver.Infrastructure.Shared.Persistence
{
    public class RedisSentinelRepository : IRedisSentinelRepository
    {
        private readonly ILogger<RedisSentinelRepository> _logger;
        private readonly IDatabase _redisSentinel;
        private readonly IConnectionMultiplexer _mux_250;
        public RedisSentinelRepository(ILogger<RedisSentinelRepository> logger, IConnectionMultiplexer redisSentinel)
        {
            _logger = logger;
            _redisSentinel = redisSentinel.GetDatabase();
        }
        public async Task<bool> SaveAsync<T>(string key, T data, TimeSpan? expiry = null)
        {
            string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + key;
            string jsonValue;

            try
            {
                // Serialize 1 lần (ĐÃ SỬA LỖI)
                //jsonValue = JsonSerializer.Serialize(data, typeof(T));
                jsonValue = JsonSerializer.Serialize(data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi Serialize data (Key: {RedisKey})", key);
                return false;
            }

            try
            {
                // ... (phần code dual-write của bạn) ...
                var taskP1 = _redisSentinel.StringSetAsync(key, jsonValue, expiry);
                var taskP2 = _redisSentinel.StringSetAsync(backupKey, jsonValue, expiry);

                await Task.WhenAll(taskP1, taskP2);

                return taskP1.Result && taskP2.Result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi dual-write vào Redis (Key: {RedisKey})", key);
                return false;
            }
        }
    }
}
