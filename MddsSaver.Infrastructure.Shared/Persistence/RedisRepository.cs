using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Infrastructure.Shared.Services;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Text.Json;
namespace MddsSaver.Infrastructure.Shared.Persistence
{
    public class RedisRepository : IRedisRepository
    {
        private readonly ILogger<RedisRepository> _logger;
        private readonly IDatabase _redisFox_250;
        private readonly IDatabase _redisFox_251;
        private readonly IConnectionMultiplexer _mux_250;
        public RedisRepository(ILogger<RedisRepository> logger, IConnectionMultiplexer redis_250, IConnectionMultiplexer redis_251) 
        {
            _logger = logger;
            _mux_250 = redis_250;
            _redisFox_250 = redis_250.GetDatabase();
            _redisFox_251 = redis_251.GetDatabase();
        }
        // 1. LOGIC GHI (DUAL-WRITE)
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
                var taskP1 = _redisFox_250.StringSetAsync(key, jsonValue, expiry);
                var taskP2 = _redisFox_250.StringSetAsync(backupKey, jsonValue, expiry);
                var taskS1 = _redisFox_251.StringSetAsync(key, jsonValue, expiry);
                var taskS2 = _redisFox_251.StringSetAsync(backupKey, jsonValue, expiry);

                await Task.WhenAll(taskP1, taskP2, taskS1, taskS2);

                return taskP1.Result && taskP2.Result && taskS1.Result && taskS2.Result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi dual-write vào Redis (Key: {RedisKey})", key);
                return false;
            }
        }

        // 2. LOGIC ĐỌC (FAILOVER-READ)
        public async Task<T> GetAsync<T>(string key)
        {
            RedisValue redisValue = RedisValue.Null;
            try
            {
                // Thử đọc ở Primary trước
                redisValue = await _redisFox_250.StringGetAsync(key);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Lỗi khi GetAsync từ Primary Redis (Key: {RedisKey}). Thử Secondary...", key);
            }

            // Nếu Primary không có hoặc lỗi -> Thử Secondary
            if (redisValue.IsNullOrEmpty)
            {
                try
                {
                    redisValue = await _redisFox_251.StringGetAsync(key);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi GetAsync từ Secondary Redis (Key: {RedisKey})", key);
                    return default(T);
                }
            }

            if (redisValue.IsNullOrEmpty)
            {
                return default(T); // Không tìm thấy ở cả 2
            }

            return JsonSerializer.Deserialize<T>(redisValue);
        }

        // 3. LOGIC XÓA (DUAL-DELETE)
        public async Task<bool> DeleteAsync(string key)
        {
            try
            {
                var taskP = _redisFox_250.KeyDeleteAsync(key);
                var taskS = _redisFox_251.KeyDeleteAsync(key);

                var results = await Task.WhenAll(taskP, taskS);

                // Trả về true nếu xóa thành công trên CẢ HAI
                return results[0] && results[1];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi dual-delete trong Redis (Key: {RedisKey})", key);
                return false;
            }
        }
        // 4. Publish async
        public async Task<long> PublishAsync(string channel, string message)
        {
            try
            {
                // Lấy subscriber từ multiplexer primary
                ISubscriber sub = _mux_250.GetSubscriber();

                // Publish message
                return await sub.PublishAsync(channel, message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi PublishAsync (Channel: {RedisChannel})", channel);
                return -1; // Trả về -1 để báo lỗi
            }
        }
    }
}
