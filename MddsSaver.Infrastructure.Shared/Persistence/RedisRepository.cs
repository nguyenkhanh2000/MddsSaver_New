using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Infrastructure.Shared.Services;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Text.Json;
using static System.Runtime.InteropServices.JavaScript.JSType;
namespace MddsSaver.Infrastructure.Shared.Persistence
{
    public class RedisRepository : IRedisRepository
    {
        private readonly ILogger<RedisRepository> _logger;
        private readonly IDatabase _redisFox_250;
        private readonly IDatabase _redisFox_251;
        private readonly IConnectionMultiplexer _mux_250;
        public RedisRepository(ILogger<RedisRepository> logger, IConnectionMultiplexer redis_250, IConnectionMultiplexer redis_251, int databaseNumber) 
        {
            _logger = logger;
            _mux_250 = redis_250;
            _redisFox_250 = redis_250.GetDatabase(databaseNumber);
            _redisFox_251 = redis_251.GetDatabase(databaseNumber);
        }
        // 1. LOGIC GHI (DUAL-WRITE)
        public async Task<bool> SaveAsync(string key, string data, TimeSpan? expiry = null)
        {
            string currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var wrappedData = new
            {
                Time = currentTime,
                Data = JsonConvert.DeserializeObject<object>(data)
            };
            string finalValue = JsonConvert.SerializeObject(wrappedData);

            string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + key;
            try
            {
                var taskP1 = _redisFox_250.StringSetAsync(key, finalValue, expiry);
                var taskP2 = _redisFox_250.StringSetAsync(backupKey, finalValue, expiry);
                var taskS1 = _redisFox_251.StringSetAsync(key, finalValue, expiry);
                var taskS2 = _redisFox_251.StringSetAsync(backupKey, finalValue, expiry);

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
        //public async Task<T> GetAsync<T>(string key)
        //{
        //    RedisValue redisValue = RedisValue.Null;
        //    try
        //    {
        //        // Thử đọc ở Primary trước
        //        redisValue = await _redisFox_250.StringGetAsync(key);
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogWarning(ex, "Lỗi khi GetAsync từ Primary Redis (Key: {RedisKey}). Thử Secondary...", key);
        //    }

        //    // Nếu Primary không có hoặc lỗi -> Thử Secondary
        //    if (redisValue.IsNullOrEmpty)
        //    {
        //        try
        //        {
        //            redisValue = await _redisFox_251.StringGetAsync(key);
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "Lỗi khi GetAsync từ Secondary Redis (Key: {RedisKey})", key);
        //            return default(T);
        //        }
        //    }

        //    if (redisValue.IsNullOrEmpty)
        //    {
        //        return default(T); // Không tìm thấy ở cả 2
        //    }

        //    return JsonSerializer.Deserialize<T>(redisValue);
        //}

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
        public async Task<bool> ExecuteTransactionAsync(Action<ITransaction> operations)
        {
            var tran = _redisFox_250.CreateTransaction();
            operations(tran);
            try
            {
                return await tran.ExecuteAsync();
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi thực thi ExecuteTransactionAsync");
                return false;
            }
        }
        public async Task<bool> UpdateSortedSetByScoreAsync(string keyVal, string keyVol, double score, string value)
        {
            try
            {
                // Tạo 2 task song song cho 2 Redis
                var task250 = Task.Run(async () =>
                {
                    await _redisFox_250.SortedSetRemoveRangeByScoreAsync(keyVal, score, score);
                    await _redisFox_250.SortedSetRemoveRangeByScoreAsync(keyVol, score, score);
                    await _redisFox_250.SortedSetAddAsync(keyVal, value, score);
                    await _redisFox_250.SortedSetAddAsync(keyVol, value, score);
                });

                var task251 = Task.Run(async () =>
                {
                    await _redisFox_251.SortedSetRemoveRangeByScoreAsync(keyVal, score, score);
                    await _redisFox_251.SortedSetRemoveRangeByScoreAsync(keyVol, score, score);
                    await _redisFox_251.SortedSetAddAsync(keyVal, value, score);
                    await _redisFox_251.SortedSetAddAsync(keyVol, value, score);
                });

                await Task.WhenAll(task250, task251);

                return true;                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi thực thi dual-write transaction cho UpdateSortedSetByScoreAsync (Score: {Score})", score);
                return false;
            }
        }
        public string GetString(string key)
        {
            try
            {
                string value = _redisFox_250.StringGet(key);
                return value;
            }
            catch (Exception ex) 
            {
                throw;
            }
        }
        public async Task<bool> SortedSetAddAsync(string key, string member, double score)
        {
            try
            {
                // 1. Tạo 2 task (một cho mỗi server)
                var task_250 = _redisFox_250.SortedSetAddAsync(key, member, score);
                var task_251 = _redisFox_251.SortedSetAddAsync(key, member, score);

                // 2. Chờ cả hai hoàn thành
                var results = await Task.WhenAll(task_250, task_251);

                // 3. Trả về true nếu CẢ HAI lệnh đều thành công
                // (Giống logic của SaveAsync và DeleteAsync)
                return results[0] && results[1];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi dual-write SortedSetAddAsync (Key: {RedisKey})", key);
                return false;
            }
        }
        public async Task<bool> ExecuteBatchAsync(IEnumerable<RedisCommand> commands)
        {
            if (commands == null || !commands.Any())
                return true;

            var batch250 = _redisFox_250.CreateBatch();
            var batch251 = _redisFox_251.CreateBatch();

            var tasks250 = new List<Task>();
            var tasks251 = new List<Task>();

            try
            {
                foreach (var cmd in commands)
                {
                    switch (cmd)
                    {
                        case SortedSetAddCommand ssa:
                            tasks250.Add(batch250.SortedSetAddAsync(ssa.Key, ssa.Member, ssa.Score));
                            tasks251.Add(batch251.SortedSetAddAsync(ssa.Key, ssa.Member, ssa.Score));
                            break;
                        case UpdateSortedSetByScoreCommand uss:
                            tasks250.Add(batch250.SortedSetRemoveRangeByScoreAsync(uss.KeyVal, uss.Score, uss.Score));
                            tasks250.Add(batch250.SortedSetAddAsync(uss.KeyVal, uss.Value, uss.Score));
                            tasks250.Add(batch250.SortedSetRemoveRangeByScoreAsync(uss.KeyVol, uss.Score, uss.Score));
                            tasks250.Add(batch250.SortedSetAddAsync(uss.KeyVol, uss.Value, uss.Score));

                            tasks251.Add(batch251.SortedSetRemoveRangeByScoreAsync(uss.KeyVal, uss.Score, uss.Score));
                            tasks251.Add(batch251.SortedSetAddAsync(uss.KeyVal, uss.Value, uss.Score));
                            tasks251.Add(batch251.SortedSetRemoveRangeByScoreAsync(uss.KeyVol, uss.Score, uss.Score));
                            tasks251.Add(batch251.SortedSetAddAsync(uss.KeyVol, uss.Value, uss.Score));
                            break;
                        case SetStringCommand ssc:
                            string currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                            var wrappedData = new
                            {
                                Time = currentTime,
                                Data = JsonConvert.DeserializeObject<object>(ssc.Value)
                            };
                            string finalValue = JsonConvert.SerializeObject(wrappedData);
                            string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + ssc.Key;

                            tasks250.Add(batch250.StringSetAsync(ssc.Key, finalValue, TimeSpan.FromMinutes(ssc.Period)));
                            tasks250.Add(batch250.StringSetAsync(backupKey, finalValue, TimeSpan.FromMinutes(ssc.Period)));

                            tasks251.Add(batch251.StringSetAsync(ssc.Key, finalValue, TimeSpan.FromMinutes(ssc.Period)));
                            tasks251.Add(batch251.StringSetAsync(backupKey, finalValue, TimeSpan.FromMinutes(ssc.Period)));
                            break;
                        default:
                            _logger.LogWarning($"Loại command không xác định trong ExecuteBatchAsync: {cmd?.GetType().Name}");
                            break;
                    }
                }

                if (!tasks250.Any())
                {
                    return true;
                }

                // 1. THỰC THI BATCH (GỬI LỆNH ĐẾN REDIS) 
                batch250.Execute();
                batch251.Execute();

                // 2. await các Task để chờ kết quả
                var allTasks250 = Task.WhenAll(tasks250);
                var allTasks251 = Task.WhenAll(tasks251);

                // 3. Await cho cả hai batch hoàn thành
                await Task.WhenAll(allTasks250, allTasks251);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi nghiêm trọng khi thực thi ExecuteBatchAsync");
                // SỬA LỖI NUỐT EXCEPTION (xem mục 2)
                throw;
            }
        }
    }
}
