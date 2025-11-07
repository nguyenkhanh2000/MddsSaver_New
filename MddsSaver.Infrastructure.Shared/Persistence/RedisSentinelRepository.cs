using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;

namespace MddsSaver.Infrastructure.Shared.Persistence
{
    public class RedisSentinelRepository : IRedisSentinelRepository
    {
        private readonly ILogger<RedisSentinelRepository> _logger;
        private readonly IDatabase _redisSentinel;
        public RedisSentinelRepository(ILogger<RedisSentinelRepository> logger, IConnectionMultiplexer redisSentinel, int databaseNumber)
        {
            _logger = logger;
            _redisSentinel = redisSentinel.GetDatabase(databaseNumber);
        }
        //public async Task<bool> SaveAsync(string key, string data, TimeSpan? expiry = null)
        //{
        //    string currentTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        //    var wrappedData = new
        //    {
        //        Time = currentTime,
        //        Data = JsonConvert.DeserializeObject<object>(data)
        //    };
        //    string finalValue = JsonConvert.SerializeObject(wrappedData);

        //    string backupKey = "BACKUP:" + DateTime.Today.ToString("yyyy:MM:dd:") + key;
        //    try
        //    {
        //        var taskP1 = _redisFox_250.StringSetAsync(key, finalValue, expiry);
        //        var taskP2 = _redisFox_250.StringSetAsync(backupKey, finalValue, expiry);
        //        var taskS1 = _redisFox_251.StringSetAsync(key, finalValue, expiry);
        //        var taskS2 = _redisFox_251.StringSetAsync(backupKey, finalValue, expiry);

        //        await Task.WhenAll(taskP1, taskP2, taskS1, taskS2);

        //        return taskP1.Result && taskP2.Result && taskS1.Result && taskS2.Result;
        //    }
        //    catch (Exception ex)
        //    {
        //        _logger.LogError(ex, "Lỗi khi dual-write vào Redis (Key: {RedisKey})", key);
        //        return false;
        //    }
        //}
        public async Task<bool> ExecuteBatchAsync(IEnumerable<RedisCommand> commands)
        {
            if (commands == null || !commands.Any())
                return true;

            var batchSentinel = _redisSentinel.CreateBatch();

            var tasksSentinel = new List<Task>();

            try
            {
                foreach (var cmd in commands)
                {
                    switch (cmd)
                    {
                        case SortedSetAddCommand ssa:
                            tasksSentinel.Add(batchSentinel.SortedSetAddAsync(ssa.Key, ssa.Member, ssa.Score));
                            break;
                        case UpdateSortedSetByScoreCommand uss:
                            tasksSentinel.Add(batchSentinel.SortedSetRemoveRangeByScoreAsync(uss.KeyVal, uss.Score, uss.Score));
                            tasksSentinel.Add(batchSentinel.SortedSetAddAsync(uss.KeyVal, uss.Value, uss.Score));
                            tasksSentinel.Add(batchSentinel.SortedSetRemoveRangeByScoreAsync(uss.KeyVol, uss.Score, uss.Score));
                            tasksSentinel.Add(batchSentinel.SortedSetAddAsync(uss.KeyVol, uss.Value, uss.Score));
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

                            tasksSentinel.Add(batchSentinel.StringSetAsync(ssc.Key, finalValue, TimeSpan.FromMinutes(ssc.Period)));
                            tasksSentinel.Add(batchSentinel.StringSetAsync(backupKey, finalValue, TimeSpan.FromMinutes(ssc.Period)));
                            break;
                        default:
                            _logger.LogWarning($"Loại command không xác định trong ExecuteBatchAsync: {cmd?.GetType().Name}");
                            break;
                    }
                }

                if (!tasksSentinel.Any())
                {
                    return true;
                }

                // 1. THỰC THI BATCH (GỬI LỆNH ĐẾN REDIS) 
                batchSentinel.Execute();

                // 2. await các Task để chờ kết quả
                var allTasks = Task.WhenAll(tasksSentinel);

                // 3. Await cho cả hai batch hoàn thành
                await Task.WhenAll(allTasks);

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
