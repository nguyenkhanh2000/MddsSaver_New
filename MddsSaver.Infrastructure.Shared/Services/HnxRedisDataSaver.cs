using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Services
{
    public class HnxRedisDataSaver : IHnxRedisDataSaver
    {
        private readonly ILogger<HnxRedisDataSaver> _logger;
        private readonly IRedisRepository _redisRepository;
        private readonly IRedisSentinelRepository _redisSentinelRepository;
        private Dictionary<string, string> d_dic_stockno = new Dictionary<string, string>();//dic lưu stock no của mess d
        public HnxRedisDataSaver(ILogger<HnxRedisDataSaver> logger, IRedisRepository redisRepo, IRedisSentinelRepository redisSentinelRep)
        {
            _logger = logger;
            _redisRepository = redisRepo;
            _redisSentinelRepository = redisSentinelRep;
        }
        public async Task SaveBatchAsync(List<object> messages, string sourceIdentifier, CancellationToken stoppingToken)
        {
            try
            {
                
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[{sourceIdentifier}] SaveBatchAsync: Lỗi khi thực hiện bulk insert!");
                // Ném lại exception để hàm gọi có thể xử lý (NACK messages)
                throw;
            }
        }
    }
}
