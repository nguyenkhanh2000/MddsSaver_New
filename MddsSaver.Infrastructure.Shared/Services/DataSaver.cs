using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Services
{
    public class DataSaver : IDataSaver
    {
        private readonly ILogger<DataSaver> _logger;
        private Dictionary<string, string> d_dic_stockno = new Dictionary<string, string>();//dic lưu stock no của mess d
        public DataSaver(ILogger<DataSaver> logger)
        {
            _logger = logger;
        }
        public async Task SaveBatchAsync(List<object> messages, CancellationToken stoppingToken)
        {
            try
            {
                // 1. Tạo transaction cho mỗi instance Redis
                //var transactionRD = _redis.GetDatabase().CreateTransaction();

                //foreach (var msg in messages) 
                //{
                //    var typeMsg = msg.GetType();
                //    if (typeMsg == typeof(ESecurityDefinition)) // msg_d
                //    {
                //        var data = (ESecurityDefinition)msg;
                //        // Lấy ra key msg lưu db
                //        d_dic_stockno[data.Symbol] = data.TickerCode;
                //        string stockno = JsonConvert.SerializeObject(d_dic_stockno);
                        
                //    }
                //    if (typeMsg == typeof(EPrice))
                //    {

                //    }
                //}
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SaveBatchAsync: Lỗi khi thực hiện bulk insert!");
                // Ném lại exception để hàm gọi có thể xử lý (NACK messages)
                throw;
            }
        }
    }
}
