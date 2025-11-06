using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class AppSetting
    {
        public ServiceSetting  ServiceSetting { get; set; }
        public RabbitMQSetting RabbitMQ       { get; set; }
        public RedisSettings   Redis          { get; set; }
        public static AppSetting MapValue(IConfiguration configuration)
        {
            var serviceSetting  = ServiceSetting.MapValue(configuration.GetSection(nameof(ServiceSetting)));
            var rabbitmqSetting = RabbitMQSetting.MapValue(configuration.GetSection(nameof(RabbitMQ)));
            var redisSetting    = RedisSettings.MapValue(configuration.GetSection(nameof(Redis)));
            return new AppSetting
            {
                ServiceSetting  = serviceSetting,
                RabbitMQ        = rabbitmqSetting,
                Redis           = redisSetting
            };
        }
        //public static AppSetting MapValue(IConfiguration configuration)
        //{
        //    return new AppSetting
        //    {
        //        // Dùng .Get<T>() để tự động map toàn bộ section
        //        ServiceSetting = configuration.GetSection(nameof(ServiceSetting)).Get<ServiceSetting>(),
        //        RabbitMQSettings = configuration.GetSection(nameof(RabbitMQSettings)).Get<RabbitMQSetting>(), 
        //        Redis = configuration.GetSection(nameof(Redis)).Get<RedisSettings>(),

        //    };
        //}
    }
}
