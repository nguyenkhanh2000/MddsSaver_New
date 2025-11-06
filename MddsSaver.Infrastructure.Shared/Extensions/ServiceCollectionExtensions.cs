using Manonero.Logger.Kafka;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Infrastructure.Shared.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddInfrastructureServices(
            this IServiceCollection services,
            IConfiguration configuration)
        {
            
            //var monitorRedisSettings = configuration.GetSection("MonitoringRedis").Get<RedisSettings>();
            //var monitorMultiplexer = ConnectionMultiplexer.Connect(monitorRedisSettings.PrimaryEndpoint);
            //services.AddScoped<IMonitoringRedisRepository>(sp =>
            //    new MonitoringRedisRepository(
            //        sp.GetRequiredService<ILogger<MonitoringRedisRepository>>(),
            //        monitorMultiplexer
            //    ));
            //services.AddScoped<IMonitor, Monitor>();
            // 1. Lay setting Redis Fox
            var RedisSettings = configuration.GetSection("Redis").Get<RedisSettings>();

            var redisSentinel = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Sentinel);
            services.AddSingleton<IRedisSentinelRepository>(sp =>
            {
                // Khi ai đó hỏi IDataRedisRepository:
                // Tạo một DataRedisRepository MỚI và inject CẢ HAI multiplexer vào
                return new RedisSentinelRepository(
                    sp.GetRequiredService<ILogger<RedisSentinelRepository>>(),
                    redisSentinel
                );
            });

            // 2. Tạo cả 2 ConnectionMultiplexer (chỉ một lần)
            var redisFox_250 = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Fox250);
            var redisFox_251 = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Fox251);
            // 3. Đăng ký IDataRedisRepository bằng factory
            services.AddSingleton<IRedisRepository>(sp =>
            {
                // Khi ai đó hỏi IDataRedisRepository:
                // Tạo một DataRedisRepository MỚI và inject CẢ HAI multiplexer vào
                return new RedisRepository(
                    sp.GetRequiredService<ILogger<RedisRepository>>(),
                    redisFox_250,   
                    redisFox_251  
                );
            });
            return services;

        }
        public static IServiceCollection AddKafkaLogger(this IServiceCollection services, IConfiguration configuration, bool isAllowedConsoleLog = false)
        {
            var section = configuration.GetSection("KafkaLogger");
            services.AddLogging(config =>
            {
                if (!isAllowedConsoleLog)
                {
                    config.ClearProviders();
                }

                if (section != null) config.AddKafka(section);
            });

            return services;
        }
    }
}
