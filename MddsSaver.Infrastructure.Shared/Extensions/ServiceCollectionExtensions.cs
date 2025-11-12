using Manonero.Logger.Kafka;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Infrastructure.Shared.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Extensions
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddInfrastructureServices(
            this IServiceCollection services,
            AppSetting appSetting)
        {
            // I. PHẦN REDIS
            // 1. Lay setting Redis
            var RedisSettings = appSetting.Redis;
            var redisSentinel = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Sentinel);
            services.AddSingleton<IRedisSentinelRepository>(sp =>
            {
                return new RedisSentinelRepository(
                    sp.GetRequiredService<ILogger<RedisSentinelRepository>>(),
                    redisSentinel,
                    RedisSettings.DatabaseNumber_Sentinel
                );
            });

            // 2. Tạo cả 2 ConnectionMultiplexer (chỉ một lần)
            var redisFox_250 = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Fox250);
            var redisFox_251 = ConnectionMultiplexer.Connect(RedisSettings.Endpoint_Fox251);
            // 3. Đăng ký IDataRedisRepository bằng factory
            services.AddSingleton<IRedisRepository>(sp =>
            {
                return new RedisRepository(
                    sp.GetRequiredService<ILogger<RedisRepository>>(),
                    redisFox_250,   
                    redisFox_251,
                    RedisSettings.DatabaseNumber_Fox
                );
            });
            // II. PHẦN ORACLE
            var oracleConnectionString = appSetting.ConnectionStrings;
            services.AddTransient<IDbConnection>(sp => new OracleConnection(oracleConnectionString.Oracle));

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
