using HsxMddsSaverRD;
using HsxMddsSaverRD.BackgroundTasks;
using MddsSaver.Application.Shared.Common;
using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Infrastructure.Shared.Extensions;
using MddsSaver.Infrastructure.Shared.Services;
using StackExchange.Redis;
using System.Threading.Channels;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        var configuration = hostContext.Configuration;
        // Tạo một đối tượng AppSetting duy nhất và đăng ký nó
        var appSettings = AppSetting.MapValue(hostContext.Configuration);
        services.AddSingleton(appSettings);
        // Đăng ký các phần cấu hình con dưới dạng Singleton để dễ dàng inject
        services.AddSingleton(appSettings.RabbitMQ);
        services.AddInfrastructureServices(appSettings);
        // Đăng ký Kafka Logger
        services.AddKafkaLogger(configuration, true);
        services.AddSingleton<IMessageTypeFilter, RedisMessageTypeFilter>();
        //Đăng ký Monitor (scoped)
        services.AddSingleton<IMonitor, MonitorMdds>();
        // TẠO HAI CHANNEL RIÊNG BIỆT CHO MỖI SÀN
        services.AddSingleton(provider => Channel.CreateUnbounded<object>(new UnboundedChannelOptions { SingleReader = true }));
        services.AddSingleton<IMessageParserFactory, MessageParserFactory>();
        services.AddSingleton<IHsxRedisDataSaver, HsxRedisDataSaver>();
        services.AddHostedService<HsxRedisConsumerWorker>();
    })
    .Build();
host.EnsureNetworkConnectivity();
host.Run();