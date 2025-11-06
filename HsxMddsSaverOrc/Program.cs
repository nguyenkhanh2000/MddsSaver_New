using HsxMddsSaverOrc;
using HsxMddsSaverOrc.BackgroundTasks;
using MddsSaver.Application.Shared.Common;
using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Infrastructure.Shared.Extensions;
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
        services.AddSingleton(appSettings.Redis);
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var settings = sp.GetRequiredService<RedisSettings>();
            return ConnectionMultiplexer.Connect(settings.Endpoints);
        });
        // Đăng ký Kafka Logger
        services.AddKafkaLogger(configuration, true);
        services.AddSingleton<IMessageTypeFilter, OracleMessageTypeFilter>();
        // TẠO HAI CHANNEL RIÊNG BIỆT CHO MỖI SÀN
        services.AddSingleton(provider => Channel.CreateUnbounded<object>(new UnboundedChannelOptions { SingleReader = true }));
        services.AddSingleton<IMessageParserFactory, MessageParserFactory>();
        services.AddHostedService<HsxMessageConsumerWorker>();
    })
    .Build();
host.EnsureNetworkConnectivity();
host.Run();