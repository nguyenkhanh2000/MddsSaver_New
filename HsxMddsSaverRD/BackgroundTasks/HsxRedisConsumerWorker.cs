using MddsSaver.Application.Shared;
using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HsxMddsSaverRD.BackgroundTasks
{
    public class HsxRedisConsumerWorker : BaseMessageConsumerWorker<HsxRedisConsumerWorker>
    {
        public HsxRedisConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<HsxRedisConsumerWorker> logger,
            AppSetting appsetting,
            IMessageParserFactory parserFactory,
            IServiceProvider serviceProvider,
            IDataSaver dataSaver,
            IMessageTypeFilter msgFilter,
            IMonitor monitor
            ) : base(
                scopeFactory,
                logger,
                appsetting,
                parserFactory,
                serviceProvider.GetServices<Channel<object>>().First(),
                dataSaver,
                msgFilter,
                monitor
                )
        {

        }
        protected override string GetSourceIdentifier() => "Redis_Hsx";
    }
}
