using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading;
using System.Threading.Tasks;
using MddsSaver.Application.Shared;
using MddsSaver.Core.Shared.Models;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Application.Shared.Interfaces;

namespace HnxMddsSaverRD.BackgroundTasks
{
    public class HnxRedisConsumerWorker : BaseMessageConsumerWorker<HnxRedisConsumerWorker>
    {
        public HnxRedisConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<HnxRedisConsumerWorker> logger,
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
        protected override string GetSourceIdentifier() => "Redis_Hnx";
    }
}
