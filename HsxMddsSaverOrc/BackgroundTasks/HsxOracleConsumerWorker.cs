using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Application.Shared;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HsxMddsSaverOrc.BackgroundTasks
{
    public class HsxOracleConsumerWorker : BaseMessageConsumerWorker<HsxOracleConsumerWorker>
    {
        public HsxOracleConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<HsxOracleConsumerWorker> logger,
            AppSetting appsetting,
            IMessageParserFactory parserFactory,
            IServiceProvider serviceProvider,
            IHsxOracleDataSaver dataSaver,
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
    }
}
