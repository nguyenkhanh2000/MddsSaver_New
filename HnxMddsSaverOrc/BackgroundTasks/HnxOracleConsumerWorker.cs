using MddsSaver.Application.Shared;
using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HnxMddsSaverOrc.BackgroundTasks
{
    public class HnxOracleConsumerWorker : BaseMessageConsumerWorker<HnxOracleConsumerWorker>
    {
        public HnxOracleConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<HnxOracleConsumerWorker> logger,
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
        protected override string GetSourceIdentifier() => "Oracle_Hnx";
    }
}
