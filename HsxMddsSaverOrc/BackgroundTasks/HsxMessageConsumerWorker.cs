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
    public class HsxMessageConsumerWorker : BaseMessageConsumerWorker
    {
        public HsxMessageConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<HsxMessageConsumerWorker> logger,
            AppSetting appsetting,
            IMessageParserFactory parserFactory,
            IServiceProvider serviceProvider,
            IConnectionMultiplexer redis,
            IDataSaver dataSaver,
            IMessageTypeFilter msgFilter
            ) : base(
                scopeFactory,
                logger,
                appsetting,
                parserFactory,
                serviceProvider.GetServices<Channel<object>>().First(),
                redis,
                dataSaver,
                msgFilter
                )
        {

        }
    }
}
