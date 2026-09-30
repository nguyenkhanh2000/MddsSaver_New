using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace MddsSaver.Application.Shared
{
    public abstract class BaseMessageConsumerWorker<T> : BackgroundService
    {
        private readonly ILogger<T> _logger;
        private readonly RabbitMQSetting _queueConfig;
        private readonly AppSetting _appSetting;
        private readonly IMessageParserFactory _parserFactory;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly Channel<object> _channelWriter;
        private readonly Channel<ConsumeWorkItem> _messageChannel;
        private readonly IDataSaver _dataSaver;
        private readonly IMessageTypeFilter _msgFilter;
        private readonly IMonitor _monitor;

        private IConnection _connection;
        private IModel _channel;

        private readonly int _batchSize;
        private readonly TimeSpan _timeWindow;

        protected abstract string GetSourceIdentifier();

        public BaseMessageConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<T> logger,
            AppSetting appsetting,
            IMessageParserFactory parserFactory,
            Channel<object> channelWriter,
            IDataSaver dataSaver,
            IMessageTypeFilter msgFilter,
            IMonitor monitor)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _parserFactory = parserFactory;
            _channelWriter = channelWriter;
            _dataSaver = dataSaver;
            _msgFilter = msgFilter;
            _monitor = monitor;
            _appSetting = appsetting;
            _queueConfig = appsetting.RabbitMQ;

            _batchSize = appsetting.RabbitMQ.BatchSize;
            _timeWindow = TimeSpan.FromMilliseconds(appsetting.RabbitMQ.TimeDelay);

            // dùng bounded channel để tạo backpressure
            _messageChannel = Channel.CreateBounded<ConsumeWorkItem>(new BoundedChannelOptions(_queueConfig.PrefetchCount * 2)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = _queueConfig.HostName,
                    Port = _queueConfig.Port,
                    UserName = _queueConfig.Username,
                    Password = _queueConfig.Password,
                    DispatchConsumersAsync = true,
                    AutomaticRecoveryEnabled = true,
                    TopologyRecoveryEnabled = true
                };

                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();

                _channel.BasicQos(0, _queueConfig.PrefetchCount, false);

                _channel.QueueBind(
                    queue: _queueConfig.QueueName,
                    exchange: _queueConfig.ExchangeName,
                    routingKey: _queueConfig.RoutingKey);

                _connection.ConnectionShutdown += (_, ea) =>
                    _logger.LogWarning("RabbitMQ connection shutdown: {ReplyText}", ea.ReplyText);

                _channel.ModelShutdown += (_, ea) =>
                    _logger.LogWarning("RabbitMQ channel shutdown: {ReplyText}", ea.ReplyText);

                _channel.CallbackException += (_, ea) =>
                    _logger.LogError(ea.Exception, "RabbitMQ callback exception");

                _logger.LogInformation("ConsumerWorker đã kết nối với RabbitMQ và đang chờ tin nhắn.");
                return base.StartAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi kết nối tới RabbitMQ");
                throw;
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                stoppingToken.ThrowIfCancellationRequested();

                var consumer = new AsyncEventingBasicConsumer(_channel);

                consumer.Received += async (_, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var messageString = Encoding.UTF8.GetString(body);

                        var msgType = _parserFactory.GetMsgType(messageString);

                        // filter fail -> enqueue ack để luồng xử lý trung tâm quyết định
                        if (!_msgFilter.Accept(msgType))
                        {
                            await _messageChannel.Writer.WriteAsync(
                                ConsumeWorkItem.ForAck(ea.DeliveryTag),
                                stoppingToken);
                            return;
                        }

                        var parsedMessage = await _parserFactory.Parse(messageString, msgType);

                        if (parsedMessage != null)
                        {
                            await _messageChannel.Writer.WriteAsync(
                                ConsumeWorkItem.ForProcessing(ea.DeliveryTag, parsedMessage),
                                stoppingToken);
                        }
                        else
                        {
                            // parse fail -> nack không requeue
                            await _messageChannel.Writer.WriteAsync(
                                ConsumeWorkItem.ForNack(ea.DeliveryTag, requeue: false),
                                stoppingToken);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // service đang dừng
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Lỗi khi xử lý tin nhắn từ RabbitMQ.");

                        try
                        {
                            await _messageChannel.Writer.WriteAsync(
                                ConsumeWorkItem.ForNack(ea.DeliveryTag, requeue: false),
                                stoppingToken);
                        }
                        catch (Exception enqueueEx)
                        {
                            _logger.LogError(enqueueEx, "Không thể enqueue NACK work item.");
                        }
                    }
                };

                _channel.BasicConsume(_queueConfig.QueueName, autoAck: false, consumer: consumer);

                await ProcessMessagesFromChannel(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Consumer ExecuteAsync đã dừng.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "consumer ExecuteAsync");
                throw;
            }
        }

        private async Task ProcessMessagesFromChannel(CancellationToken stoppingToken)
        {
            var batch = new List<ConsumeWorkItem>(_batchSize);
            var reader = _messageChannel.Reader;

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeWorkItem firstItem;

                    try
                    {
                        firstItem = await reader.ReadAsync(stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (firstItem.Kind == WorkItemKind.Process)
                    {
                        batch.Add(firstItem);
                    }
                    else
                    {
                        await HandleSingleAckNackAsync(firstItem);
                    }

                    using var timeoutCts = new CancellationTokenSource(_timeWindow);
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutCts.Token);

                    try
                    {
                        while (batch.Count < _batchSize)
                        {
                            var nextItem = await reader.ReadAsync(linkedCts.Token);

                            if (nextItem.Kind == WorkItemKind.Process)
                            {
                                batch.Add(nextItem);
                            }
                            else
                            {
                                await HandleSingleAckNackAsync(nextItem);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // hết time window hoặc dừng service -> flush batch hiện tại
                    }

                    if (batch.Count > 0)
                    {
                        await ProcessAndSaveBatchAsync(batch, stoppingToken);
                        batch.Clear();
                    }
                }

                // drain những message còn lại khi shutdown
                while (reader.TryRead(out var remainingItem))
                {
                    if (remainingItem.Kind == WorkItemKind.Process)
                    {
                        batch.Add(remainingItem);

                        if (batch.Count >= _batchSize)
                        {
                            await ProcessAndSaveBatchAsync(batch, CancellationToken.None);
                            batch.Clear();
                        }
                    }
                    else
                    {
                        await HandleSingleAckNackAsync(remainingItem);
                    }
                }

                if (batch.Count > 0)
                {
                    await ProcessAndSaveBatchAsync(batch, CancellationToken.None);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ProcessMessagesFromChannel");
            }
        }

        private async Task ProcessAndSaveBatchAsync(List<ConsumeWorkItem> messagesToProcess, CancellationToken stoppingToken)
        {
            if (messagesToProcess.Count == 0)
            {
                return;
            }

            try
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();

                var parsedMessages = messagesToProcess
                    .Where(x => x.Kind == WorkItemKind.Process && x.ParsedMessage != null)
                    .Select(x => x.ParsedMessage)
                    .ToList();

                if (parsedMessages.Count == 0)
                {
                    return;
                }

                await _dataSaver.SaveBatchAsync(parsedMessages, GetSourceIdentifier(), stoppingToken);

                // ACK từng message để an toàn, tránh multiple ack
                foreach (var item in messagesToProcess)
                {
                    _channel.BasicAck(item.DeliveryTag, multiple: false);
                }

                await _monitor.SendStatusToMonitor(
                    _monitor.GetLocalDateTime(),
                    _monitor.GetLocalIP(),
                    _appSetting.Redis.KeyAppName_Proc,
                    parsedMessages.Count,
                    sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi thực hiện bulk insert. Bắt đầu NACK {Count} tin nhắn.", messagesToProcess.Count);

                foreach (var item in messagesToProcess)
                {
                    try
                    {
                        _channel.BasicNack(item.DeliveryTag, multiple: false, requeue: true);
                    }
                    catch (Exception nackEx)
                    {
                        _logger.LogError(nackEx, "Lỗi khi thực hiện BasicNack cho DeliveryTag={DeliveryTag}", item.DeliveryTag);
                    }
                }
            }
        }

        private Task HandleSingleAckNackAsync(ConsumeWorkItem item)
        {
            try
            {
                switch (item.Kind)
                {
                    case WorkItemKind.Ack:
                        _channel.BasicAck(item.DeliveryTag, multiple: false);
                        break;

                    case WorkItemKind.Nack:
                        _channel.BasicNack(item.DeliveryTag, multiple: false, requeue: item.Requeue);
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi ACK/NACK DeliveryTag={DeliveryTag}", item.DeliveryTag);
            }

            return Task.CompletedTask;
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                _messageChannel.Writer.TryComplete();
            }
            catch
            {
                // ignore
            }

            await base.StopAsync(cancellationToken);
        }

        public override void Dispose()
        {
            try
            {
                _channel?.Close();
            }
            catch
            {
                // ignore
            }

            try
            {
                _connection?.Close();
            }
            catch
            {
                // ignore
            }

            _channel?.Dispose();
            _connection?.Dispose();

            base.Dispose();
        }

        private sealed class ConsumeWorkItem
        {
            public ulong DeliveryTag { get; private set; }
            public object ParsedMessage { get; private set; }
            public WorkItemKind Kind { get; private set; }
            public bool Requeue { get; private set; }

            public static ConsumeWorkItem ForProcessing(ulong deliveryTag, object parsedMessage)
                => new ConsumeWorkItem
                {
                    DeliveryTag = deliveryTag,
                    ParsedMessage = parsedMessage,
                    Kind = WorkItemKind.Process
                };

            public static ConsumeWorkItem ForAck(ulong deliveryTag)
                => new ConsumeWorkItem
                {
                    DeliveryTag = deliveryTag,
                    Kind = WorkItemKind.Ack
                };

            public static ConsumeWorkItem ForNack(ulong deliveryTag, bool requeue)
                => new ConsumeWorkItem
                {
                    DeliveryTag = deliveryTag,
                    Kind = WorkItemKind.Nack,
                    Requeue = requeue
                };
        }

        private enum WorkItemKind
        {
            Process,
            Ack,
            Nack
        }
        //public abstract class BaseMessageConsumerWorker<T> : BackgroundService
        //{
        //    private readonly ILogger<T> _logger;
        //    private readonly RabbitMQSetting _queueConfig;
        //    private readonly AppSetting _appSetting;
        //    private readonly IMessageParserFactory _parserFactory;
        //    private readonly IServiceScopeFactory _scopeFactory;
        //    private readonly Channel<object> _channelWriter;
        //    private readonly Channel<RabbitMqMessageWrapper> _messageChannel;
        //    private readonly IDataSaver _dataSaver;
        //    private readonly IMessageTypeFilter _msgFilter;
        //    private IConnection _connection;
        //    private IModel _channel;
        //    private IMonitor _monitor;
        //    //cau hinh batching
        //    private readonly int _batchSize;
        //    private readonly TimeSpan _timeWindow;
        //    protected abstract string GetSourceIdentifier();
        //    public BaseMessageConsumerWorker(
        //        IServiceScopeFactory scopeFactory,
        //        ILogger<T> logger,
        //        AppSetting appsetting,
        //        IMessageParserFactory parserFactory,
        //        Channel<object> channelWriter,
        //        IDataSaver dataSaver,
        //        IMessageTypeFilter msgFilter,
        //        IMonitor monitor
        //        )
        //    {
        //        _logger = logger;
        //        _scopeFactory = scopeFactory;
        //        _parserFactory = parserFactory;
        //        _channelWriter = channelWriter;
        //        _dataSaver = dataSaver;
        //        _msgFilter = msgFilter;
        //        _monitor = monitor;
        //        _appSetting = appsetting;
        //        _queueConfig = appsetting.RabbitMQ;

        //        _messageChannel = Channel.CreateUnbounded<RabbitMqMessageWrapper>();

        //        _batchSize = appsetting.RabbitMQ.BatchSize;
        //        _timeWindow = TimeSpan.FromMilliseconds(appsetting.RabbitMQ.TimeDelay);
        //    }
        //    public override Task StartAsync(CancellationToken cancellationToken)
        //    {
        //        try
        //        {
        //            // Mỗi worker tự tạo ConnectionFactory riêng
        //            var factory = new ConnectionFactory()
        //            {
        //                HostName = _queueConfig.HostName,
        //                Port = _queueConfig.Port,
        //                UserName = _queueConfig.Username,
        //                Password = _queueConfig.Password
        //            };
        //            // Tạo kết nối và channel
        //            _connection = factory.CreateConnection();
        //            _channel = _connection.CreateModel();
        //            _channel.BasicQos(0, _queueConfig.PrefetchCount, false);
        //            _channel.QueueBind(queue: _queueConfig.QueueName, exchange: _queueConfig.ExchangeName, routingKey: _queueConfig.RoutingKey);

        //            _logger.LogInformation("ConsumerWorker đã kết nối với RabbitMQ và đang chờ tin nhắn.");
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "Lỗi khi kết nối tới RabbitMQ");
        //        }

        //        return base.StartAsync(cancellationToken);
        //    }
        //    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //    {
        //        try
        //        {
        //            stoppingToken.ThrowIfCancellationRequested();
        //            var consumer = new EventingBasicConsumer(_channel);
        //            consumer.Received += async (model, ea) =>
        //            {
        //                try
        //                {
        //                    var body = ea.Body.ToArray();
        //                    var messageString = Encoding.UTF8.GetString(body);
        //                    // Lấy msgType nhanh
        //                    var msgType = _parserFactory.GetMsgType(messageString);
        //                    // Lọc theo service
        //                    if (!_msgFilter.Accept(msgType)) 
        //                    {
        //                        // Ack để bỏ qua
        //                        _channel.BasicAck(ea.DeliveryTag, multiple: false);
        //                        return;
        //                    }
        //                    // parse khi đã pass filter
        //                    var parsedMessage = await _parserFactory.Parse(messageString, msgType);
        //                    if (parsedMessage != null)
        //                    {
        //                        // Chỉ cần ghi vào Channel. Dùng TryWrite vì nó không block.
        //                        var wrapper = new RabbitMqMessageWrapper { DeliveryTag = ea.DeliveryTag, ParsedMessage = parsedMessage };
        //                        _messageChannel.Writer.TryWrite(wrapper);
        //                    }
        //                    else
        //                    {
        //                        // NACK nếu parse thất bại, không requeue
        //                        _channel.BasicNack(ea.DeliveryTag, false, false);
        //                    }
        //                }
        //                catch (Exception ex)
        //                {
        //                    _logger.LogError(ex, "Lỗi khi xử lý tin nhắn từ RabbitMQ.");
        //                    _channel.BasicNack(ea.DeliveryTag, false, false);
        //                }
        //            };
        //            // Bắt đầu lắng nghe tin nhắn
        //            _channel.BasicConsume(_queueConfig.QueueName, false, consumer);
        //            // Bắt đầu một Task riêng để xử lý message từ Channel
        //            // Đây là nơi tập trung toàn bộ logic batching
        //            await ProcessMessagesFromChannel(stoppingToken);
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "consumer ExecuteAsync");
        //        }
        //    }
        //    private async Task ProcessMessagesFromChannel(CancellationToken stoppingToken)
        //    {
        //        try
        //        {
        //            var batch = new List<RabbitMqMessageWrapper>(_batchSize);
        //            var reader = _messageChannel.Reader;

        //            while (!stoppingToken.IsCancellationRequested)
        //            {
        //                // 1. Chờ message đầu tiên của batch đến
        //                // Vòng lặp sẽ ngủ đông ở đây một cách hiệu quả, không polling
        //                try
        //                {
        //                    var firstMessage = await reader.ReadAsync(stoppingToken);
        //                    batch.Add(firstMessage);
        //                }
        //                catch (OperationCanceledException)
        //                {
        //                    break; // Dịch vụ đang dừng
        //                }

        //                // 2. Gom thêm message cho đủ batch hoặc hết thời gian chờ
        //                try
        //                {
        //                    // Tạo một CancellationTokenSource để giới hạn thời gian gom batch
        //                    using var timeoutCts = new CancellationTokenSource(_timeWindow);
        //                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutCts.Token);

        //                    while (batch.Count < _batchSize)
        //                    {
        //                        // Đọc message tiếp theo, nhưng không block, chỉ chờ trong khoảng thời gian còn lại
        //                        var nextMessage = await reader.ReadAsync(linkedCts.Token);
        //                        batch.Add(nextMessage);
        //                    }
        //                }
        //                catch (OperationCanceledException)
        //                {
        //                    // Hết thời gian chờ (_timeWindow) hoặc dịch vụ dừng.
        //                    // Đây là điều mong muốn, không phải lỗi.
        //                    await ProcessAndSaveBatchAsync(batch, stoppingToken);
        //                    batch.Clear();
        //                }

        //                // 3. Xử lý batch đã gom được
        //                if (batch.Count > 0)
        //                {
        //                    await ProcessAndSaveBatchAsync(batch, stoppingToken);
        //                    batch.Clear();
        //                }
        //            }

        //            // Xử lý nốt những message cuối cùng khi dịch vụ dừng
        //            // Đọc tất cả những gì còn lại trong channel
        //            while (reader.TryRead(out var remainingMessage))
        //            {
        //                batch.Add(remainingMessage);
        //            }
        //            if (batch.Count > 0)
        //            {
        //                await ProcessAndSaveBatchAsync(batch, stoppingToken);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, "ProcessMessagesFromChannel");
        //        }
        //    }
        //    private async Task ProcessAndSaveBatchAsync(List<RabbitMqMessageWrapper> messagesToProcess, CancellationToken stoppingToken)
        //    {
        //        if (messagesToProcess.Count == 0)
        //        {
        //            return;
        //        }
        //        try
        //        {
        //            var SW = System.Diagnostics.Stopwatch.StartNew();
        //            // Chuyển danh sách tin nhắn để DataSaver xử lý
        //            var parsedMessages = messagesToProcess.Select(m => m.ParsedMessage).ToList();
        //            await _dataSaver.SaveBatchAsync(parsedMessages, GetSourceIdentifier(), stoppingToken);

        //            // Bulk insert thành công, ACK toàn bộ batch
        //            // Lấy deliveryTag của message cuối cùng trong batch và ACK tất cả message trước đó
        //            _channel.BasicAck(messagesToProcess.Last().DeliveryTag, true);
        //            await _monitor.SendStatusToMonitor(_monitor.GetLocalDateTime(), _monitor.GetLocalIP(), _appSetting.Redis.KeyAppName_Proc, messagesToProcess.Count, SW.ElapsedMilliseconds);
        //        }
        //        catch (Exception ex)
        //        {
        //            _logger.LogError(ex, $"Lỗi khi thực hiện bulk insert. Bắt đầu NACK {messagesToProcess.Count} tin nhắn.");
        //            // Bulk insert thất bại, NACK toàn bộ batch để RabbitMQ re-queue
        //            try
        //            {
        //                foreach (var msgWrapper in messagesToProcess)
        //                {
        //                    // NACK với requeue = true để thử xử lý lại
        //                    _channel.BasicNack(msgWrapper.DeliveryTag, false, true);
        //                }
        //            }
        //            catch (Exception nackEx)
        //            {
        //                _logger.LogError(nackEx, "Lỗi khi thực hiện BasicNack.");
        //            }
        //        }
        //    }
        //}
    }
}
