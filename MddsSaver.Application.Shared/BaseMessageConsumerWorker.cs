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
        private readonly Channel<RabbitMqMessageWrapper> _messageChannel;
        private readonly IDataSaver _dataSaver;
        private readonly IMessageTypeFilter _msgFilter;
        private IConnection _connection;
        private IModel _channel;
        private IMonitor _monitor;
        //cau hinh batching
        private readonly int _batchSize;
        private readonly TimeSpan _timeWindow;
        public BaseMessageConsumerWorker(
            IServiceScopeFactory scopeFactory,
            ILogger<T> logger,
            AppSetting appsetting,
            IMessageParserFactory parserFactory,
            Channel<object> channelWriter,
            IDataSaver dataSaver,
            IMessageTypeFilter msgFilter,
            IMonitor monitor
            )
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

            _messageChannel = Channel.CreateUnbounded<RabbitMqMessageWrapper>();

            _batchSize = appsetting.RabbitMQ.BatchSize;
            _timeWindow = TimeSpan.FromMilliseconds(appsetting.RabbitMQ.TimeDelay);
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            try
            {
                // Mỗi worker tự tạo ConnectionFactory riêng
                var factory = new ConnectionFactory()
                {
                    HostName = _queueConfig.HostName,
                    Port = _queueConfig.Port,
                    UserName = _queueConfig.Username,
                    Password = _queueConfig.Password
                };
                // Tạo kết nối và channel
                _connection = factory.CreateConnection();
                _channel = _connection.CreateModel();
                _channel.BasicQos(0, _queueConfig.PrefetchCount, false);
                _channel.QueueBind(queue: _queueConfig.QueueName, exchange: _queueConfig.ExchangeName, routingKey: _queueConfig.RoutingKey);

                _logger.LogInformation("ConsumerWorker đã kết nối với RabbitMQ và đang chờ tin nhắn.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Lỗi khi kết nối tới RabbitMQ");
            }

            return base.StartAsync(cancellationToken);
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                stoppingToken.ThrowIfCancellationRequested();
                var consumer = new EventingBasicConsumer(_channel);
                consumer.Received += async (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var messageString = Encoding.UTF8.GetString(body);
                        // Lấy msgType nhanh
                        var msgType = _parserFactory.GetMsgType(messageString);
                        // Lọc theo service
                        if (!_msgFilter.Accept(msgType)) 
                        {
                            // Ack để bỏ qua
                            _channel.BasicAck(ea.DeliveryTag, multiple: false);
                            return;
                        }
                        // parse khi đã pass filter
                        var parsedMessage = await _parserFactory.Parse(messageString, msgType);
                        if (parsedMessage != null)
                        {
                            // Chỉ cần ghi vào Channel. Dùng TryWrite vì nó không block.
                            var wrapper = new RabbitMqMessageWrapper { DeliveryTag = ea.DeliveryTag, ParsedMessage = parsedMessage };
                            _messageChannel.Writer.TryWrite(wrapper);
                        }
                        else
                        {
                            // NACK nếu parse thất bại, không requeue
                            _channel.BasicNack(ea.DeliveryTag, false, false);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Lỗi khi xử lý tin nhắn từ RabbitMQ.");
                        _channel.BasicNack(ea.DeliveryTag, false, false);
                    }
                };
                // Bắt đầu lắng nghe tin nhắn
                _channel.BasicConsume(_queueConfig.QueueName, false, consumer);
                // Bắt đầu một Task riêng để xử lý message từ Channel
                // Đây là nơi tập trung toàn bộ logic batching
                await ProcessMessagesFromChannel(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "consumer ExecuteAsync");
            }
        }
        private async Task ProcessMessagesFromChannel(CancellationToken stoppingToken)
        {
            try
            {
                var batch = new List<RabbitMqMessageWrapper>(_batchSize);
                var reader = _messageChannel.Reader;

                while (!stoppingToken.IsCancellationRequested)
                {
                    // 1. Chờ message đầu tiên của batch đến
                    // Vòng lặp sẽ ngủ đông ở đây một cách hiệu quả, không polling
                    try
                    {
                        var firstMessage = await reader.ReadAsync(stoppingToken);
                        batch.Add(firstMessage);
                    }
                    catch (OperationCanceledException)
                    {
                        break; // Dịch vụ đang dừng
                    }

                    // 2. Gom thêm message cho đủ batch hoặc hết thời gian chờ
                    try
                    {
                        // Tạo một CancellationTokenSource để giới hạn thời gian gom batch
                        using var timeoutCts = new CancellationTokenSource(_timeWindow);
                        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutCts.Token);

                        while (batch.Count < _batchSize)
                        {
                            // Đọc message tiếp theo, nhưng không block, chỉ chờ trong khoảng thời gian còn lại
                            var nextMessage = await reader.ReadAsync(linkedCts.Token);
                            batch.Add(nextMessage);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Hết thời gian chờ (_timeWindow) hoặc dịch vụ dừng.
                        // Đây là điều mong muốn, không phải lỗi.
                        await ProcessAndSaveBatchAsync(batch, stoppingToken);
                        batch.Clear();
                    }

                    // 3. Xử lý batch đã gom được
                    if (batch.Count > 0)
                    {
                        await ProcessAndSaveBatchAsync(batch, stoppingToken);
                        batch.Clear();
                    }
                }

                // Xử lý nốt những message cuối cùng khi dịch vụ dừng
                // Đọc tất cả những gì còn lại trong channel
                while (reader.TryRead(out var remainingMessage))
                {
                    batch.Add(remainingMessage);
                }
                if (batch.Count > 0)
                {
                    await ProcessAndSaveBatchAsync(batch, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ProcessMessagesFromChannel");
            }
        }
        private async Task ProcessAndSaveBatchAsync(List<RabbitMqMessageWrapper> messagesToProcess, CancellationToken stoppingToken)
        {
            if (messagesToProcess.Count == 0)
            {
                return;
            }
            try
            {
                var SW = System.Diagnostics.Stopwatch.StartNew();
                // Chuyển danh sách tin nhắn để DataSaver xử lý
                var parsedMessages = messagesToProcess.Select(m => m.ParsedMessage).ToList();
                await _dataSaver.SaveBatchAsync(parsedMessages, stoppingToken);

                // Bulk insert thành công, ACK toàn bộ batch
                // Lấy deliveryTag của message cuối cùng trong batch và ACK tất cả message trước đó
                _channel.BasicAck(messagesToProcess.Last().DeliveryTag, true);
                await _monitor.SendStatusToMonitor(_monitor.GetLocalDateTime(), _monitor.GetLocalIP(), _appSetting.Redis.KeyAppName_Proc, messagesToProcess.Count, SW.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Lỗi khi thực hiện bulk insert. Bắt đầu NACK {messagesToProcess.Count} tin nhắn.");
                // Bulk insert thất bại, NACK toàn bộ batch để RabbitMQ re-queue
                try
                {
                    foreach (var msgWrapper in messagesToProcess)
                    {
                        // NACK với requeue = true để thử xử lý lại
                        _channel.BasicNack(msgWrapper.DeliveryTag, false, true);
                    }
                }
                catch (Exception nackEx)
                {
                    _logger.LogError(nackEx, "Lỗi khi thực hiện BasicNack.");
                }
            }
        }
    }
}
