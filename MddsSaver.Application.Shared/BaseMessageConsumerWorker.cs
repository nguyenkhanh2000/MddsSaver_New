using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public abstract class BaseMessageConsumerWorker<T> : BackgroundService
{
    private readonly ILogger<T> _logger;
    private readonly RabbitMQSetting _queueConfig;
    private readonly AppSetting _appSetting;
    private readonly IMessageParserFactory _parserFactory;
    private readonly IDataSaver _dataSaver;
    private readonly IMessageTypeFilter _msgFilter;
    private readonly IMonitor _monitor;

    /*
     * Tạm giữ 2 dependency này trong constructor để các class kế thừa
     * hiện tại của bạn không cần sửa ngay.
     *
     * Thực tế BaseMessageConsumerWorker hiện không cần:
     * - IServiceScopeFactory
     * - Channel<object>
     *
     * Sau khi test ổn có thể xóa khỏi constructor + DI.
     */
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Channel<object> _legacyChannelWriter;

    private readonly Channel<ConsumeWorkItem> _messageChannel;

    private IConnection? _connection;
    private IModel? _channel;

    private readonly int _batchSize;
    private readonly TimeSpan _timeWindow;

    // Giới hạn thời gian flush khi pod nhận SIGTERM.
    // Không sử dụng CancellationToken.None vô hạn như code cũ.
    private static readonly TimeSpan ShutdownFlushTimeout =
        TimeSpan.FromSeconds(30);

    private string? _consumerTag;

    /*
     * 0 = running
     * 1 = stopping
     */
    private int _isStopping;

    protected abstract string GetSourceIdentifier();

    protected BaseMessageConsumerWorker(
        IServiceScopeFactory scopeFactory,
        ILogger<T> logger,
        AppSetting appsetting,
        IMessageParserFactory parserFactory,
        Channel<object> channelWriter,
        IDataSaver dataSaver,
        IMessageTypeFilter msgFilter,
        IMonitor monitor)
    {
        _scopeFactory = scopeFactory;
        _legacyChannelWriter = channelWriter;

        _logger = logger;
        _parserFactory = parserFactory;
        _dataSaver = dataSaver;
        _msgFilter = msgFilter;
        _monitor = monitor;

        _appSetting = appsetting
            ?? throw new ArgumentNullException(nameof(appsetting));

        _queueConfig = appsetting.RabbitMQ
            ?? throw new InvalidOperationException(
                "RabbitMQ configuration is missing.");

        if (_queueConfig.BatchSize <= 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ BatchSize must be greater than 0.");
        }

        if (_queueConfig.PrefetchCount <= 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ PrefetchCount must be greater than 0.");
        }

        if (_queueConfig.TimeDelay <= 0)
        {
            throw new InvalidOperationException(
                "RabbitMQ TimeDelay must be greater than 0.");
        }

        _batchSize = _queueConfig.BatchSize;
        _timeWindow =
            TimeSpan.FromMilliseconds(_queueConfig.TimeDelay);

        /*
         * Không để queue memory tăng vô hạn.
         *
         * Prefetch = 5000
         * capacity = 10000
         *
         * Nếu xử lý phía sau chậm, WriteAsync() sẽ backpressure
         * thay vì tiếp tục đẩy object vào RAM.
         */
        var capacity = checked(Math.Max(_queueConfig.PrefetchCount, _queueConfig.PrefetchCount * 2));

        _messageChannel =
            Channel.CreateBounded<ConsumeWorkItem>(
                new BoundedChannelOptions(capacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,

                    /*
                     * Chỉ ProcessMessagesFromChannel đọc.
                     */
                    SingleReader = true,

                    /*
                     * Không giả định RabbitMQ callback luôn chỉ chạy
                     * duy nhất 1 thread.
                     *
                     * False an toàn hơn nếu sau này bật
                     * ConsumerDispatchConcurrency > 1.
                     */
                    SingleWriter = false,

                    /*
                     * Tránh callback continuation chạy inline
                     * ngoài dự kiến.
                     */
                    AllowSynchronousContinuations = false
                });
    }

    // ============================================================
    // START
    // ============================================================

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation(
                "Starting RabbitMQ consumer. " +
                "Queue={Queue}, BatchSize={BatchSize}, " +
                "Prefetch={Prefetch}, TimeWindow={TimeWindow}ms",
                _queueConfig.QueueName,
                _batchSize,
                _queueConfig.PrefetchCount,
                _timeWindow.TotalMilliseconds);

            var factory = new ConnectionFactory
            {
                HostName = _queueConfig.HostName,
                Port = _queueConfig.Port,
                UserName = _queueConfig.Username,
                Password = _queueConfig.Password,

                DispatchConsumersAsync = true,

                /*
                 * RabbitMQ.Client tự recover connection/channel
                 * nếu network bị mất tạm thời.
                 */
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true
            };

            _connection = factory.CreateConnection();

            _connection.ConnectionShutdown += OnConnectionShutdown;

            _connection.CallbackException += OnConnectionCallbackException;

            _channel = _connection.CreateModel();

            _channel.ModelShutdown += OnModelShutdown;

            _channel.CallbackException += OnChannelCallbackException;

            /*
             * Không cho RabbitMQ gửi vô hạn message chưa ACK.
             */
            _channel.BasicQos(
                prefetchSize: 0,
                prefetchCount: _queueConfig.PrefetchCount,
                global: false);

            /*
             * Giữ như logic hiện tại của project.
             *
             * Nếu Queue đã được bind bằng IaC/RabbitMQ config
             * thì sau này có thể bỏ QueueBind khỏi application.
             */
            _channel.QueueBind(
                queue: _queueConfig.QueueName,
                exchange: _queueConfig.ExchangeName,
                routingKey: _queueConfig.RoutingKey);

            _logger.LogInformation("Connected RabbitMQ successfully. Queue={Queue}", _queueConfig.QueueName);

            return base.StartAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "Cannot initialize RabbitMQ consumer. Queue={Queue}",
                _queueConfig.QueueName);

            CleanupRabbitMq();

            throw;
        }
    }

    // ============================================================
    // EXECUTE
    // ============================================================

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_channel == null)
        {
            throw new InvalidOperationException(
                "RabbitMQ channel has not been initialized.");
        }

        try
        {
            stoppingToken.ThrowIfCancellationRequested();

            var consumer = new AsyncEventingBasicConsumer(_channel);

            consumer.Received += OnMessageReceivedAsync;

            /*
             * Lưu ConsumerTag để StopAsync gọi BasicCancel().
             *
             * Đây là phần rất quan trọng cho Kubernetes:
             *
             * SIGTERM
             *   ↓
             * BasicCancel
             *   ↓
             * không nhận message mới
             *   ↓
             * flush message đang có
             */
            _consumerTag = _channel.BasicConsume(
                queue: _queueConfig.QueueName,
                autoAck: false,
                consumer: consumer);

            _logger.LogInformation(
                "RabbitMQ consumer started. " +
                "Queue={Queue}, ConsumerTag={ConsumerTag}",
                _queueConfig.QueueName,
                _consumerTag);

            await ProcessMessagesFromChannel(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "RabbitMQ consumer stopping normally. Queue={Queue}",
                _queueConfig.QueueName);
        }
        catch (Exception ex)
        {
            /*
             * Không swallow exception.
             *
             * Nếu worker rơi vào state không an toàn,
             * để BackgroundService/Host restart.
             *
             * Trên K8s pod sẽ được restart nếu process stop.
             */
            _logger.LogCritical(
                ex,
                "RabbitMQ consumer crashed. Queue={Queue}",
                _queueConfig.QueueName);

            throw;
        }
    }

    // ============================================================
    // RABBIT RECEIVED
    // ============================================================

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs ea)
    {
        /*
         * Khi StopAsync đã bắt đầu thì không parse thêm message mới.
         *
         * Không ACK ở đây.
         *
         * Nếu message đã được RabbitMQ deliver nhưng chưa ACK,
         * khi connection đóng RabbitMQ sẽ redeliver.
         */
        if (Volatile.Read(ref _isStopping) == 1)
        {
            return;
        }

        try
        {
            /*
             * ea.Body chỉ valid trong callback.
             *
             * Chuyển thành string ngay để không giữ reference
             * tới RabbitMQ buffer.
             */
            var messageString = Encoding.UTF8.GetString(ea.Body.Span);

            var msgType = _parserFactory.GetMsgType(messageString);

            // ----------------------------------------------------
            // MESSAGE KHÔNG CẦN APP NÀY XỬ LÝ
            // ----------------------------------------------------

            if (!_msgFilter.Accept(msgType))
            {
                await EnqueueWorkItemAsync(ConsumeWorkItem.ForAck(ea.DeliveryTag));

                return;
            }

            // ----------------------------------------------------
            // PARSE
            // ----------------------------------------------------

            object? parsedMessage;

            try
            {
                parsedMessage = await _parserFactory.Parse(messageString,msgType);
            }
            catch (Exception parseEx)
            {
                /*
                 * Parse exception thường là poison message.
                 *
                 * Không requeue vì nếu requeue:
                 *
                 * parse fail
                 *   ↓
                 * requeue
                 *   ↓
                 * parse fail
                 *   ↓
                 * infinite loop
                 *
                 * Queue production nên cấu hình DLX/DLQ.
                 */
                _logger.LogError(
                    parseEx,
                    "Parse RabbitMQ message failed. " +
                    "DeliveryTag={DeliveryTag}, MsgType={MsgType}",
                    ea.DeliveryTag,
                    msgType);

                await EnqueueWorkItemAsync(
                    ConsumeWorkItem.ForNack(
                        ea.DeliveryTag,
                        requeue: false));

                return;
            }

            if (parsedMessage == null)
            {
                _logger.LogWarning(
                    "Parser returned null. " +
                    "DeliveryTag={DeliveryTag}, MsgType={MsgType}",
                    ea.DeliveryTag,
                    msgType);

                await EnqueueWorkItemAsync(
                    ConsumeWorkItem.ForNack(
                        ea.DeliveryTag,
                        requeue: false));

                return;
            }

            // ----------------------------------------------------
            // ĐƯA VÀO BOUNDED CHANNEL
            // ----------------------------------------------------

            await EnqueueWorkItemAsync(ConsumeWorkItem.ForProcessing(ea.DeliveryTag, parsedMessage));
        }
        catch (OperationCanceledException)
        {
            /*
             * Shutdown.
             *
             * Không ACK/NACK tại callback.
             * Message chưa ACK sẽ được RabbitMQ redeliver
             * khi connection đóng.
             */
        }
        catch (ChannelClosedException)
        {
            /*
             * Worker đang shutdown.
             *
             * Không cố ACK/NACK trực tiếp tại callback vì toàn bộ
             * ACK/NACK được serialize ở processing loop.
             */
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unexpected RabbitMQ message error. " +
                "DeliveryTag={DeliveryTag}",
                ea.DeliveryTag);

            /*
             * Với exception không xác định ở tầng callback:
             * NACK false tránh poison message loop.
             *
             * Production nên có DLQ.
             */
            try
            {
                await EnqueueWorkItemAsync(
                    ConsumeWorkItem.ForNack(
                        ea.DeliveryTag,
                        requeue: false));
            }
            catch (Exception enqueueException)
            {
                /*
                 * Nếu shutdown/channel đã đóng:
                 * bỏ ACK/NACK.
                 *
                 * RabbitMQ sẽ redeliver unacked message sau khi
                 * connection đóng.
                 */
                _logger.LogWarning(
                    enqueueException,
                    "Unable to enqueue NACK during shutdown. " +
                    "DeliveryTag={DeliveryTag}",
                    ea.DeliveryTag);
            }
        }
    }

    private async ValueTask EnqueueWorkItemAsync(ConsumeWorkItem item)
    {
        /*
         * Dùng TryWrite trước:
         *
         * - fast path: không tạo async state machine nếu channel còn chỗ
         * - channel đầy: mới await WriteAsync()
         */
        if (_messageChannel.Writer.TryWrite(item))
        {
            return;
        }

        /*
         * Không dùng CancellationToken của callback.
         *
         * Backpressure phải thực sự chờ consumer phía sau.
         *
         * StopAsync sẽ BasicCancel Rabbit consumer trước.
         */
        await _messageChannel.Writer.WriteAsync(item);
    }

    // ============================================================
    // BATCH PROCESSOR
    // ============================================================

    private async Task ProcessMessagesFromChannel(
        CancellationToken stoppingToken)
    {
        var reader = _messageChannel.Reader;

        /*
         * Reuse cùng List để giảm allocation/GC.
         */
        var batch =
            new List<ConsumeWorkItem>(_batchSize);

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
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                await AddOrHandleItemAsync(
                    firstItem,
                    batch);

                /*
                 * Bắt đầu TimeWindow kể từ message đầu tiên
                 * của batch.
                 */
                using var timeoutCts = new CancellationTokenSource(_timeWindow);

                using var linkedCts =
                    CancellationTokenSource
                        .CreateLinkedTokenSource(
                            stoppingToken,
                            timeoutCts.Token);

                try
                {
                    while (batch.Count < _batchSize)
                    {
                        /*
                         * Drain các item đã có sẵn trước.
                         *
                         * Cách này giảm số lần await/context switch
                         * khi RabbitMQ đang đẩy message nhanh.
                         */
                        while (
                            batch.Count < _batchSize &&
                            reader.TryRead(out var readyItem))
                        {
                            await AddOrHandleItemAsync(
                                readyItem,
                                batch);
                        }

                        if (batch.Count >= _batchSize)
                        {
                            break;
                        }

                        var nextItem = await reader.ReadAsync(linkedCts.Token);

                        await AddOrHandleItemAsync(nextItem, batch);
                    }
                }
                catch (OperationCanceledException)
                {
                    /*
                     * Hai trường hợp hợp lệ:
                     *
                     * 1. TimeWindow hết
                     * 2. Application shutdown
                     *
                     * Đều flush batch hiện có.
                     */
                }

                if (batch.Count > 0)
                {
                    await ProcessAndSaveBatchAsync(
                        batch,
                        stoppingToken);

                    batch.Clear();
                }
            }

            /*
             * SIGTERM / StopAsync:
             *
             * Không dùng CancellationToken.None vô hạn.
             *
             * Cho tối đa 30 giây để flush.
             */
            await DrainRemainingMessagesAsync(
                reader,
                batch);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(
                ex,
                "ProcessMessagesFromChannel failed. Queue={Queue}",
                _queueConfig.QueueName);

            throw;
        }
    }

    private async Task AddOrHandleItemAsync(
        ConsumeWorkItem item,
        List<ConsumeWorkItem> batch)
    {
        switch (item.Kind)
        {
            case WorkItemKind.Process:

                batch.Add(item);

                break;

            case WorkItemKind.Ack:

            case WorkItemKind.Nack:

                HandleSingleAckNack(item);

                break;

            default:

                throw new ArgumentOutOfRangeException(
                    nameof(item.Kind),
                    item.Kind,
                    "Unknown work item kind.");
        }

        await Task.CompletedTask;
    }

    // ============================================================
    // SAVE BATCH
    // ============================================================

    private async Task ProcessAndSaveBatchAsync(
        List<ConsumeWorkItem> messagesToProcess,
        CancellationToken cancellationToken)
    {
        if (messagesToProcess.Count == 0)
        {
            return;
        }

        var stopwatch =
            Stopwatch.StartNew();

        var parsedMessages =
            new List<object>(
                messagesToProcess.Count);

        foreach (var item in messagesToProcess)
        {
            if (item.Kind == WorkItemKind.Process &&
                item.ParsedMessage != null)
            {
                parsedMessages.Add(
                    item.ParsedMessage);
            }
        }

        if (parsedMessages.Count == 0)
        {
            return;
        }

        // ========================================================
        // PHASE 1: SAVE
        // ========================================================

        try
        {
            await _dataSaver.SaveBatchAsync(
                parsedMessages,
                GetSourceIdentifier(),
                cancellationToken);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            /*
             * App đang shutdown trong lúc save.
             *
             * Không biết DataSaver đã commit hay chưa.
             *
             * Không ACK.
             *
             * Khi connection RabbitMQ đóng,
             * unacked message sẽ được redeliver.
             *
             * Vì vậy IDataSaver PHẢI hỗ trợ idempotency.
             */
            _logger.LogWarning(
                "Batch save cancelled during shutdown. " +
                "Count={Count}. Messages remain unacked.",
                messagesToProcess.Count);

            throw;
        }
        catch (Exception ex)
        {
            /*
             * SAVE FAIL
             *
             * Chưa ACK message nào.
             *
             * Requeue để retry.
             */
            _logger.LogError(
                ex,
                "Save batch failed. " +
                "NACK + requeue {Count} messages.",
                messagesToProcess.Count);

            NackBatch(
                messagesToProcess,
                requeue: true);

            return;
        }

        // ========================================================
        // PHASE 2: ACK
        // ========================================================

        /*
         * Từ thời điểm này:
         *
         * DATA ĐÃ SAVE THÀNH CÔNG.
         *
         * Nếu ACK bị lỗi thì TUYỆT ĐỐI KHÔNG NACK toàn batch lại.
         *
         * Vì:
         *
         *   DB commit thành công
         *       ↓
         *   ACK #1 OK
         *   ACK #2 OK
         *   ACK #3 connection fail
         *
         * Nếu NACK tất cả:
         *   #1/#2 đã ACK
         *   phần còn lại có thể redeliver
         *
         * trạng thái không còn xác định.
         */
        try
        {
            AckBatch(messagesToProcess);
        }
        catch (Exception ackException)
        {
            _logger.LogCritical(
                ackException,
                "DATA WAS SAVED but RabbitMQ ACK failed. " +
                "Do NOT NACK the batch. " +
                "The consumer will be restarted and RabbitMQ may " +
                "redeliver unacked messages. " +
                "IDataSaver must be idempotent. Count={Count}",
                messagesToProcess.Count);

            /*
             * Fail worker.
             *
             * Đây tốt hơn việc tiếp tục chạy trong trạng thái
             * RabbitMQ channel không còn tin cậy.
             */
            throw;
        }

        stopwatch.Stop();

        // ========================================================
        // PHASE 3: MONITOR
        // ========================================================

        /*
         * Monitor không được phép làm thay đổi trạng thái RabbitMQ.
         *
         * Đây là một bug quan trọng trong code cũ:
         *
         * SAVE OK
         * ACK OK
         * monitor FAIL
         *    ↓
         * catch
         *    ↓
         * BasicNack lại message đã ACK
         */
        try
        {
            await _monitor.SendStatusToMonitor(
                _monitor.GetLocalDateTime(),
                _monitor.GetLocalIP(),
                _appSetting.Redis.KeyAppName_Proc,
                parsedMessages.Count,
                stopwatch.ElapsedMilliseconds);
        }
        catch (Exception monitorException)
        {
            _logger.LogWarning(
                monitorException,
                "Monitor update failed after successful batch. " +
                "RabbitMQ messages are already ACKed. Count={Count}",
                parsedMessages.Count);
        }

        _logger.LogDebug(
            "Processed batch successfully. " +
            "Count={Count}, Elapsed={Elapsed}ms",
            parsedMessages.Count,
            stopwatch.ElapsedMilliseconds);
    }

    // ============================================================
    // ACK / NACK
    // ============================================================

    private void AckBatch(
        IReadOnlyList<ConsumeWorkItem> messages)
    {
        var channel =
            GetOpenChannel();

        foreach (var item in messages)
        {
            /*
             * Cố tình dùng multiple:false.
             *
             * Vì trong cùng delivery sequence có thể có:
             *
             * - process message đang nằm trong batch
             * - filtered message đã ACK trước đó
             *
             * multiple:true có thể ACK nhầm message chưa save.
             */
            channel.BasicAck(
                deliveryTag: item.DeliveryTag,
                multiple: false);
        }
    }

    private void NackBatch(
        IReadOnlyList<ConsumeWorkItem> messages,
        bool requeue)
    {
        var channel =
            GetOpenChannel();

        foreach (var item in messages)
        {
            try
            {
                channel.BasicNack(
                    deliveryTag: item.DeliveryTag,
                    multiple: false,
                    requeue: requeue);
            }
            catch (Exception ex)
            {
                /*
                 * Không cố vòng lặp vô hạn.
                 *
                 * Nếu Rabbit channel chết, các message chưa ACK
                 * sẽ tự redeliver khi connection đóng.
                 */
                _logger.LogError(
                    ex,
                    "BasicNack failed. DeliveryTag={DeliveryTag}",
                    item.DeliveryTag);

                break;
            }
        }
    }

    private void HandleSingleAckNack(
        ConsumeWorkItem item)
    {
        var channel =
            GetOpenChannel();

        switch (item.Kind)
        {
            case WorkItemKind.Ack:

                channel.BasicAck(
                    deliveryTag: item.DeliveryTag,
                    multiple: false);

                break;

            case WorkItemKind.Nack:

                channel.BasicNack(
                    deliveryTag: item.DeliveryTag,
                    multiple: false,
                    requeue: item.Requeue);

                break;

            default:

                throw new InvalidOperationException(
                    $"Work item {item.Kind} cannot be ACK/NACK here.");
        }
    }

    private IModel GetOpenChannel()
    {
        if (_channel == null)
        {
            throw new InvalidOperationException(
                "RabbitMQ channel is null.");
        }

        if (!_channel.IsOpen)
        {
            throw new InvalidOperationException(
                "RabbitMQ channel is closed.");
        }

        return _channel;
    }

    // ============================================================
    // SHUTDOWN DRAIN
    // ============================================================

    private async Task DrainRemainingMessagesAsync(
        ChannelReader<ConsumeWorkItem> reader,
        List<ConsumeWorkItem> batch)
    {
        using var flushCts =
            new CancellationTokenSource(
                ShutdownFlushTimeout);

        _logger.LogInformation(
            "Starting graceful shutdown drain. " +
            "Timeout={Timeout}s",
            ShutdownFlushTimeout.TotalSeconds);

        try
        {
            while (
                !flushCts.IsCancellationRequested &&
                reader.TryRead(out var item))
            {
                if (item.Kind == WorkItemKind.Process)
                {
                    batch.Add(item);

                    if (batch.Count >= _batchSize)
                    {
                        await ProcessAndSaveBatchAsync(
                            batch,
                            flushCts.Token);

                        batch.Clear();
                    }
                }
                else
                {
                    HandleSingleAckNack(item);
                }
            }

            if (
                batch.Count > 0 &&
                !flushCts.IsCancellationRequested)
            {
                await ProcessAndSaveBatchAsync(
                    batch,
                    flushCts.Token);

                batch.Clear();
            }
        }
        catch (OperationCanceledException)
        {
            /*
             * Không ACK phần chưa xử lý.
             *
             * RabbitMQ sẽ redeliver sau khi connection đóng.
             */
            _logger.LogWarning(
                "Graceful shutdown flush timed out.");
        }
        catch (Exception ex)
        {
            /*
             * Không swallow một cách im lặng.
             *
             * Nhưng shutdown vẫn phải tiếp tục.
             */
            _logger.LogError(
                ex,
                "Error while draining RabbitMQ messages during shutdown.");
        }

        _logger.LogInformation(
            "Graceful shutdown drain completed.");
    }

    // ============================================================
    // STOP
    // ============================================================

    public override async Task StopAsync(
        CancellationToken cancellationToken)
    {
        if (
            Interlocked.Exchange(
                ref _isStopping,
                1) == 1)
        {
            await base.StopAsync(
                cancellationToken);

            return;
        }

        _logger.LogInformation(
            "Stopping RabbitMQ consumer. Queue={Queue}",
            _queueConfig.QueueName);

        /*
         * Bước 1:
         *
         * Yêu cầu RabbitMQ ngừng gửi message mới.
         *
         * Quan trọng khi chạy Kubernetes:
         *
         * SIGTERM
         *   ↓
         * BasicCancel
         *   ↓
         * stop receiving
         *   ↓
         * flush current batch
         */
        TryCancelRabbitConsumer();

        /*
         * Bước 2:
         *
         * BackgroundService sẽ cancel stoppingToken.
         *
         * ProcessMessagesFromChannel sau đó drain queue nội bộ.
         */
        try
        {
            await base.StopAsync(
                cancellationToken);
        }
        finally
        {
            /*
             * Không còn producer mới sau BasicCancel.
             */
            _messageChannel.Writer.TryComplete();

            /*
             * Connection chỉ close sau khi worker đã drain.
             */
            CleanupRabbitMq();
        }

        _logger.LogInformation(
            "RabbitMQ consumer stopped. Queue={Queue}",
            _queueConfig.QueueName);
    }

    private void TryCancelRabbitConsumer()
    {
        if (string.IsNullOrWhiteSpace(_consumerTag))
        {
            return;
        }

        try
        {
            if (_channel?.IsOpen == true)
            {
                _channel.BasicCancel(
                    _consumerTag);

                _logger.LogInformation(
                    "RabbitMQ BasicCancel sent. " +
                    "ConsumerTag={ConsumerTag}",
                    _consumerTag);
            }
        }
        catch (Exception ex)
        {
            /*
             * Nếu BasicCancel fail:
             * tiếp tục shutdown.
             *
             * Khi connection đóng, unacked message
             * sẽ được RabbitMQ redeliver.
             */
            _logger.LogWarning(
                ex,
                "BasicCancel failed. ConsumerTag={ConsumerTag}",
                _consumerTag);
        }
    }

    // ============================================================
    // RABBIT EVENTS
    // ============================================================

    private void OnConnectionShutdown(
        object? sender,
        ShutdownEventArgs ea)
    {
        if (Volatile.Read(ref _isStopping) == 1)
        {
            _logger.LogInformation(
                "RabbitMQ connection shutdown during application stop. " +
                "ReplyCode={ReplyCode}, ReplyText={ReplyText}",
                ea.ReplyCode,
                ea.ReplyText);

            return;
        }

        _logger.LogWarning(
            "RabbitMQ connection unexpectedly shutdown. " +
            "ReplyCode={ReplyCode}, ReplyText={ReplyText}",
            ea.ReplyCode,
            ea.ReplyText);
    }

    private void OnModelShutdown(
        object? sender,
        ShutdownEventArgs ea)
    {
        if (Volatile.Read(ref _isStopping) == 1)
        {
            return;
        }

        _logger.LogWarning(
            "RabbitMQ channel unexpectedly shutdown. " +
            "ReplyCode={ReplyCode}, ReplyText={ReplyText}",
            ea.ReplyCode,
            ea.ReplyText);
    }

    private void OnConnectionCallbackException(
        object? sender,
        CallbackExceptionEventArgs ea)
    {
        _logger.LogError(
            ea.Exception,
            "RabbitMQ connection callback exception.");
    }

    private void OnChannelCallbackException(
        object? sender,
        CallbackExceptionEventArgs ea)
    {
        _logger.LogError(
            ea.Exception,
            "RabbitMQ channel callback exception.");
    }

    // ============================================================
    // CLEANUP
    // ============================================================

    private void CleanupRabbitMq()
    {
        try
        {
            if (_channel?.IsOpen == true)
            {
                _channel.Close();
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "Error while closing RabbitMQ channel.");
        }

        try
        {
            if (_connection?.IsOpen == true)
            {
                _connection.Close();
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "Error while closing RabbitMQ connection.");
        }

        try
        {
            _channel?.Dispose();
        }
        catch
        {
            // Ignore dispose exceptions.
        }

        try
        {
            _connection?.Dispose();
        }
        catch
        {
            // Ignore dispose exceptions.
        }

        _channel = null;
        _connection = null;
    }

    public override void Dispose()
    {
        Interlocked.Exchange(
            ref _isStopping,
            1);

        _messageChannel.Writer.TryComplete();

        CleanupRabbitMq();

        base.Dispose();
    }

    // ============================================================
    // INTERNAL WORK ITEM
    // ============================================================

    private sealed class ConsumeWorkItem
    {
        public ulong DeliveryTag { get; init; }

        public object? ParsedMessage { get; init; }

        public WorkItemKind Kind { get; init; }

        public bool Requeue { get; init; }

        public static ConsumeWorkItem ForProcessing(
            ulong deliveryTag,
            object parsedMessage)
        {
            return new ConsumeWorkItem
            {
                DeliveryTag = deliveryTag,
                ParsedMessage = parsedMessage,
                Kind = WorkItemKind.Process
            };
        }

        public static ConsumeWorkItem ForAck(
            ulong deliveryTag)
        {
            return new ConsumeWorkItem
            {
                DeliveryTag = deliveryTag,
                Kind = WorkItemKind.Ack
            };
        }

        public static ConsumeWorkItem ForNack(
            ulong deliveryTag,
            bool requeue)
        {
            return new ConsumeWorkItem
            {
                DeliveryTag = deliveryTag,
                Kind = WorkItemKind.Nack,
                Requeue = requeue
            };
        }
    }

    private enum WorkItemKind
    {
        Process = 0,
        Ack = 1,
        Nack = 2
    }
}