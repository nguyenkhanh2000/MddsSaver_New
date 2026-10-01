using MddsSaver.Core.Shared.Data;
using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Services
{
    public class GeneralOracleDataSaver : IDataSaver
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<GeneralOracleDataSaver> _logger;
        private const int MaxParallelBulkInserts = 4;
        public GeneralOracleDataSaver(IServiceProvider serviceProvider, ILogger<GeneralOracleDataSaver> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }
        public async Task SaveBatchAsync(List<object> messages, string sourceIdentifier, CancellationToken stoppingToken)
        {
            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * GroupBy chỉ giữ reference tới object.
                 *
                 * Không tạo:
                 *
                 * var items = group.ToList()
                 * rồi
                 * items.Cast<T>().ToList()
                 *
                 * như code cũ.
                 *
                 * Như vậy giảm được một List<object> trung gian
                 * cho mỗi loại message.
                 */
                var groupedMessages = messages
                    .Where(static m => m != null)
                    .GroupBy(static m => m.GetType());

                /*
                 * Một SaveBatchAsync có semaphore riêng.
                 *
                 * Điều này giới hạn số Oracle bulk của batch hiện tại.
                 */
                using var semaphore = new SemaphoreSlim(MaxParallelBulkInserts, MaxParallelBulkInserts);

                /*
                 * Số group thực tế rất nhỏ
                 * (tối đa bằng số message type ~ vài chục),
                 * nên List<Task> ở đây không đáng kể về RAM.
                 */
                var tasks = new List<Task>();

                foreach (var group in groupedMessages)
                {
                    stoppingToken.ThrowIfCancellationRequested();

                    /*
                     * Quan trọng:
                     *
                     * KHÔNG gọi BulkInsert...Async ở đây.
                     *
                     * Ta chỉ tạo Func<CancellationToken, Task>.
                     *
                     * BulkInsert chỉ thực sự được gọi SAU KHI
                     * Semaphore.WaitAsync thành công.
                     */
                    var insertAction =
                        CreateBulkInsertAction(
                            group.Key,
                            group);

                    if (insertAction == null)
                    {
                        _logger.LogWarning(
                            "[{Source}] Không tìm thấy BulkInsert handler cho type {MessageType}",
                            sourceIdentifier,
                            group.Key.FullName);

                        continue;
                    }

                    tasks.Add(
                        RunWithSemaphoreAsync(
                            insertAction,
                            semaphore,
                            stoppingToken));
                }

                /*
                 * Không Task.Run.
                 *
                 * Task.WhenAll ở đây chỉ chờ tối đa 4 công việc
                 * thực sự chạy song song.
                 */
                await Task.WhenAll(tasks)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "[{Source}] SaveBatchAsync bị hủy do application shutdown.",
                    sourceIdentifier);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "[{Source}] SaveBatchAsync: Lỗi khi thực hiện Oracle bulk insert.",
                    sourceIdentifier);

                /*
                 * Phải throw để BaseMessageConsumerWorker
                 * biết SaveBatch thất bại và KHÔNG ACK.
                 */
                throw;
            }
        }

        // ============================================================
        // CREATE BULK ACTION
        // ============================================================

        private Func<CancellationToken, Task>? CreateBulkInsertAction(Type type,IEnumerable<object> group)
        {
            /*
             * Mỗi group chỉ convert thành đúng 1 List<T>.
             *
             * Code cũ:
             *
             * group.ToList()
             *       ↓
             * List<object>
             *       ↓
             * Cast<T>().ToList()
             *       ↓
             * List<T>
             *
             *
             * Code mới:
             *
             * group.Cast<T>().ToList()
             *
             * => bỏ được List<object> trung gian.
             */

            if (type == typeof(ESecurityDefinition))
            {
                var items = group.Cast<ESecurityDefinition>().ToList();

                return token => BulkInsertSecurityDefinitionsAsync(items, token);
            }

            if (type == typeof(EPrice))
            {
                var items =
                    group.Cast<EPrice>().ToList();

                return token =>
                    BulkInsertPriceAsync(
                        items,
                        token);
            }

            if (type == typeof(EPriceRecovery))
            {
                var items =
                    group.Cast<EPriceRecovery>().ToList();

                return token =>
                    BulkInsertPriceRecoveryAsync(
                        items,
                        token);
            }

            if (type == typeof(ESecurityStatus))
            {
                var items =
                    group.Cast<ESecurityStatus>().ToList();

                return token =>
                    BulkInsertSecurityStatusAsync(
                        items,
                        token);
            }

            if (type == typeof(EIndex))
            {
                var items =
                    group.Cast<EIndex>().ToList();

                return token =>
                    BulkInsertIndexAsync(
                        items,
                        token);
            }

            if (type == typeof(EInvestorPerIndustry))
            {
                var items =
                    group.Cast<EInvestorPerIndustry>().ToList();

                return token =>
                    BulkInsertInvestorPerIndustryAsync(
                        items,
                        token);
            }

            if (type == typeof(EInvestorPerSymbol))
            {
                var items =
                    group.Cast<EInvestorPerSymbol>().ToList();

                return token =>
                    BulkInsertInvestorPerSymbolAsync(
                        items,
                        token);
            }

            if (type == typeof(ETopNMembersPerSymbol))
            {
                var items =
                    group.Cast<ETopNMembersPerSymbol>().ToList();

                return token =>
                    BulkInsertTopNMembersPerSymbolAsync(
                        items,
                        token);
            }

            if (type == typeof(ESecurityInformationNotification))
            {
                var items =
                    group.Cast<ESecurityInformationNotification>()
                        .ToList();

                return token =>
                    BulkInsertSecurityInfoNotificationAsync(
                        items,
                        token);
            }

            if (type == typeof(ESymbolClosingInformation))
            {
                var items =
                    group.Cast<ESymbolClosingInformation>()
                        .ToList();

                return token =>
                    BulkInsertSymbolClosingInfoAsync(
                        items,
                        token);
            }

            if (type == typeof(EOpenInterest))
            {
                var items =
                    group.Cast<EOpenInterest>().ToList();

                return token =>
                    BulkInsertOpenInterestAsync(
                        items,
                        token);
            }

            if (type == typeof(EVolatilityInterruption))
            {
                var items =
                    group.Cast<EVolatilityInterruption>()
                        .ToList();

                return token =>
                    BulkInsertVolatilityInterruptionAsync(
                        items,
                        token);
            }

            if (type == typeof(EDeemTradePrice))
            {
                var items =
                    group.Cast<EDeemTradePrice>().ToList();

                return token =>
                    BulkInsertDeemTradePriceAsync(
                        items,
                        token);
            }

            if (type == typeof(EForeignerOrderLimit))
            {
                var items =
                    group.Cast<EForeignerOrderLimit>().ToList();

                return token =>
                    BulkInsertForeignerOrderLimitAsync(
                        items,
                        token);
            }

            if (type == typeof(EMarketMakerInformation))
            {
                var items =
                    group.Cast<EMarketMakerInformation>()
                        .ToList();

                return token =>
                    BulkInsertMarketMakerInfoAsync(
                        items,
                        token);
            }

            if (type == typeof(ESymbolEvent))
            {
                var items =
                    group.Cast<ESymbolEvent>().ToList();

                return token =>
                    BulkInsertSymbolEventAsync(
                        items,
                        token);
            }

            if (type == typeof(EDrvProductEvent))
            {
                var items =
                    group.Cast<EDrvProductEvent>().ToList();

                return token =>
                    BulkInsertDrvProductEventAsync(
                        items,
                        token);
            }

            if (type == typeof(EIndexConstituentsInformation))
            {
                var items =
                    group.Cast<EIndexConstituentsInformation>()
                        .ToList();

                return token =>
                    BulkInsertIndexConstituentsAsync(
                        items,
                        token);
            }

            if (type == typeof(EETFiNav))
            {
                var items =
                    group.Cast<EETFiNav>().ToList();

                return token =>
                    BulkInsertETFiNavAsync(
                        items,
                        token);
            }

            if (type == typeof(EETFiIndex))
            {
                var items =
                    group.Cast<EETFiIndex>().ToList();

                return token =>
                    BulkInsertETFiIndexAsync(
                        items,
                        token);
            }

            if (type == typeof(EETFTrackingError))
            {
                var items =
                    group.Cast<EETFTrackingError>().ToList();

                return token =>
                    BulkInsertETFTrackingErrorAsync(
                        items,
                        token);
            }

            if (type == typeof(ETopNSymbolsWithTradingQuantity))
            {
                var items =
                    group.Cast<ETopNSymbolsWithTradingQuantity>()
                        .ToList();

                return token =>
                    BulkInsertTopNSymbolsWithTradingQuantityAsync(
                        items,
                        token);
            }

            if (type == typeof(ETopNSymbolsWithCurrentPrice))
            {
                var items =
                    group.Cast<ETopNSymbolsWithCurrentPrice>()
                        .ToList();

                return token =>
                    BulkInsertTopNSymbolsWithCurrentPriceAsync(
                        items,
                        token);
            }

            if (type == typeof(ETopNSymbolsWithHighRatioOfPrice))
            {
                var items =
                    group.Cast<ETopNSymbolsWithHighRatioOfPrice>()
                        .ToList();

                return token =>
                    BulkInsertTopNSymbolsWithHighRatioOfPriceAsync(
                        items,
                        token);
            }

            if (type == typeof(ETopNSymbolsWithLowRatioOfPrice))
            {
                var items =
                    group.Cast<ETopNSymbolsWithLowRatioOfPrice>()
                        .ToList();

                return token =>
                    BulkInsertTopNSymbolsWithLowRatioOfPriceAsync(
                        items,
                        token);
            }

            if (type == typeof(ETradingResultOfForeignInvestors))
            {
                var items =
                    group.Cast<ETradingResultOfForeignInvestors>()
                        .ToList();

                return token =>
                    BulkInsertTradingResultOfForeignInvestorsAsync(
                        items,
                        token);
            }

            if (type == typeof(EDisclosure))
            {
                var items =
                    group.Cast<EDisclosure>().ToList();

                return token =>
                    BulkInsertDisclosureAsync(
                        items,
                        token);
            }

            if (type == typeof(ERandomEnd))
            {
                var items =
                    group.Cast<ERandomEnd>().ToList();

                return token =>
                    BulkInsertRandomEndAsync(
                        items,
                        token);
            }

            if (type == typeof(EPriceLimitExpansion))
            {
                var items =
                    group.Cast<EPriceLimitExpansion>().ToList();

                return token =>
                    BulkInsertPriceLimitExpansionAsync(
                        items,
                        token);
            }

            return null;
        }

        // ============================================================
        // CONCURRENCY LIMIT
        // ============================================================

        private static async Task RunWithSemaphoreAsync(
            Func<CancellationToken, Task> insertAction,
            SemaphoreSlim semaphore,
            CancellationToken cancellationToken)
        {
            /*
             * QUAN TRỌNG:
             *
             * Semaphore được acquire TRƯỚC khi BulkInsert được gọi.
             */
            await semaphore
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                /*
                 * Chỉ từ đây BulkInsert mới bắt đầu chạy.
                 *
                 * Vì vậy:
                 *
                 * CreateDataTable()
                 * OracleConnection()
                 * OpenAsync()
                 * OracleBulkCopy()
                 *
                 * của tối đa 4 loại message được hoạt động cùng lúc.
                 */
                await insertAction(cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        }
        private async Task BulkInsertSecurityDefinitionsAsync(List<ESecurityDefinition> definitions, CancellationToken stoppingToken)
        {
            const string tableName = "Msg_d";

            if (definitions == null || definitions.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable được tạo sau khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 *
                 * Vì vậy số DataTable lớn tồn tại đồng thời
                 * được giới hạn bởi MaxParallelBulkInserts.
                 */
                using var dataTable =
                    CreateSecurityDefinitionDataTable(definitions);

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Mỗi bulk operation sử dụng một DI scope riêng.
                 */
                using var scope =
                    _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * Chỉ mở Oracle connection sau khi DataTable
                 * đã được build xong.
                 *
                 * Như vậy connection được giữ trong thời gian ngắn hơn.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping column một lần.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng:
                 *
                 * await Task.Run(
                 *     () => bulkCopy.WriteToServer(dataTable))
                 *
                 * OracleBulkCopy.WriteToServer là synchronous.
                 *
                 * Task.Run chỉ chuyển blocking sang ThreadPool
                 * và gây thêm scheduling/context-switch.
                 *
                 * Concurrency đã được giới hạn ở
                 * GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    definitions.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    definitions.Count);

                throw;
            }
        }
        private async Task BulkInsertPriceAsync(List<EPrice> prices, CancellationToken stoppingToken)
        {
            const string tableName = "Msg_x";

            if (prices == null || prices.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * QUAN TRỌNG:
                 *
                 * Với GeneralOracleDataSaver đã sửa:
                 * method này chỉ được gọi SAU KHI đã acquire Semaphore.
                 *
                 * Nghĩa là DataTable lớn chỉ được tạo tối đa
                 * MaxParallelBulkInserts cùng lúc.
                 */
                using var dataTable = CreatePriceDataTable(prices);

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Scope sống đúng bằng lifetime của một bulk operation.
                 */
                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * OpenAsync là async I/O thật.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping chỉ thực hiện một lần cho DataTable.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng:
                 *
                 * await Task.Run(() => bulkCopy.WriteToServer(...))
                 *
                 * WriteToServer là synchronous.
                 *
                 * Task.Run chỉ chuyển blocking sang ThreadPool thread khác,
                 * không biến OracleBulkCopy thành async.
                 *
                 * GeneralOracleDataSaver bên ngoài đã giới hạn concurrency,
                 * ví dụ tối đa 4 bulk chạy đồng thời.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    prices.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    prices.Count);

                /*
                 * Giữ nguyên stack trace gốc.
                 */
                throw;
            }
        }

        private async Task BulkInsertPriceRecoveryAsync(List<EPriceRecovery> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_w";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable chỉ bắt đầu tạo sau khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 *
                 * Nhờ đó số DataTable lớn tồn tại đồng thời được giới hạn.
                 */
                using var dataTable = CreatePriceRecoveryDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Scope riêng cho từng bulk operation.
                 */
                using var scope = _serviceProvider.CreateScope();

                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * Open Oracle connection sau khi DataTable đã được build.
                 * Giảm thời gian giữ connection.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping 147 column chỉ thực hiện 1 lần/batch.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 *
                 * OracleBulkCopy.WriteToServer là synchronous.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertSecurityStatusAsync(List<ESecurityStatus> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_f";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable được tạo sau khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 */
                using var dataTable = CreateSecurityStatusDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping chỉ một lần cho toàn batch.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn bên ngoài.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertIndexAsync(List<EIndex> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m1";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable chỉ được tạo sau khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 */
                using var dataTable = CreateIndexDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * Chỉ mở connection sau khi DataTable đã build xong.
                 * Giảm thời gian giữ Oracle connection.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping một lần cho toàn batch.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 *
                 * OracleBulkCopy.WriteToServer là synchronous.
                 * Concurrency đã được kiểm soát ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertInvestorPerIndustryAsync(
    List<EInvestorPerIndustry> messages,
    CancellationToken stoppingToken)
        {
            const string tableName = "msg_m2";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable chỉ được tạo khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 */
                using var dataTable = CreateInvestorPerIndustryDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Scope riêng cho từng bulk operation.
                 */
                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * Chỉ mở Oracle connection sau khi DataTable
                 * đã được build xong.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping chỉ một lần cho toàn batch.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 *
                 * OracleBulkCopy.WriteToServer là synchronous.
                 * Số bulk chạy đồng thời đã được giới hạn bởi
                 * GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertInvestorPerSymbolAsync(List<EInvestorPerSymbol> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m3";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                /*
                 * DataTable chỉ được tạo sau khi GeneralOracleDataSaver
                 * đã acquire semaphore.
                 */
                using var dataTable = CreateInvestorPerSymbolDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Scope riêng cho từng bulk operation.
                 */
                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                /*
                 * Chỉ mở connection sau khi DataTable build xong
                 * để giảm thời gian giữ Oracle connection.
                 */
                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                /*
                 * Mapping một lần cho toàn batch.
                 */
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 *
                 * OracleBulkCopy.WriteToServer là synchronous.
                 * Concurrency đã được giới hạn bởi GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTopNMembersPerSymbolAsync(List<ETopNMembersPerSymbol> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m4";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTopNMembersPerSymbolDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertSecurityInfoNotificationAsync(List<ESecurityInformationNotification> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m7";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateSecurityInfoNotificationDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertSymbolClosingInfoAsync(List<ESymbolClosingInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m8";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateSymbolClosingInfoDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertOpenInterestAsync(List<EOpenInterest> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ma";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateOpenInterestDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertVolatilityInterruptionAsync(List<EVolatilityInterruption> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_md";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateVolatilityInterruptionDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn bên ngoài.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertDeemTradePriceAsync(List<EDeemTradePrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_me";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateDeemTradePriceDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertForeignerOrderLimitAsync(List<EForeignerOrderLimit> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mf";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateForeignerOrderLimitDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertMarketMakerInfoAsync(List<EMarketMakerInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mh";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateMarketMakerInfoDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertSymbolEventAsync(List<ESymbolEvent> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mi";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateSymbolEventDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertDrvProductEventAsync(List<EDrvProductEvent> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mj";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateDrvProductEventDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertIndexConstituentsAsync(List<EIndexConstituentsInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ml";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateIndexConstituentsDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertETFiNavAsync(List<EETFiNav> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mm";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateETFiNavDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertETFiIndexAsync(List<EETFiIndex> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mn";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateETFiIndexDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertETFTrackingErrorAsync(List<EETFTrackingError> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mo";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateETFTrackingErrorDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTopNSymbolsWithTradingQuantityAsync(List<ETopNSymbolsWithTradingQuantity> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mp";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTopNSymbolsWithTradingQuantityDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTopNSymbolsWithCurrentPriceAsync(List<ETopNSymbolsWithCurrentPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mq";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTopNSymbolsWithCurrentPriceDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTopNSymbolsWithHighRatioOfPriceAsync(List<ETopNSymbolsWithHighRatioOfPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mr";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTopNSymbolsWithHighRatioOfPriceDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                /*
                 * Không dùng Task.Run.
                 * Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                 */
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTopNSymbolsWithLowRatioOfPriceAsync(List<ETopNSymbolsWithLowRatioOfPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ms";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTopNSymbolsWithLowRatioOfPriceDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                // Không dùng Task.Run.
                // Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertTradingResultOfForeignInvestorsAsync(List<ETradingResultOfForeignInvestors> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mt";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateTradingResultOfForeignInvestorsDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                // Không dùng Task.Run.
                // Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertDisclosureAsync(List<EDisclosure> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mu";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateDisclosureDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                // Không dùng Task.Run.
                // Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertRandomEndAsync(List<ERandomEnd> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mw";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreateRandomEndDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                // Không dùng Task.Run.
                // Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private async Task BulkInsertPriceLimitExpansionAsync(List<EPriceLimitExpansion> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mx";

            if (messages == null || messages.Count == 0)
            {
                return;
            }

            stoppingToken.ThrowIfCancellationRequested();

            try
            {
                using var dataTable = CreatePriceLimitExpansionDataTable(messages);

                stoppingToken.ThrowIfCancellationRequested();

                using var scope = _serviceProvider.CreateScope();

                var dbConnection =
                    scope.ServiceProvider
                        .GetRequiredService<IDbConnection>();

                if (dbConnection is not OracleConnection connection)
                {
                    throw new InvalidOperationException(
                        $"IDbConnection phải là OracleConnection. " +
                        $"Actual type: {dbConnection?.GetType().FullName ?? "null"}");
                }

                await connection
                    .OpenAsync(stoppingToken)
                    .ConfigureAwait(false);

                stoppingToken.ThrowIfCancellationRequested();

                using var bulkCopy =
                    new OracleBulkCopy(connection)
                    {
                        DestinationTableName = tableName,
                        BulkCopyTimeout = 600
                    };

                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(
                        column.ColumnName,
                        column.ColumnName);
                }

                stoppingToken.ThrowIfCancellationRequested();

                // Không dùng Task.Run.
                // Concurrency đã được giới hạn ở GeneralOracleDataSaver.
                bulkCopy.WriteToServer(dataTable);
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation(
                    "Oracle Bulk Insert bị hủy do application shutdown. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Lỗi Oracle Bulk Insert. " +
                    "Table={TableName}, Count={Count}",
                    tableName,
                    messages.Count);

                throw;
            }
        }
        private DataTable CreateSecurityDefinitionDataTable(List<ESecurityDefinition> definitions)
        {
            if (definitions == null)
            {
                throw new ArgumentNullException(nameof(definitions));
            }

            /*
             * Cho DataTable biết trước số lượng row dự kiến,
             * giảm việc mở rộng internal storage nhiều lần.
             */
            var dt = new DataTable
            {
                MinimumCapacity = definitions.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // --------------------------------------------------------
                // HEADER
                //
                // Ordinal 0 -> 8
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // --------------------------------------------------------
                // PAYLOAD
                //
                // Ordinal 9 -> 53
                // --------------------------------------------------------

                dt.Columns.Add(
                    MsgDSchema.TotNumReports,
                    typeof(long));                           // 9

                dt.Columns.Add(
                    MsgDSchema.SecurityExchange,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgDSchema.Symbol,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgDSchema.TickerCode,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgDSchema.SymbolShortCode,
                    typeof(string));                         // 13

                dt.Columns.Add(
                    MsgDSchema.SymbolName,
                    typeof(string));                         // 14

                dt.Columns.Add(
                    MsgDSchema.SymbolEnName,
                    typeof(string));                         // 15

                dt.Columns.Add(
                    MsgDSchema.ProductId,
                    typeof(string));                         // 16

                dt.Columns.Add(
                    MsgDSchema.ProductGrpId,
                    typeof(string));                         // 17

                dt.Columns.Add(
                    MsgDSchema.SecurityGroupId,
                    typeof(string));                         // 18

                dt.Columns.Add(
                    MsgDSchema.PutOrCall,
                    typeof(string));                         // 19

                dt.Columns.Add(
                    MsgDSchema.ExerciseStyle,
                    typeof(string));                         // 20

                dt.Columns.Add(
                    MsgDSchema.MaturityMonthYear,
                    typeof(string));                         // 21

                dt.Columns.Add(
                    MsgDSchema.MaturityDate,
                    typeof(string));                         // 22

                dt.Columns.Add(
                    MsgDSchema.Issuer,
                    typeof(string));                         // 23

                dt.Columns.Add(
                    MsgDSchema.IssueDate,
                    typeof(string));                         // 24

                dt.Columns.Add(
                    MsgDSchema.ContractMultiplier,
                    typeof(decimal));                        // 25

                dt.Columns.Add(
                    MsgDSchema.CouponRate,
                    typeof(decimal));                        // 26

                dt.Columns.Add(
                    MsgDSchema.Currency,
                    typeof(string));                         // 27

                dt.Columns.Add(
                    MsgDSchema.ListedShares,
                    typeof(long));                           // 28

                dt.Columns.Add(
                    MsgDSchema.HighLimitPrice,
                    typeof(decimal));                        // 29

                dt.Columns.Add(
                    MsgDSchema.LowLimitPrice,
                    typeof(decimal));                        // 30

                dt.Columns.Add(
                    MsgDSchema.StrikePrice,
                    typeof(decimal));                        // 31

                dt.Columns.Add(
                    MsgDSchema.SecurityStatus,
                    typeof(string));                         // 32

                dt.Columns.Add(
                    MsgDSchema.ContractSize,
                    typeof(decimal));                        // 33

                dt.Columns.Add(
                    MsgDSchema.SettlMethod,
                    typeof(string));                         // 34

                dt.Columns.Add(
                    MsgDSchema.Yield,
                    typeof(decimal));                        // 35

                dt.Columns.Add(
                    MsgDSchema.ReferencePrice,
                    typeof(decimal));                        // 36

                dt.Columns.Add(
                    MsgDSchema.EvaluationPrice,
                    typeof(decimal));                        // 37

                dt.Columns.Add(
                    MsgDSchema.HgstOrderPrice,
                    typeof(decimal));                        // 38

                dt.Columns.Add(
                    MsgDSchema.LwstOrderPrice,
                    typeof(decimal));                        // 39

                dt.Columns.Add(
                    MsgDSchema.PrevClosePx,
                    typeof(decimal));                        // 40

                dt.Columns.Add(
                    MsgDSchema.SymbolCloseInfoPxType,
                    typeof(string));                         // 41

                dt.Columns.Add(
                    MsgDSchema.FirstTradingDate,
                    typeof(string));                         // 42

                dt.Columns.Add(
                    MsgDSchema.FinalTradeDate,
                    typeof(string));                         // 43

                dt.Columns.Add(
                    MsgDSchema.FinalSettleDate,
                    typeof(string));                         // 44

                dt.Columns.Add(
                    MsgDSchema.ListingDate,
                    typeof(string));                         // 45

                dt.Columns.Add(
                    MsgDSchema.ReTriggeringConditionCode,
                    typeof(string));                         // 46

                dt.Columns.Add(
                    MsgDSchema.ExClassType,
                    typeof(string));                         // 47

                dt.Columns.Add(
                    MsgDSchema.VWap,
                    typeof(decimal));                        // 48

                dt.Columns.Add(
                    MsgDSchema.SymbolAdminStatusCode,
                    typeof(string));                         // 49

                dt.Columns.Add(
                    MsgDSchema.SymbolTradingMethodSc,
                    typeof(string));                         // 50

                dt.Columns.Add(
                    MsgDSchema.SymbolTradingSantionSc,
                    typeof(string));                         // 51

                dt.Columns.Add(
                    MsgDSchema.SectorTypeCode,
                    typeof(string));                         // 52

                dt.Columns.Add(
                    MsgDSchema.RedumptionDate,
                    typeof(string));                         // 53


                // --------------------------------------------------------
                // FOOTER
                //
                // Ordinal 54 -> 55
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 54

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 55


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                /*
                 * Code cũ gọi DateTime.Now cho từng record.
                 *
                 * Không cần thiết.
                 *
                 * Dùng một timestamp cho cả batch:
                 * - giảm call
                 * - dữ liệu nhất quán hơn
                 */
                var batchCreateTime = DateTime.Now;

                /*
                 * DataTable sẽ giảm overhead khi bulk loading.
                 */
                dt.BeginLoadData();

                foreach (var def in definitions)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        def.BeginString != null
                            ? def.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)def.BodyLength;

                    row[2] =
                        def.MsgType != null
                            ? def.MsgType
                            : DBNull.Value;

                    row[3] =
                        def.SenderCompID != null
                            ? def.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        def.TargetCompID != null
                            ? def.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        def.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            def.SendingTime);

                    row[7] =
                        def.MarketID != null
                            ? def.MarketID
                            : DBNull.Value;

                    row[8] =
                        def.BoardID != null
                            ? def.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[9] =
                        def.TotNumReports;

                    row[10] =
                        def.SecurityExchange != null
                            ? def.SecurityExchange
                            : DBNull.Value;

                    row[11] =
                        def.Symbol != null
                            ? def.Symbol
                            : DBNull.Value;

                    row[12] =
                        def.TickerCode != null
                            ? def.TickerCode
                            : DBNull.Value;

                    row[13] =
                        def.SymbolShortCode != null
                            ? def.SymbolShortCode
                            : DBNull.Value;

                    row[14] =
                        def.SymbolName != null
                            ? def.SymbolName
                            : DBNull.Value;

                    row[15] =
                        def.SymbolEnglishName != null
                            ? def.SymbolEnglishName
                            : DBNull.Value;

                    row[16] =
                        def.ProductID != null
                            ? def.ProductID
                            : DBNull.Value;

                    row[17] =
                        def.ProductGrpID != null
                            ? def.ProductGrpID
                            : DBNull.Value;

                    row[18] =
                        def.SecurityGroupID != null
                            ? def.SecurityGroupID
                            : DBNull.Value;

                    row[19] =
                        def.PutOrCall != null
                            ? def.PutOrCall
                            : DBNull.Value;

                    row[20] =
                        def.ExerciseStyle != null
                            ? def.ExerciseStyle
                            : DBNull.Value;

                    row[21] =
                        def.MaturityMonthYear != null
                            ? def.MaturityMonthYear
                            : DBNull.Value;

                    row[22] =
                        def.MaturityDate != null
                            ? def.MaturityDate
                            : DBNull.Value;

                    row[23] =
                        def.Issuer != null
                            ? def.Issuer
                            : DBNull.Value;

                    row[24] =
                        def.IssueDate != null
                            ? def.IssueDate
                            : DBNull.Value;


                    // ====================================================
                    // NUMERIC FIELDS
                    // ====================================================

                    row[25] =
                        def.ContractMultiplier;

                    row[26] =
                        def.CouponRate;

                    row[27] =
                        def.Currency != null
                            ? def.Currency
                            : DBNull.Value;

                    row[28] =
                        def.ListedShares;

                    row[29] =
                        def.HighLimitPrice;

                    row[30] =
                        def.LowLimitPrice;

                    row[31] =
                        def.StrikePrice;

                    row[32] =
                        def.SecurityStatus != null
                            ? def.SecurityStatus
                            : DBNull.Value;

                    row[33] =
                        def.ContractSize;

                    row[34] =
                        def.SettlMethod != null
                            ? def.SettlMethod
                            : DBNull.Value;

                    row[35] =
                        def.Yield;

                    row[36] =
                        def.ReferencePrice;

                    row[37] =
                        def.EvaluationPrice;

                    row[38] =
                        def.HgstOrderPrice;

                    row[39] =
                        def.LwstOrderPrice;

                    row[40] =
                        def.PrevClosePx;


                    // ====================================================
                    // STRING / STATUS FIELDS
                    // ====================================================

                    row[41] =
                        def.SymbolCloseInfoPxType != null
                            ? def.SymbolCloseInfoPxType
                            : DBNull.Value;

                    row[42] =
                        def.FirstTradingDate != null
                            ? def.FirstTradingDate
                            : DBNull.Value;

                    row[43] =
                        def.FinalTradeDate != null
                            ? def.FinalTradeDate
                            : DBNull.Value;

                    row[44] =
                        def.FinalSettleDate != null
                            ? def.FinalSettleDate
                            : DBNull.Value;

                    row[45] =
                        def.ListingDate != null
                            ? def.ListingDate
                            : DBNull.Value;

                    /*
                     * Giữ nguyên mapping code cũ:
                     *
                     * ReTriggeringConditionCode
                     * <- RandomEndTriggeringConditionCode
                     */
                    row[46] =
                        def.RandomEndTriggeringConditionCode != null
                            ? def.RandomEndTriggeringConditionCode
                            : DBNull.Value;

                    row[47] =
                        def.ExClassType != null
                            ? def.ExClassType
                            : DBNull.Value;

                    row[48] = def.VWAP;

                    row[49] =
                        def.SymbolAdminStatusCode != null
                            ? def.SymbolAdminStatusCode
                            : DBNull.Value;

                    row[50] =
                        def.SymbolTradingMethodStatusCode != null
                            ? def.SymbolTradingMethodStatusCode
                            : DBNull.Value;

                    row[51] =
                        def.SymbolTradingSantionStatusCode != null
                            ? def.SymbolTradingSantionStatusCode
                            : DBNull.Value;

                    row[52] =
                        def.SectorTypeCode != null
                            ? def.SectorTypeCode
                            : DBNull.Value;

                    row[53] =
                        def.RedumptionDate != null
                            ? def.RedumptionDate
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            def.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[54] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[54] =
                            DBNull.Value;
                    }

                    row[55] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                /*
                 * Nếu build DataTable lỗi:
                 * release reference càng sớm càng tốt.
                 */
                dt.Dispose();

                /*
                 * Không dùng throw ex;
                 * để giữ nguyên stack trace gốc.
                 */
                throw;
            }
        }
        private DataTable CreatePriceDataTable(List<EPrice> prices)
        {
            if (prices == null)
            {
                throw new ArgumentNullException(nameof(prices));
            }

            /*
             * MinimumCapacity giúp DataTable biết trước số row dự kiến,
             * giảm việc mở rộng internal storage nhiều lần.
             */
            var dt = new DataTable
            {
                MinimumCapacity = prices.Count
            };

            // ============================================================
            // 1. DEFINE COLUMNS
            // ============================================================

            // ------------------------------------------------------------
            // HEADER
            //
            // Ordinal:
            // 0 -> 8
            // ------------------------------------------------------------

            dt.Columns.Add(
                BaseMessageSchema.BeginString,
                typeof(string));                         // 0

            dt.Columns.Add(
                BaseMessageSchema.BodyLength,
                typeof(int));                            // 1

            dt.Columns.Add(
                BaseMessageSchema.MsgType,
                typeof(string));                         // 2

            dt.Columns.Add(
                BaseMessageSchema.SenderCompId,
                typeof(string));                         // 3

            dt.Columns.Add(
                BaseMessageSchema.TargetCompId,
                typeof(string));                         // 4

            dt.Columns.Add(
                BaseMessageSchema.MsgSeqNum,
                typeof(long));                           // 5

            dt.Columns.Add(
                BaseMessageSchema.SendingTime,
                typeof(DateTime));                       // 6

            dt.Columns.Add(
                BaseMessageSchema.MarketId,
                typeof(string));                         // 7

            dt.Columns.Add(
                BaseMessageSchema.BoardId,
                typeof(string));                         // 8


            // ------------------------------------------------------------
            // PAYLOAD
            //
            // Ordinal:
            // 9 -> 12
            // ------------------------------------------------------------

            dt.Columns.Add(
                MsgXSchema.TradingSessionId,
                typeof(string));                         // 9

            dt.Columns.Add(
                MsgXSchema.Symbol,
                typeof(string));                         // 10

            dt.Columns.Add(
                MsgXSchema.TradeDate,
                typeof(DateTime));                       // 11

            dt.Columns.Add(
                MsgXSchema.TransactTime,
                typeof(string));                         // 12


            // ------------------------------------------------------------
            // STATISTICS
            //
            // Ordinal:
            // 13 -> 18
            // ------------------------------------------------------------

            dt.Columns.Add(
                MsgXSchema.TotalVolumeTraded,
                typeof(long));                           // 13

            dt.Columns.Add(
                MsgXSchema.GrossTradeAmt,
                typeof(decimal));                        // 14

            dt.Columns.Add(
                MsgXSchema.SellTotOrderQty,
                typeof(long));                           // 15

            dt.Columns.Add(
                MsgXSchema.BuyTotOrderQty,
                typeof(long));                           // 16

            dt.Columns.Add(
                MsgXSchema.SellValidOrderCnt,
                typeof(long));                           // 17

            dt.Columns.Add(
                MsgXSchema.BuyValidOrderCnt,
                typeof(long));                           // 18


            // ------------------------------------------------------------
            // DEPTH LEVEL 1 -> 10
            //
            // Mỗi level = 12 columns
            //
            // Level 1 : 19  -> 30
            // Level 2 : 31  -> 42
            // Level 3 : 43  -> 54
            // Level 4 : 55  -> 66
            // Level 5 : 67  -> 78
            // Level 6 : 79  -> 90
            // Level 7 : 91  -> 102
            // Level 8 : 103 -> 114
            // Level 9 : 115 -> 126
            // Level 10: 127 -> 138
            // ------------------------------------------------------------

            for (int i = 1; i <= 10; i++)
            {
                // BUY
                dt.Columns.Add(
                    $"{MsgXSchema.BpPrefix}{i}",
                    typeof(decimal));

                dt.Columns.Add(
                    $"{MsgXSchema.BqPrefix}{i}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Noo}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdey}",
                    typeof(decimal));

                dt.Columns.Add(
                    $"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdemms}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdepno}",
                    typeof(int));


                // SELL
                dt.Columns.Add(
                    $"{MsgXSchema.SpPrefix}{i}",
                    typeof(decimal));

                dt.Columns.Add(
                    $"{MsgXSchema.SqPrefix}{i}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Noo}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdey}",
                    typeof(decimal));

                dt.Columns.Add(
                    $"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdemms}",
                    typeof(long));

                dt.Columns.Add(
                    $"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdepno}",
                    typeof(int));
            }


            // ------------------------------------------------------------
            // LAST PRICE FIELDS
            //
            // Ordinal:
            // 139 -> 143
            // ------------------------------------------------------------

            dt.Columns.Add(
                MsgXSchema.Mp,
                typeof(decimal));                        // 139

            dt.Columns.Add(
                MsgXSchema.Mq,
                typeof(long));                           // 140

            dt.Columns.Add(
                MsgXSchema.Op,
                typeof(decimal));                        // 141

            dt.Columns.Add(
                MsgXSchema.Lp,
                typeof(decimal));                        // 142

            dt.Columns.Add(
                MsgXSchema.Hp,
                typeof(decimal));                        // 143


            // ------------------------------------------------------------
            // FOOTER
            //
            // 144 -> 145
            // ------------------------------------------------------------

            dt.Columns.Add(
                BaseMessageSchema.Checksum,
                typeof(long));                           // 144

            dt.Columns.Add(
                BaseMessageSchema.CreateTime,
                typeof(DateTime));                       // 145


            // ============================================================
            // 2. LOAD DATA
            // ============================================================

            /*
             * Một timestamp cho cả batch.
             *
             * Code cũ gọi DateTime.Now cho từng row.
             */
            var batchCreateTime = DateTime.Now;

            /*
             * BeginLoadData:
             * giảm một số overhead event/index/constraint trong lúc bulk load.
             */
            dt.BeginLoadData();

            try
            {
                foreach (var price in prices)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        price.BeginString != null
                            ? price.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)price.BodyLength;

                    row[2] =
                        price.MsgType != null
                            ? price.MsgType
                            : DBNull.Value;

                    row[3] =
                        price.SenderCompID != null
                            ? price.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        price.TargetCompID != null
                            ? price.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        price.MsgSeqNum;

                    row[6] =
                        ParseCompactDateTimeToDbNull(
                            price.SendingTime);

                    row[7] =
                        price.MarketID != null
                            ? price.MarketID
                            : DBNull.Value;

                    row[8] =
                        price.BoardID != null
                            ? price.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[9] =
                        price.TradingSessionID != null
                            ? price.TradingSessionID
                            : DBNull.Value;

                    row[10] =
                        price.Symbol != null
                            ? price.Symbol
                            : DBNull.Value;

                    row[11] =
                        ParseCompactDateToDbNull(
                            price.TradeDate);

                    row[12] =
                        price.TransactTime != null
                            ? price.TransactTime
                            : DBNull.Value;


                    // ====================================================
                    // STATISTICS
                    // ====================================================

                    row[13] =
                        ToDbNull(
                            price.TotalVolumeTraded);

                    row[14] =
                        ToDbNull(
                            price.GrossTradeAmt);

                    row[15] =
                        ToDbNull(
                            price.SellTotOrderQty);

                    row[16] =
                        ToDbNull(
                            price.BuyTotOrderQty);

                    row[17] =
                        ToDbNull(
                            price.SellValidOrderCnt);

                    row[18] =
                        ToDbNull(
                            price.BuyValidOrderCnt);


                    // ====================================================
                    // LEVEL 1
                    //
                    // 19 -> 30
                    // ====================================================

                    row[19] =
                        ToDbNull(price.BuyPrice1);

                    row[20] =
                        ToDbNull(price.BuyQuantity1);

                    row[21] =
                        ToDbNull((long)price.BuyPrice1_NOO);

                    row[22] =
                        ToDbNull(price.BuyPrice1_MDEY);

                    row[23] =
                        ToDbNull(price.BuyPrice1_MDEMMS);

                    row[24] =
                        DBNull.Value;

                    row[25] =
                        ToDbNull(price.SellPrice1);

                    row[26] =
                        ToDbNull(price.SellQuantity1);

                    row[27] =
                        ToDbNull((long)price.SellPrice1_NOO);

                    row[28] =
                        ToDbNull(price.SellPrice1_MDEY);

                    row[29] =
                        ToDbNull(price.SellPrice1_MDEMMS);

                    row[30] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 2
                    //
                    // 31 -> 42
                    // ====================================================

                    row[31] =
                        ToDbNull(price.BuyPrice2);

                    row[32] =
                        ToDbNull(price.BuyQuantity2);

                    row[33] =
                        ToDbNull((long)price.BuyPrice2_NOO);

                    row[34] =
                        ToDbNull(price.BuyPrice2_MDEY);

                    row[35] =
                        ToDbNull(price.BuyPrice2_MDEMMS);

                    row[36] =
                        DBNull.Value;

                    row[37] =
                        ToDbNull(price.SellPrice2);

                    row[38] =
                        ToDbNull(price.SellQuantity2);

                    row[39] =
                        ToDbNull((long)price.SellPrice2_NOO);

                    row[40] =
                        ToDbNull(price.SellPrice2_MDEY);

                    row[41] =
                        ToDbNull(price.SellPrice2_MDEMMS);

                    row[42] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 3
                    //
                    // 43 -> 54
                    // ====================================================

                    row[43] =
                        ToDbNull(price.BuyPrice3);

                    row[44] =
                        ToDbNull(price.BuyQuantity3);

                    row[45] =
                        ToDbNull((long)price.BuyPrice3_NOO);

                    row[46] =
                        ToDbNull(price.BuyPrice3_MDEY);

                    row[47] =
                        ToDbNull(price.BuyPrice3_MDEMMS);

                    row[48] =
                        DBNull.Value;

                    row[49] =
                        ToDbNull(price.SellPrice3);

                    row[50] =
                        ToDbNull(price.SellQuantity3);

                    row[51] =
                        ToDbNull((long)price.SellPrice3_NOO);

                    row[52] =
                        ToDbNull(price.SellPrice3_MDEY);

                    row[53] =
                        ToDbNull(price.SellPrice3_MDEMMS);

                    row[54] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 4
                    //
                    // 55 -> 66
                    // ====================================================

                    row[55] =
                        ToDbNull(price.BuyPrice4);

                    row[56] =
                        ToDbNull(price.BuyQuantity4);

                    row[57] =
                        ToDbNull((long)price.BuyPrice4_NOO);

                    row[58] =
                        ToDbNull(price.BuyPrice4_MDEY);

                    row[59] =
                        ToDbNull(price.BuyPrice4_MDEMMS);

                    row[60] =
                        DBNull.Value;

                    row[61] =
                        ToDbNull(price.SellPrice4);

                    row[62] =
                        ToDbNull(price.SellQuantity4);

                    row[63] =
                        ToDbNull((long)price.SellPrice4_NOO);

                    row[64] =
                        ToDbNull(price.SellPrice4_MDEY);

                    row[65] =
                        ToDbNull(price.SellPrice4_MDEMMS);

                    row[66] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 5
                    //
                    // 67 -> 78
                    // ====================================================

                    row[67] =
                        ToDbNull(price.BuyPrice5);

                    row[68] =
                        ToDbNull(price.BuyQuantity5);

                    row[69] =
                        ToDbNull((long)price.BuyPrice5_NOO);

                    row[70] =
                        ToDbNull(price.BuyPrice5_MDEY);

                    row[71] =
                        ToDbNull(price.BuyPrice5_MDEMMS);

                    row[72] =
                        DBNull.Value;

                    row[73] =
                        ToDbNull(price.SellPrice5);

                    row[74] =
                        ToDbNull(price.SellQuantity5);

                    row[75] =
                        ToDbNull((long)price.SellPrice5_NOO);

                    row[76] =
                        ToDbNull(price.SellPrice5_MDEY);

                    row[77] =
                        ToDbNull(price.SellPrice5_MDEMMS);

                    row[78] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 6
                    //
                    // 79 -> 90
                    // ====================================================

                    row[79] =
                        ToDbNull(price.BuyPrice6);

                    row[80] =
                        ToDbNull(price.BuyQuantity6);

                    row[81] =
                        ToDbNull((long)price.BuyPrice6_NOO);

                    row[82] =
                        ToDbNull(price.BuyPrice6_MDEY);

                    row[83] =
                        ToDbNull(price.BuyPrice6_MDEMMS);

                    row[84] =
                        DBNull.Value;

                    row[85] =
                        ToDbNull(price.SellPrice6);

                    row[86] =
                        ToDbNull(price.SellQuantity6);

                    row[87] =
                        ToDbNull((long)price.SellPrice6_NOO);

                    row[88] =
                        ToDbNull(price.SellPrice6_MDEY);

                    row[89] =
                        ToDbNull(price.SellPrice6_MDEMMS);

                    row[90] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 7
                    //
                    // 91 -> 102
                    // ====================================================

                    row[91] =
                        ToDbNull(price.BuyPrice7);

                    row[92] =
                        ToDbNull(price.BuyQuantity7);

                    row[93] =
                        ToDbNull((long)price.BuyPrice7_NOO);

                    row[94] =
                        ToDbNull(price.BuyPrice7_MDEY);

                    row[95] =
                        ToDbNull(price.BuyPrice7_MDEMMS);

                    row[96] =
                        DBNull.Value;

                    row[97] =
                        ToDbNull(price.SellPrice7);

                    row[98] =
                        ToDbNull(price.SellQuantity7);

                    row[99] =
                        ToDbNull((long)price.SellPrice7_NOO);

                    row[100] =
                        ToDbNull(price.SellPrice7_MDEY);

                    row[101] =
                        ToDbNull(price.SellPrice7_MDEMMS);

                    row[102] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 8
                    //
                    // 103 -> 114
                    // ====================================================

                    row[103] =
                        ToDbNull(price.BuyPrice8);

                    row[104] =
                        ToDbNull(price.BuyQuantity8);

                    row[105] =
                        ToDbNull((long)price.BuyPrice8_NOO);

                    row[106] =
                        ToDbNull(price.BuyPrice8_MDEY);

                    row[107] =
                        ToDbNull(price.BuyPrice8_MDEMMS);

                    row[108] =
                        DBNull.Value;

                    row[109] =
                        ToDbNull(price.SellPrice8);

                    row[110] =
                        ToDbNull(price.SellQuantity8);

                    row[111] =
                        ToDbNull((long)price.SellPrice8_NOO);

                    row[112] =
                        ToDbNull(price.SellPrice8_MDEY);

                    row[113] =
                        ToDbNull(price.SellPrice8_MDEMMS);

                    row[114] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 9
                    //
                    // 115 -> 126
                    // ====================================================

                    row[115] =
                        ToDbNull(price.BuyPrice9);

                    row[116] =
                        ToDbNull(price.BuyQuantity9);

                    row[117] =
                        ToDbNull((long)price.BuyPrice9_NOO);

                    row[118] =
                        ToDbNull(price.BuyPrice9_MDEY);

                    row[119] =
                        ToDbNull(price.BuyPrice9_MDEMMS);

                    row[120] =
                        DBNull.Value;

                    row[121] =
                        ToDbNull(price.SellPrice9);

                    row[122] =
                        ToDbNull(price.SellQuantity9);

                    row[123] =
                        ToDbNull((long)price.SellPrice9_NOO);

                    row[124] =
                        ToDbNull(price.SellPrice9_MDEY);

                    row[125] =
                        ToDbNull(price.SellPrice9_MDEMMS);

                    row[126] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 10
                    //
                    // 127 -> 138
                    // ====================================================

                    row[127] =
                        ToDbNull(price.BuyPrice10);

                    row[128] =
                        ToDbNull(price.BuyQuantity10);

                    row[129] =
                        ToDbNull((long)price.BuyPrice10_NOO);

                    row[130] =
                        ToDbNull(price.BuyPrice10_MDEY);

                    row[131] =
                        ToDbNull(price.BuyPrice10_MDEMMS);

                    row[132] =
                        DBNull.Value;

                    row[133] =
                        ToDbNull(price.SellPrice10);

                    row[134] =
                        ToDbNull(price.SellQuantity10);

                    row[135] =
                        ToDbNull((long)price.SellPrice10_NOO);

                    row[136] =
                        ToDbNull(price.SellPrice10_MDEY);

                    row[137] =
                        ToDbNull(price.SellPrice10_MDEMMS);

                    row[138] =
                        DBNull.Value;


                    // ====================================================
                    // LAST PRICE
                    //
                    // 139 -> 143
                    // ====================================================

                    row[139] =
                        ToDbNull(price.MatchPrice);

                    row[140] =
                        ToDbNull(price.MatchQuantity);

                    row[141] =
                        ToDbNull(price.OpenPrice);

                    row[142] =
                        ToDbNull(price.LowestPrice);

                    row[143] =
                        ToDbNull(price.HighestPrice);


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            price.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[144] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[144] =
                            DBNull.Value;
                    }

                    /*
                     * Một lần DateTime.Now cho toàn batch.
                     */
                    row[145] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                return dt;
            }
            catch
            {
                /*
                 * Nếu có lỗi khi build DataTable:
                 * dispose ngay phần đã tạo để release resource sớm.
                 */
                dt.Dispose();

                /*
                 * Không dùng:
                 *
                 * throw ex;
                 *
                 * vì throw ex làm mất stack trace gốc.
                 */
                throw;
            }
            finally
            {
                /*
                 * EndLoadData phải được gọi sau BeginLoadData.
                 *
                 * Tuy nhiên nếu EndLoadData itself throw, ta không muốn
                 * che mất exception gốc trong quá trình build row.
                 */
                try
                {
                    dt.EndLoadData();
                }
                catch
                {
                    /*
                     * Không throw ở finally.
                     *
                     * Bulk insert phía sau vẫn sẽ fail nếu DataTable
                     * thực sự invalid.
                     */
                }
            }
        }

        private DataTable CreatePriceRecoveryDataTable(List<EPriceRecovery> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                /*
                 * Biết trước số row để DataTable hạn chế
                 * việc resize internal storage.
                 */
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // --------------------------------------------------------
                // HEADER
                //
                // Ordinal 0 -> 8
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // --------------------------------------------------------
                // COMMON PRICE DATA
                //
                // Ordinal 9 -> 10
                // --------------------------------------------------------

                dt.Columns.Add(
                    BasePriceSchema.TradingSessionId,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    BasePriceSchema.Symbol,
                    typeof(string));                         // 10


                // --------------------------------------------------------
                // MSG_W PRICE FIELDS
                //
                // Ordinal 11 -> 18
                // --------------------------------------------------------

                dt.Columns.Add(
                    MsgWSchema.OpnPx,
                    typeof(decimal));                        // 11

                dt.Columns.Add(
                    MsgWSchema.TrdSessnHighPx,
                    typeof(decimal));                        // 12

                dt.Columns.Add(
                    MsgWSchema.TrdSessnLowPx,
                    typeof(decimal));                        // 13

                dt.Columns.Add(
                    MsgWSchema.SymbolCloseInfoPx,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgWSchema.OpnPxYld,
                    typeof(decimal));                        // 15

                dt.Columns.Add(
                    MsgWSchema.TrdSessnHighPxYld,
                    typeof(decimal));                        // 16

                dt.Columns.Add(
                    MsgWSchema.TrdSessnLowPxYld,
                    typeof(decimal));                        // 17

                dt.Columns.Add(
                    MsgWSchema.ClsPxYld,
                    typeof(decimal));                        // 18


                // --------------------------------------------------------
                // STATISTICS
                //
                // Ordinal 19 -> 24
                // --------------------------------------------------------

                dt.Columns.Add(
                    BasePriceSchema.TotalVolumeTraded,
                    typeof(long));                           // 19

                dt.Columns.Add(
                    BasePriceSchema.GrossTradeAmt,
                    typeof(decimal));                        // 20

                dt.Columns.Add(
                    BasePriceSchema.SellTotOrderQty,
                    typeof(long));                           // 21

                dt.Columns.Add(
                    BasePriceSchema.BuyTotOrderQty,
                    typeof(long));                           // 22

                dt.Columns.Add(
                    BasePriceSchema.SellValidOrderCnt,
                    typeof(long));                           // 23

                dt.Columns.Add(
                    BasePriceSchema.BuyValidOrderCnt,
                    typeof(long));                           // 24


                // --------------------------------------------------------
                // DEPTH LEVEL 1 -> 10
                //
                // Mỗi level có 12 columns:
                //
                // Buy:
                // BP, BQ, NOO, MDEY, MDEMMS, MDEPNO
                //
                // Sell:
                // SP, SQ, NOO, MDEY, MDEMMS, MDEPNO
                //
                // Level 1 : 25  -> 36
                // Level 2 : 37  -> 48
                // Level 3 : 49  -> 60
                // Level 4 : 61  -> 72
                // Level 5 : 73  -> 84
                // Level 6 : 85  -> 96
                // Level 7 : 97  -> 108
                // Level 8 : 109 -> 120
                // Level 9 : 121 -> 132
                // Level 10: 133 -> 144
                // --------------------------------------------------------

                for (int i = 1; i <= 10; i++)
                {
                    // BUY
                    dt.Columns.Add(
                        $"{BasePriceSchema.BpPrefix}{i}",
                        typeof(decimal));

                    dt.Columns.Add(
                        $"{BasePriceSchema.BqPrefix}{i}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Noo}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdey}",
                        typeof(decimal));

                    dt.Columns.Add(
                        $"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdemms}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdepno}",
                        typeof(int));


                    // SELL
                    dt.Columns.Add(
                        $"{BasePriceSchema.SpPrefix}{i}",
                        typeof(decimal));

                    dt.Columns.Add(
                        $"{BasePriceSchema.SqPrefix}{i}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Noo}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdey}",
                        typeof(decimal));

                    dt.Columns.Add(
                        $"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdemms}",
                        typeof(long));

                    dt.Columns.Add(
                        $"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdepno}",
                        typeof(int));
                }


                // --------------------------------------------------------
                // FOOTER
                //
                // Ordinal 145 -> 146
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 145

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 146


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                /*
                 * Chỉ gọi DateTime.Now 1 lần cho toàn batch.
                 */
                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var priceRecovery in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        priceRecovery.BeginString != null
                            ? priceRecovery.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)priceRecovery.BodyLength;

                    row[2] =
                        priceRecovery.MsgType != null
                            ? priceRecovery.MsgType
                            : DBNull.Value;

                    row[3] =
                        priceRecovery.SenderCompID != null
                            ? priceRecovery.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        priceRecovery.TargetCompID != null
                            ? priceRecovery.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        priceRecovery.MsgSeqNum;

                    row[6] =
                        ParseCompactDateTimeToDbNull(
                            priceRecovery.SendingTime);

                    row[7] =
                        priceRecovery.MarketID != null
                            ? priceRecovery.MarketID
                            : DBNull.Value;

                    row[8] =
                        priceRecovery.BoardID != null
                            ? priceRecovery.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // COMMON PRICE DATA
                    // ====================================================

                    row[9] =
                        priceRecovery.TradingSessionID != null
                            ? priceRecovery.TradingSessionID
                            : DBNull.Value;

                    row[10] =
                        priceRecovery.Symbol != null
                            ? priceRecovery.Symbol
                            : DBNull.Value;


                    // ====================================================
                    // MSG_W PRICE FIELDS
                    // ====================================================

                    row[11] =
                        ToDbNull(priceRecovery.OpnPx);

                    row[12] =
                        ToDbNull(priceRecovery.TrdSessnHighPx);

                    row[13] =
                        ToDbNull(priceRecovery.TrdSessnLowPx);

                    row[14] =
                        ToDbNull(priceRecovery.SymbolCloseInfoPx);

                    row[15] =
                        ToDbNull(priceRecovery.OpnPxYld);

                    row[16] =
                        ToDbNull(priceRecovery.TrdSessnHighPxYld);

                    row[17] =
                        ToDbNull(priceRecovery.TrdSessnLowPxYld);

                    row[18] =
                        ToDbNull(priceRecovery.ClsPxYld);


                    // ====================================================
                    // STATISTICS
                    // ====================================================

                    row[19] =
                        ToDbNull(priceRecovery.TotalVolumeTraded);

                    row[20] =
                        ToDbNull(priceRecovery.GrossTradeAmt);

                    row[21] =
                        ToDbNull(priceRecovery.SellTotOrderQty);

                    row[22] =
                        ToDbNull(priceRecovery.BuyTotOrderQty);

                    row[23] =
                        ToDbNull(priceRecovery.SellValidOrderCnt);

                    row[24] =
                        ToDbNull(priceRecovery.BuyValidOrderCnt);


                    // ====================================================
                    // LEVEL 1
                    // 25 -> 36
                    // ====================================================

                    row[25] =
                        ToDbNull(priceRecovery.BuyPrice1);

                    row[26] =
                        ToDbNull(priceRecovery.BuyQuantity1);

                    row[27] =
                        ToDbNull((long)priceRecovery.BuyPrice1_NOO);

                    row[28] =
                        ToDbNull(priceRecovery.BuyPrice1_MDEY);

                    row[29] =
                        ToDbNull(priceRecovery.BuyPrice1_MDEMMS);

                    row[30] =
                        DBNull.Value;

                    row[31] =
                        ToDbNull(priceRecovery.SellPrice1);

                    row[32] =
                        ToDbNull(priceRecovery.SellQuantity1);

                    row[33] =
                        ToDbNull((long)priceRecovery.SellPrice1_NOO);

                    row[34] =
                        ToDbNull(priceRecovery.SellPrice1_MDEY);

                    row[35] =
                        ToDbNull(priceRecovery.SellPrice1_MDEMMS);

                    row[36] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 2
                    // 37 -> 48
                    // ====================================================

                    row[37] =
                        ToDbNull(priceRecovery.BuyPrice2);

                    row[38] =
                        ToDbNull(priceRecovery.BuyQuantity2);

                    row[39] =
                        ToDbNull((long)priceRecovery.BuyPrice2_NOO);

                    row[40] =
                        ToDbNull(priceRecovery.BuyPrice2_MDEY);

                    row[41] =
                        ToDbNull(priceRecovery.BuyPrice2_MDEMMS);

                    row[42] =
                        DBNull.Value;

                    row[43] =
                        ToDbNull(priceRecovery.SellPrice2);

                    row[44] =
                        ToDbNull(priceRecovery.SellQuantity2);

                    row[45] =
                        ToDbNull((long)priceRecovery.SellPrice2_NOO);

                    row[46] =
                        ToDbNull(priceRecovery.SellPrice2_MDEY);

                    row[47] =
                        ToDbNull(priceRecovery.SellPrice2_MDEMMS);

                    row[48] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 3
                    // 49 -> 60
                    // ====================================================

                    row[49] =
                        ToDbNull(priceRecovery.BuyPrice3);

                    row[50] =
                        ToDbNull(priceRecovery.BuyQuantity3);

                    row[51] =
                        ToDbNull((long)priceRecovery.BuyPrice3_NOO);

                    row[52] =
                        ToDbNull(priceRecovery.BuyPrice3_MDEY);

                    row[53] =
                        ToDbNull(priceRecovery.BuyPrice3_MDEMMS);

                    row[54] =
                        DBNull.Value;

                    row[55] =
                        ToDbNull(priceRecovery.SellPrice3);

                    row[56] =
                        ToDbNull(priceRecovery.SellQuantity3);

                    row[57] =
                        ToDbNull((long)priceRecovery.SellPrice3_NOO);

                    row[58] =
                        ToDbNull(priceRecovery.SellPrice3_MDEY);

                    row[59] =
                        ToDbNull(priceRecovery.SellPrice3_MDEMMS);

                    row[60] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 4
                    // 61 -> 72
                    // ====================================================

                    row[61] =
                        ToDbNull(priceRecovery.BuyPrice4);

                    row[62] =
                        ToDbNull(priceRecovery.BuyQuantity4);

                    row[63] =
                        ToDbNull((long)priceRecovery.BuyPrice4_NOO);

                    row[64] =
                        ToDbNull(priceRecovery.BuyPrice4_MDEY);

                    row[65] =
                        ToDbNull(priceRecovery.BuyPrice4_MDEMMS);

                    row[66] =
                        DBNull.Value;

                    row[67] =
                        ToDbNull(priceRecovery.SellPrice4);

                    row[68] =
                        ToDbNull(priceRecovery.SellQuantity4);

                    row[69] =
                        ToDbNull((long)priceRecovery.SellPrice4_NOO);

                    row[70] =
                        ToDbNull(priceRecovery.SellPrice4_MDEY);

                    row[71] =
                        ToDbNull(priceRecovery.SellPrice4_MDEMMS);

                    row[72] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 5
                    // 73 -> 84
                    // ====================================================

                    row[73] =
                        ToDbNull(priceRecovery.BuyPrice5);

                    row[74] =
                        ToDbNull(priceRecovery.BuyQuantity5);

                    row[75] =
                        ToDbNull((long)priceRecovery.BuyPrice5_NOO);

                    row[76] =
                        ToDbNull(priceRecovery.BuyPrice5_MDEY);

                    row[77] =
                        ToDbNull(priceRecovery.BuyPrice5_MDEMMS);

                    row[78] =
                        DBNull.Value;

                    row[79] =
                        ToDbNull(priceRecovery.SellPrice5);

                    row[80] =
                        ToDbNull(priceRecovery.SellQuantity5);

                    row[81] =
                        ToDbNull((long)priceRecovery.SellPrice5_NOO);

                    row[82] =
                        ToDbNull(priceRecovery.SellPrice5_MDEY);

                    row[83] =
                        ToDbNull(priceRecovery.SellPrice5_MDEMMS);

                    row[84] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 6
                    // 85 -> 96
                    // ====================================================

                    row[85] =
                        ToDbNull(priceRecovery.BuyPrice6);

                    row[86] =
                        ToDbNull(priceRecovery.BuyQuantity6);

                    row[87] =
                        ToDbNull((long)priceRecovery.BuyPrice6_NOO);

                    row[88] =
                        ToDbNull(priceRecovery.BuyPrice6_MDEY);

                    row[89] =
                        ToDbNull(priceRecovery.BuyPrice6_MDEMMS);

                    row[90] =
                        DBNull.Value;

                    row[91] =
                        ToDbNull(priceRecovery.SellPrice6);

                    row[92] =
                        ToDbNull(priceRecovery.SellQuantity6);

                    row[93] =
                        ToDbNull((long)priceRecovery.SellPrice6_NOO);

                    row[94] =
                        ToDbNull(priceRecovery.SellPrice6_MDEY);

                    row[95] =
                        ToDbNull(priceRecovery.SellPrice6_MDEMMS);

                    row[96] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 7
                    // 97 -> 108
                    // ====================================================

                    row[97] =
                        ToDbNull(priceRecovery.BuyPrice7);

                    row[98] =
                        ToDbNull(priceRecovery.BuyQuantity7);

                    row[99] =
                        ToDbNull((long)priceRecovery.BuyPrice7_NOO);

                    row[100] =
                        ToDbNull(priceRecovery.BuyPrice7_MDEY);

                    row[101] =
                        ToDbNull(priceRecovery.BuyPrice7_MDEMMS);

                    row[102] =
                        DBNull.Value;

                    row[103] =
                        ToDbNull(priceRecovery.SellPrice7);

                    row[104] =
                        ToDbNull(priceRecovery.SellQuantity7);

                    row[105] =
                        ToDbNull((long)priceRecovery.SellPrice7_NOO);

                    row[106] =
                        ToDbNull(priceRecovery.SellPrice7_MDEY);

                    row[107] =
                        ToDbNull(priceRecovery.SellPrice7_MDEMMS);

                    row[108] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 8
                    // 109 -> 120
                    // ====================================================

                    row[109] =
                        ToDbNull(priceRecovery.BuyPrice8);

                    row[110] =
                        ToDbNull(priceRecovery.BuyQuantity8);

                    row[111] =
                        ToDbNull((long)priceRecovery.BuyPrice8_NOO);

                    row[112] =
                        ToDbNull(priceRecovery.BuyPrice8_MDEY);

                    row[113] =
                        ToDbNull(priceRecovery.BuyPrice8_MDEMMS);

                    row[114] =
                        DBNull.Value;

                    row[115] =
                        ToDbNull(priceRecovery.SellPrice8);

                    row[116] =
                        ToDbNull(priceRecovery.SellQuantity8);

                    row[117] =
                        ToDbNull((long)priceRecovery.SellPrice8_NOO);

                    row[118] =
                        ToDbNull(priceRecovery.SellPrice8_MDEY);

                    row[119] =
                        ToDbNull(priceRecovery.SellPrice8_MDEMMS);

                    row[120] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 9
                    // 121 -> 132
                    // ====================================================

                    row[121] =
                        ToDbNull(priceRecovery.BuyPrice9);

                    row[122] =
                        ToDbNull(priceRecovery.BuyQuantity9);

                    row[123] =
                        ToDbNull((long)priceRecovery.BuyPrice9_NOO);

                    row[124] =
                        ToDbNull(priceRecovery.BuyPrice9_MDEY);

                    row[125] =
                        ToDbNull(priceRecovery.BuyPrice9_MDEMMS);

                    row[126] =
                        DBNull.Value;

                    row[127] =
                        ToDbNull(priceRecovery.SellPrice9);

                    row[128] =
                        ToDbNull(priceRecovery.SellQuantity9);

                    row[129] =
                        ToDbNull((long)priceRecovery.SellPrice9_NOO);

                    row[130] =
                        ToDbNull(priceRecovery.SellPrice9_MDEY);

                    row[131] =
                        ToDbNull(priceRecovery.SellPrice9_MDEMMS);

                    row[132] =
                        DBNull.Value;


                    // ====================================================
                    // LEVEL 10
                    // 133 -> 144
                    // ====================================================

                    row[133] =
                        ToDbNull(priceRecovery.BuyPrice10);

                    row[134] =
                        ToDbNull(priceRecovery.BuyQuantity10);

                    row[135] =
                        ToDbNull((long)priceRecovery.BuyPrice10_NOO);

                    row[136] =
                        ToDbNull(priceRecovery.BuyPrice10_MDEY);

                    row[137] =
                        ToDbNull(priceRecovery.BuyPrice10_MDEMMS);

                    row[138] =
                        DBNull.Value;

                    row[139] =
                        ToDbNull(priceRecovery.SellPrice10);

                    row[140] =
                        ToDbNull(priceRecovery.SellQuantity10);

                    row[141] =
                        ToDbNull((long)priceRecovery.SellPrice10_NOO);

                    row[142] =
                        ToDbNull(priceRecovery.SellPrice10_MDEY);

                    row[143] =
                        ToDbNull(priceRecovery.SellPrice10_MDEMMS);

                    row[144] =
                        DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            priceRecovery.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[145] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[145] =
                            DBNull.Value;
                    }

                    row[146] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                /*
                 * Nếu lỗi trong lúc build DataTable,
                 * giải phóng resource đã tạo.
                 */
                dt.Dispose();

                /*
                 * Giữ nguyên stack trace gốc.
                 */
                throw;
            }
        }
        private DataTable CreateSecurityStatusDataTable(List<ESecurityStatus> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 15
                dt.Columns.Add(
                    MsgFSchema.TscProdGrpId,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgFSchema.BoardEvtId,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgFSchema.SessOpenCloseCode,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgFSchema.Symbol,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgFSchema.TradingSessionId,
                    typeof(string));                         // 13

                dt.Columns.Add(
                    MsgFSchema.HaltRsnCode,
                    typeof(long));                           // 14

                dt.Columns.Add(
                    MsgFSchema.ProductId,
                    typeof(string));                         // 15


                // FOOTER
                // 16 -> 17
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 16

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 17


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var status in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        status.BeginString != null
                            ? status.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)status.BodyLength;

                    row[2] =
                        status.MsgType != null
                            ? status.MsgType
                            : DBNull.Value;

                    row[3] =
                        status.SenderCompID != null
                            ? status.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        status.TargetCompID != null
                            ? status.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        ToDbNull(status.MsgSeqNum);

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            status.SendingTime);

                    row[7] =
                        status.MarketID != null
                            ? status.MarketID
                            : DBNull.Value;

                    row[8] =
                        status.BoardID != null
                            ? status.BoardID
                            : DBNull.Value;


                    // PAYLOAD
                    row[9] =
                        status.TscProdGrpId != null
                            ? status.TscProdGrpId
                            : DBNull.Value;

                    row[10] =
                        status.BoardEvtID != null
                            ? status.BoardEvtID
                            : DBNull.Value;

                    row[11] =
                        status.SessOpenCloseCode != null
                            ? status.SessOpenCloseCode
                            : DBNull.Value;

                    row[12] =
                        status.Symbol != null
                            ? status.Symbol
                            : DBNull.Value;

                    row[13] =
                        status.TradingSessionID != null
                            ? status.TradingSessionID
                            : DBNull.Value;


                    // HALT REASON CODE
                    if (long.TryParse(
                            status.HaltRsnCode,
                            out var parsedHaltRsnCode))
                    {
                        row[14] =
                            parsedHaltRsnCode;
                    }
                    else
                    {
                        row[14] =
                            DBNull.Value;
                    }

                    row[15] =
                        status.ProductID != null
                            ? status.ProductID
                            : DBNull.Value;


                    // FOOTER
                    if (long.TryParse(
                            status.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[16] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[16] =
                            DBNull.Value;
                    }

                    row[17] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateIndexDataTable(List<EIndex> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // --------------------------------------------------------
                // HEADER
                // Ordinal 0 -> 7
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // --------------------------------------------------------
                // PAYLOAD
                // Ordinal 8 -> 29
                // --------------------------------------------------------

                dt.Columns.Add(
                    MsgM1Schema.TradingSessionId,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgM1Schema.MarketIndexClass,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgM1Schema.IndexsTypeCode,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgM1Schema.Currency,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgM1Schema.TransactTime,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgM1Schema.TransDate,
                    typeof(DateTime));                       // 13

                dt.Columns.Add(
                    MsgM1Schema.ValueIndexes,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgM1Schema.TotalVolumeTraded,
                    typeof(long));                           // 15

                dt.Columns.Add(
                    MsgM1Schema.GrossTradeAmt,
                    typeof(decimal));                        // 16

                dt.Columns.Add(
                    MsgM1Schema.ContauctAccTrdvol,
                    typeof(long));                           // 17

                dt.Columns.Add(
                    MsgM1Schema.ContauctAccTrdval,
                    typeof(decimal));                        // 18

                dt.Columns.Add(
                    MsgM1Schema.BlktrdAccTrdvol,
                    typeof(long));                           // 19

                dt.Columns.Add(
                    MsgM1Schema.BlktrdAccTrdval,
                    typeof(decimal));                        // 20

                dt.Columns.Add(
                    MsgM1Schema.FluctuationUpperLimitIc,
                    typeof(int));                            // 21

                dt.Columns.Add(
                    MsgM1Schema.FluctuationUpIc,
                    typeof(int));                            // 22

                dt.Columns.Add(
                    MsgM1Schema.FluctuationSteadinessIc,
                    typeof(int));                            // 23

                dt.Columns.Add(
                    MsgM1Schema.FluctuationDownIc,
                    typeof(int));                            // 24

                dt.Columns.Add(
                    MsgM1Schema.FluctuationLowerLimitIc,
                    typeof(int));                            // 25

                dt.Columns.Add(
                    MsgM1Schema.FluctuationUpIv,
                    typeof(long));                           // 26

                dt.Columns.Add(
                    MsgM1Schema.FluctuationDownIv,
                    typeof(long));                           // 27

                dt.Columns.Add(
                    MsgM1Schema.FluctuationSteadinessIv,
                    typeof(long));                           // 28


                // --------------------------------------------------------
                // FOOTER
                // Ordinal 29 -> 30
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 29

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 30


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                /*
                 * Một timestamp duy nhất cho cả batch.
                 */
                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var index in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        index.BeginString != null
                            ? index.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)index.BodyLength;

                    row[2] =
                        index.MsgType != null
                            ? index.MsgType
                            : DBNull.Value;

                    row[3] =
                        index.SenderCompID != null
                            ? index.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        index.TargetCompID != null
                            ? index.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        index.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            index.SendingTime);

                    row[7] =
                        index.MarketID != null
                            ? index.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        index.TradingSessionID != null
                            ? index.TradingSessionID
                            : DBNull.Value;

                    row[9] =
                        index.MarketIndexClass != null
                            ? index.MarketIndexClass
                            : DBNull.Value;

                    row[10] =
                        index.IndexsTypeCode != null
                            ? index.IndexsTypeCode
                            : DBNull.Value;

                    row[11] =
                        index.Currency != null
                            ? index.Currency
                            : DBNull.Value;

                    row[12] =
                        index.TransactTime != null
                            ? index.TransactTime
                            : DBNull.Value;

                    row[13] =
                        ParseDayMonYearToDbNull(
                            index.TransDate);


                    // ====================================================
                    // NUMERIC DATA
                    //
                    // Giữ nguyên logic ToDbNull từ code cũ
                    // để không thay đổi behavior dữ liệu.
                    // ====================================================

                    row[14] =
                        ToDbNull(index.ValueIndexes);

                    row[15] =
                        ToDbNull(index.TotalVolumeTraded);

                    row[16] =
                        ToDbNull(index.GrossTradeAmt);

                    row[17] =
                        ToDbNull(index.ContauctAccTrdvol);

                    row[18] =
                        ToDbNull(index.ContauctAccTrdval);

                    row[19] =
                        ToDbNull(index.BlktrdAccTrdvol);

                    row[20] =
                        ToDbNull(index.BlktrdAccTrdval);


                    // ====================================================
                    // FLUCTUATION ISSUE COUNT
                    // ====================================================

                    row[21] =
                        ToDbNull(
                            index.FluctuationUpperLimitIssueCount);

                    row[22] =
                        ToDbNull(
                            index.FluctuationUpIssueCount);

                    row[23] =
                        ToDbNull(
                            index.FluctuationSteadinessIssueCount);

                    row[24] =
                        ToDbNull(
                            index.FluctuationDownIssueCount);

                    row[25] =
                        ToDbNull(
                            index.FluctuationLowerLimitIssueCount);


                    // ====================================================
                    // FLUCTUATION ISSUE VOLUME
                    // ====================================================

                    row[26] =
                        ToDbNull(
                            index.FluctuationUpIssueVolume);

                    row[27] =
                        ToDbNull(
                            index.FluctuationDownIssueVolume);

                    row[28] =
                        ToDbNull(
                            index.FluctuationSteadinessIssueVolume);


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            index.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[29] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[29] =
                            DBNull.Value;
                    }

                    row[30] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateInvestorPerIndustryDataTable(List<EInvestorPerIndustry> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                /*
                 * Giúp DataTable dự trù số row,
                 * giảm số lần mở rộng internal storage.
                 */
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // --------------------------------------------------------
                // HEADER
                // Ordinal 0 -> 7
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // --------------------------------------------------------
                // PAYLOAD
                // Ordinal 8 -> 18
                // --------------------------------------------------------

                dt.Columns.Add(
                    MsgM2Schema.TransactTime,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgM2Schema.MarketIndexClass,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgM2Schema.IndexsTypeCode,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgM2Schema.Currency,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgM2Schema.InvestCode,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgM2Schema.SellVolume,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    MsgM2Schema.SellTradeAmount,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgM2Schema.BuyVolume,
                    typeof(long));                           // 15

                dt.Columns.Add(
                    MsgM2Schema.BuyTradedAmount,
                    typeof(decimal));                        // 16

                dt.Columns.Add(
                    MsgM2Schema.BondClassificationCode,
                    typeof(string));                         // 17

                dt.Columns.Add(
                    MsgM2Schema.SecurityGroupId,
                    typeof(string));                         // 18


                // --------------------------------------------------------
                // FOOTER
                // Ordinal 19 -> 20
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 19

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 20


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                /*
                 * Một timestamp duy nhất cho toàn batch.
                 */
                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.TransactTime != null
                            ? msg.TransactTime
                            : DBNull.Value;

                    row[9] =
                        msg.MarketIndexClass != null
                            ? msg.MarketIndexClass
                            : DBNull.Value;

                    row[10] =
                        msg.IndexsTypeCode != null
                            ? msg.IndexsTypeCode
                            : DBNull.Value;

                    row[11] =
                        msg.Currency != null
                            ? msg.Currency
                            : DBNull.Value;

                    row[12] =
                        msg.InvestCode != null
                            ? msg.InvestCode
                            : DBNull.Value;


                    // ====================================================
                    // NUMERIC
                    //
                    // Giữ nguyên behavior code cũ:
                    // gán trực tiếp, không dùng HasValue.
                    // ====================================================

                    row[13] =
                        msg.SellVolume;

                    row[14] =
                        msg.SellTradeAmount;

                    row[15] =
                        msg.BuyVolume;

                    row[16] =
                        msg.BuyTradedAmount;


                    // ====================================================
                    // CLASSIFICATION
                    // ====================================================

                    row[17] =
                        msg.BondClassificationCode != null
                            ? msg.BondClassificationCode
                            : DBNull.Value;

                    row[18] =
                        msg.SecurityGroupID != null
                            ? msg.SecurityGroupID
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[19] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[19] =
                            DBNull.Value;
                    }

                    row[20] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                /*
                 * Nếu lỗi khi build DataTable,
                 * giải phóng resource ngay.
                 */
                dt.Dispose();

                /*
                 * Giữ nguyên stack trace gốc.
                 */
                throw;
            }
        }
        private DataTable CreateInvestorPerSymbolDataTable(List<EInvestorPerSymbol> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                /*
                 * Giúp DataTable dự trù số row,
                 * giảm việc resize internal storage.
                 */
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // --------------------------------------------------------
                // HEADER
                // Ordinal 0 -> 7
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // --------------------------------------------------------
                // PAYLOAD
                // Ordinal 8 -> 13
                // --------------------------------------------------------

                dt.Columns.Add(
                    MsgM3Schema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgM3Schema.InvestCode,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgM3Schema.SellVolume,
                    typeof(long));                           // 10

                dt.Columns.Add(
                    MsgM3Schema.SellTradeAmount,
                    typeof(decimal));                        // 11

                dt.Columns.Add(
                    MsgM3Schema.BuyVolume,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    MsgM3Schema.BuyTradedAmount,
                    typeof(decimal));                        // 13


                // --------------------------------------------------------
                // FOOTER
                // Ordinal 14 -> 15
                // --------------------------------------------------------

                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 14

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 15


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                /*
                 * Một timestamp cho toàn batch.
                 */
                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        msg.InvestCode != null
                            ? msg.InvestCode
                            : DBNull.Value;


                    /*
                     * Giữ nguyên behavior code cũ:
                     * các numeric field được gán trực tiếp,
                     * không dùng HasValue.
                     */
                    row[10] =
                        msg.SellVolume;

                    row[11] =
                        msg.SellTradeAmount;

                    row[12] =
                        msg.BuyVolume;

                    row[13] =
                        msg.BuyTradedAmount;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[14] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[14] =
                            DBNull.Value;
                    }

                    row[15] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                /*
                 * Nếu lỗi trong lúc build DataTable,
                 * dispose ngay phần đã tạo.
                 */
                dt.Dispose();

                throw;
            }
        }
        private DataTable CreateTopNMembersPerSymbolDataTable(List<ETopNMembersPerSymbol> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 17
                dt.Columns.Add(
                    MsgM4Schema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgM4Schema.TotNumReports,
                    typeof(long));                           // 9

                dt.Columns.Add(
                    MsgM4Schema.SellRankSeq,
                    typeof(int));                            // 10

                dt.Columns.Add(
                    MsgM4Schema.SellMemberNo,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgM4Schema.SellVolume,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    MsgM4Schema.SellTradeAmount,
                    typeof(decimal));                        // 13

                dt.Columns.Add(
                    MsgM4Schema.BuyRankSeq,
                    typeof(int));                            // 14

                dt.Columns.Add(
                    MsgM4Schema.BuyMemberNo,
                    typeof(string));                         // 15

                dt.Columns.Add(
                    MsgM4Schema.BuyVolume,
                    typeof(long));                           // 16

                dt.Columns.Add(
                    MsgM4Schema.BuyTradedAmount,
                    typeof(decimal));                        // 17


                // FOOTER
                // 18 -> 19
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 18

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 19


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        msg.TotNumReports;

                    row[10] =
                        msg.SellRankSeq;

                    row[11] =
                        msg.SellMemberNo != null
                            ? msg.SellMemberNo
                            : DBNull.Value;

                    row[12] =
                        msg.SellVolume;

                    row[13] =
                        msg.SellTradeAmount;

                    row[14] =
                        msg.BuyRankSeq;

                    row[15] =
                        msg.BuyMemberNo != null
                            ? msg.BuyMemberNo
                            : DBNull.Value;

                    row[16] =
                        msg.BuyVolume;

                    row[17] =
                        msg.BuyTradedAmount;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[18] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[18] =
                            DBNull.Value;
                    }

                    row[19] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateSecurityInfoNotificationDataTable(List<ESecurityInformationNotification> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 17
                dt.Columns.Add(
                    MsgM7Schema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgM7Schema.ReferencePrice,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgM7Schema.HighLimitPrice,
                    typeof(decimal));                        // 11

                dt.Columns.Add(
                    MsgM7Schema.LowLimitPrice,
                    typeof(decimal));                        // 12

                dt.Columns.Add(
                    MsgM7Schema.EvaluationPrice,
                    typeof(decimal));                        // 13

                dt.Columns.Add(
                    MsgM7Schema.HgstOrderPrice,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgM7Schema.LwstOrderPrice,
                    typeof(decimal));                        // 15

                dt.Columns.Add(
                    MsgM7Schema.ListedShares,
                    typeof(long));                           // 16

                dt.Columns.Add(
                    MsgM7Schema.ExClassType,
                    typeof(string));                         // 17


                // FOOTER
                // 18 -> 19
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 18

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 19


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * numeric field gán trực tiếp.
                     */
                    row[10] =
                        msg.ReferencePrice;

                    row[11] =
                        msg.HighLimitPrice;

                    row[12] =
                        msg.LowLimitPrice;

                    row[13] =
                        msg.EvaluationPrice;

                    row[14] =
                        msg.HgstOrderPrice;

                    row[15] =
                        msg.LwstOrderPrice;

                    row[16] =
                        msg.ListedShares;

                    row[17] =
                        msg.ExClassType != null
                            ? msg.ExClassType
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[18] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[18] =
                            DBNull.Value;
                    }

                    row[19] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateSymbolClosingInfoDataTable(List<ESymbolClosingInformation> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 12
                dt.Columns.Add(
                    MsgM8Schema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgM8Schema.SymbolCloseInfoPx,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgM8Schema.SymbolCloseInfoYield,
                    typeof(decimal));                        // 11

                dt.Columns.Add(
                    MsgM8Schema.SymbolCloseInfoPxType,
                    typeof(string));                         // 12


                // FOOTER
                // 13 -> 14
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 14


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // PAYLOAD
                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * numeric field gán trực tiếp.
                     */
                    row[10] =
                        msg.SymbolCloseInfoPx;

                    row[11] =
                        msg.SymbolCloseInfoYield;

                    row[12] =
                        msg.SymbolCloseInfoPxType != null
                            ? msg.SymbolCloseInfoPxType
                            : DBNull.Value;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[13] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[13] =
                            DBNull.Value;
                    }

                    row[14] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateOpenInterestDataTable(List<EOpenInterest> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMASchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMASchema.TradeDate,
                    typeof(DateTime));                       // 9

                dt.Columns.Add(
                    MsgMASchema.OpenInterestQty,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgMASchema.SettlementPrice,
                    typeof(decimal));                        // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // PAYLOAD
                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        ParseDayMonYearToDbNull(
                            msg.TradeDate);

                    /*
                     * Giữ nguyên behavior code cũ:
                     * OpenInterestQty gán trực tiếp.
                     */
                    row[10] =
                        msg.OpenInterestQty;

                    /*
                     * Giữ nguyên logic code cũ:
                     * SettlementPrice luôn NULL.
                     */
                    row[11] =
                        DBNull.Value;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateVolatilityInterruptionDataTable(List<EVolatilityInterruption> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 16
                dt.Columns.Add(
                    MsgMDSchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMDSchema.VITypeCode,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMDSchema.VIKindCode,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMDSchema.StaticVIBasePrice,
                    typeof(decimal));                        // 12

                dt.Columns.Add(
                    MsgMDSchema.DynamicVIBasePrice,
                    typeof(decimal));                        // 13

                dt.Columns.Add(
                    MsgMDSchema.VIPrice,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgMDSchema.StaticVIDispartiyRatio,
                    typeof(decimal));                        // 15

                dt.Columns.Add(
                    MsgMDSchema.DynamicVIDispartiyRatio,
                    typeof(decimal));                        // 16


                // FOOTER
                // 17 -> 18
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 17

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 18


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        ToDbNull(msg.MsgSeqNum);

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // PAYLOAD
                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[10] =
                        msg.VITypeCode != null
                            ? msg.VITypeCode
                            : DBNull.Value;

                    row[11] =
                        msg.VIKindCode != null
                            ? msg.VIKindCode
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * numeric field gán trực tiếp.
                     */
                    row[12] =
                        msg.StaticVIBasePrice;

                    row[13] =
                        msg.DynamicVIBasePrice;

                    row[14] =
                        msg.VIPrice;

                    row[15] =
                        msg.StaticVIDispartiyRatio;

                    row[16] =
                        msg.DynamicVIDispartiyRatio;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[17] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[17] =
                            DBNull.Value;
                    }

                    row[18] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateDeemTradePriceDataTable(List<EDeemTradePrice> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 12
                dt.Columns.Add(
                    MsgMESchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMESchema.ExpectedTradePx,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgMESchema.ExpectedTradeQty,
                    typeof(long));                           // 11

                dt.Columns.Add(
                    MsgMESchema.ExpectedTradeYield,
                    typeof(decimal));                        // 12


                // FOOTER
                // 13 -> 14
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 14


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // PAYLOAD
                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * các numeric field gán trực tiếp.
                     */
                    row[10] =
                        msg.ExpectedTradePx;

                    row[11] =
                        msg.ExpectedTradeQty;

                    row[12] =
                        msg.ExpectedTradeYield;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[13] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[13] =
                            DBNull.Value;
                    }

                    row[14] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateForeignerOrderLimitDataTable(List<EForeignerOrderLimit> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 10
                dt.Columns.Add(
                    MsgMFSchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMFSchema.ForeignerBuyPosblQty,
                    typeof(long));                           // 9

                dt.Columns.Add(
                    MsgMFSchema.ForeignerOrderLimitQty,
                    typeof(long));                           // 10


                // FOOTER
                // 11 -> 12
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 11

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 12


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // PAYLOAD
                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * các numeric field gán trực tiếp.
                     */
                    row[9] =
                        msg.ForeignerBuyPosblQty;

                    row[10] =
                        msg.ForeignerOrderLimitQty;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[11] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[11] =
                            DBNull.Value;
                    }

                    row[12] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateMarketMakerInfoDataTable(List<EMarketMakerInformation> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 9
                dt.Columns.Add(
                    MsgMHSchema.MarketMakerContractCode,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMHSchema.MemberNo,
                    typeof(string));                         // 9


                // FOOTER
                // 10 -> 11
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 10

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 11


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // PAYLOAD
                    row[8] =
                        msg.MarketMakerContractCode != null
                            ? msg.MarketMakerContractCode
                            : DBNull.Value;

                    row[9] =
                        msg.MemberNo != null
                            ? msg.MemberNo
                            : DBNull.Value;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[10] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[10] =
                            DBNull.Value;
                    }

                    row[11] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateSymbolEventDataTable(List<ESymbolEvent> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 12
                dt.Columns.Add(
                    MsgMISchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMISchema.EventKindCode,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMISchema.EventOccurrenceReasonCode,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMISchema.EventStartDate,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMISchema.EventEndDate,
                    typeof(string));                         // 12


                // FOOTER
                // 13 -> 14
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 14


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        msg.EventKindCode != null
                            ? msg.EventKindCode
                            : DBNull.Value;

                    row[10] =
                        msg.EventOccurrenceReasonCode != null
                            ? msg.EventOccurrenceReasonCode
                            : DBNull.Value;

                    row[11] =
                        msg.EventStartDate != null
                            ? msg.EventStartDate
                            : DBNull.Value;

                    row[12] =
                        msg.EventEndDate != null
                            ? msg.EventEndDate
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[13] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[13] =
                            DBNull.Value;
                    }

                    row[14] =
                        batchCreateTime;


                    // ====================================================
                    // ADD ROW
                    // ====================================================

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateDrvProductEventDataTable(List<EDrvProductEvent> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 6
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6


                // PAYLOAD
                // 7 -> 11
                dt.Columns.Add(
                    MsgMJSchema.ProductId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    MsgMJSchema.EventKindCode,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMJSchema.EventOccurrenceReasonCode,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMJSchema.EventStartDate,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMJSchema.EventEndDate,
                    typeof(string));                         // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);


                    // PAYLOAD
                    row[7] =
                        msg.ProductID != null
                            ? msg.ProductID
                            : DBNull.Value;

                    row[8] =
                        msg.EventKindCode != null
                            ? msg.EventKindCode
                            : DBNull.Value;

                    row[9] =
                        msg.EventOccurrenceReasonCode != null
                            ? msg.EventOccurrenceReasonCode
                            : DBNull.Value;

                    row[10] =
                        msg.EventStartDate != null
                            ? msg.EventStartDate
                            : DBNull.Value;

                    row[11] =
                        msg.EventEndDate != null
                            ? msg.EventEndDate
                            : DBNull.Value;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateIndexConstituentsDataTable(List<EIndexConstituentsInformation> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 15
                dt.Columns.Add(
                    MsgMLSchema.MarketIndexClass,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMLSchema.IndexsTypeCode,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMLSchema.Currency,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMLSchema.IdxName,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMLSchema.IdxEnglishName,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgMLSchema.TotalMsgNo,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    MsgMLSchema.CurrentMsgNo,
                    typeof(long));                           // 14

                dt.Columns.Add(
                    MsgMLSchema.Symbol,
                    typeof(string));                         // 15


                // FOOTER
                // 16 -> 17
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 16

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 17


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.MarketIndexClass != null
                            ? msg.MarketIndexClass
                            : DBNull.Value;

                    row[9] =
                        msg.IndexsTypeCode != null
                            ? msg.IndexsTypeCode
                            : DBNull.Value;

                    row[10] =
                        msg.Currency != null
                            ? msg.Currency
                            : DBNull.Value;

                    row[11] =
                        msg.IdxName != null
                            ? msg.IdxName
                            : DBNull.Value;

                    row[12] =
                        msg.IdxEnglishName != null
                            ? msg.IdxEnglishName
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * numeric field gán trực tiếp.
                     */
                    row[13] =
                        msg.TotalMsgNo;

                    row[14] =
                        msg.CurrentMsgNo;

                    row[15] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[16] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[16] =
                            DBNull.Value;
                    }

                    row[17] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateETFiNavDataTable(List<EETFiNav> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 10
                dt.Columns.Add(
                    MsgMMSchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMMSchema.TransactTime,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMMSchema.INAVValue,
                    typeof(decimal));                        // 10


                // FOOTER
                // 11 -> 12
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 11

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 12


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        msg.TransactTime != null
                            ? msg.TransactTime
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * iNAVvalue gán trực tiếp.
                     */
                    row[10] =
                        msg.iNAVvalue;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[11] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[11] =
                            DBNull.Value;
                    }

                    row[12] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateETFiIndexDataTable(List<EETFiIndex> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 10
                dt.Columns.Add(
                    MsgMNSchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMNSchema.TransactTime,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMNSchema.ValuesIndexes,
                    typeof(decimal));                        // 10


                // FOOTER
                // 11 -> 12
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 11

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 12


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * MsgSeqNum đi qua ToDbNull.
                     */
                    row[5] =
                        ToDbNull(msg.MsgSeqNum);

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        msg.TransactTime != null
                            ? msg.TransactTime
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * ValueIndexes gán trực tiếp.
                     */
                    row[10] =
                        msg.ValueIndexes;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[11] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[11] =
                            DBNull.Value;
                    }

                    row[12] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateETFTrackingErrorDataTable(List<EETFTrackingError> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMOSchema.Symbol,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMOSchema.TradeDate,
                    typeof(DateTime));                       // 9

                dt.Columns.Add(
                    MsgMOSchema.TrackingError,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgMOSchema.DisparateRatio,
                    typeof(decimal));                        // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[9] =
                        ParseDayMonYearToDbNull(
                            msg.TradeDate);

                    /*
                     * Giữ nguyên behavior code cũ:
                     * numeric field gán trực tiếp.
                     */
                    row[10] =
                        msg.TrackingError;

                    row[11] =
                        msg.DisparateRatio;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateTopNSymbolsWithTradingQuantityDataTable(List<ETopNSymbolsWithTradingQuantity> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMPSchema.TotNumReports,
                    typeof(long));                           // 8

                dt.Columns.Add(
                    MsgMPSchema.Rank,
                    typeof(int));                            // 9

                dt.Columns.Add(
                    MsgMPSchema.Symbol,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMPSchema.MDEntrySize,
                    typeof(long));                           // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.TotNumReports;

                    row[9] =
                        msg.Rank;

                    row[10] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[11] =
                        msg.MDEntrySize;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateTopNSymbolsWithCurrentPriceDataTable(List<ETopNSymbolsWithCurrentPrice> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMQSchema.TotNumReports,
                    typeof(long));                           // 8

                dt.Columns.Add(
                    MsgMQSchema.Rank,
                    typeof(int));                            // 9

                dt.Columns.Add(
                    MsgMQSchema.Symbol,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMQSchema.MDEntryPx,
                    typeof(decimal));                        // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.TotNumReports;

                    row[9] =
                        msg.Rank;

                    row[10] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * MDEntryPx gán trực tiếp.
                     */
                    row[11] =
                        msg.MDEntryPx;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateTopNSymbolsWithHighRatioOfPriceDataTable(List<ETopNSymbolsWithHighRatioOfPrice> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMRSchema.TotNumReports,
                    typeof(long));                           // 8

                dt.Columns.Add(
                    MsgMRSchema.Rank,
                    typeof(int));                            // 9

                dt.Columns.Add(
                    MsgMRSchema.Symbol,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMRSchema.PriceFluctuationRatio,
                    typeof(decimal));                        // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // PAYLOAD
                    row[8] =
                        msg.TotNumReports;

                    row[9] =
                        msg.Rank;

                    row[10] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    /*
                     * Giữ nguyên behavior code cũ:
                     * PriceFluctuationRatio gán trực tiếp.
                     */
                    row[11] =
                        msg.PriceFluctuationRatio;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateTopNSymbolsWithLowRatioOfPriceDataTable(List<ETopNSymbolsWithLowRatioOfPrice> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 11
                dt.Columns.Add(
                    MsgMSSchema.TotNumReports,
                    typeof(long));                           // 8

                dt.Columns.Add(
                    MsgMSSchema.Rank,
                    typeof(int));                            // 9

                dt.Columns.Add(
                    MsgMSSchema.Symbol,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMSSchema.PriceFluctuationRatio,
                    typeof(decimal));                        // 11


                // FOOTER
                // 12 -> 13
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 13


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.TotNumReports;

                    row[9] =
                        msg.Rank;

                    row[10] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    // Giữ nguyên behavior code cũ
                    row[11] =
                        msg.PriceFluctuationRatio;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[12] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[12] =
                            DBNull.Value;
                    }

                    row[13] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateTradingResultOfForeignInvestorsDataTable(List<ETradingResultOfForeignInvestors> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 20
                dt.Columns.Add(
                    MsgMTSchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMTSchema.TradingSessionId,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMTSchema.TransactTime,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMTSchema.FornInvestTypeCode,
                    typeof(string));                         // 12

                dt.Columns.Add(
                    MsgMTSchema.SellVolume,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    MsgMTSchema.SellTradeAmount,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgMTSchema.BuyVolume,
                    typeof(long));                           // 15

                dt.Columns.Add(
                    MsgMTSchema.BuyTradedAmount,
                    typeof(decimal));                        // 16

                dt.Columns.Add(
                    MsgMTSchema.SellVolumeTotal,
                    typeof(long));                           // 17

                dt.Columns.Add(
                    MsgMTSchema.SellTradeAmountTotal,
                    typeof(decimal));                        // 18

                dt.Columns.Add(
                    MsgMTSchema.BuyVolumeTotal,
                    typeof(long));                           // 19

                dt.Columns.Add(
                    MsgMTSchema.BuyTradedAmountTotal,
                    typeof(decimal));                        // 20


                // FOOTER
                // 21 -> 22
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 21

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 22


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[10] =
                        msg.TradingSessionID != null
                            ? msg.TradingSessionID
                            : DBNull.Value;

                    row[11] =
                        msg.TransactTime != null
                            ? msg.TransactTime
                            : DBNull.Value;

                    row[12] =
                        msg.FornInvestTypeCode != null
                            ? msg.FornInvestTypeCode
                            : DBNull.Value;

                    row[13] =
                        msg.SellVolume;

                    row[14] =
                        msg.SellTradeAmount;

                    row[15] =
                        msg.BuyVolume;

                    row[16] =
                        msg.BuyTradedAmount;

                    row[17] =
                        msg.SellVolumeTotal;

                    row[18] =
                        msg.SellTradeAmountTotal;

                    row[19] =
                        msg.BuyVolumeTotal;

                    /*
                     * Giữ nguyên mapping code cũ:
                     * BuyTradedAmountTotal <- BuyTradeAmountTotal
                     */
                    row[20] =
                        msg.BuyTradeAmountTotal;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[21] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[21] =
                            DBNull.Value;
                    }

                    row[22] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateDisclosureDataTable(List<EDisclosure> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 7
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7


                // PAYLOAD
                // 8 -> 20
                dt.Columns.Add(
                    MsgMUSchema.SecurityExchange,
                    typeof(string));                         // 8

                dt.Columns.Add(
                    MsgMUSchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMUSchema.SymbolName,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMUSchema.DisclosureId,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMUSchema.TotalMsgNo,
                    typeof(long));                           // 12

                dt.Columns.Add(
                    MsgMUSchema.CurrentMsgNo,
                    typeof(long));                           // 13

                dt.Columns.Add(
                    MsgMUSchema.LanquageCategory,
                    typeof(string));                         // 14

                dt.Columns.Add(
                    MsgMUSchema.DataCategory,
                    typeof(string));                         // 15

                dt.Columns.Add(
                    MsgMUSchema.PublicInformationDate,
                    typeof(string));                         // 16

                dt.Columns.Add(
                    MsgMUSchema.TransmissionDate,
                    typeof(string));                         // 17

                dt.Columns.Add(
                    MsgMUSchema.ProcessType,
                    typeof(string));                         // 18

                dt.Columns.Add(
                    MsgMUSchema.Headline,
                    typeof(string));                         // 19

                dt.Columns.Add(
                    MsgMUSchema.Body,
                    typeof(string));                         // 20


                // FOOTER
                // 21 -> 22
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 21

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 22


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[8] =
                        msg.SecurityExchange != null
                            ? msg.SecurityExchange
                            : DBNull.Value;

                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[10] =
                        msg.SymbolName != null
                            ? msg.SymbolName
                            : DBNull.Value;

                    row[11] =
                        msg.DisclosureID != null
                            ? msg.DisclosureID
                            : DBNull.Value;

                    row[12] =
                        msg.TotalMsgNo;

                    row[13] =
                        msg.CurrentMsgNo;

                    row[14] =
                        msg.LanquageCategory != null
                            ? msg.LanquageCategory
                            : DBNull.Value;

                    row[15] =
                        msg.DataCategory != null
                            ? msg.DataCategory
                            : DBNull.Value;

                    row[16] =
                        msg.PublicInformationDate != null
                            ? msg.PublicInformationDate
                            : DBNull.Value;

                    row[17] =
                        msg.TransmissionDate != null
                            ? msg.TransmissionDate
                            : DBNull.Value;

                    row[18] =
                        msg.ProcessType != null
                            ? msg.ProcessType
                            : DBNull.Value;

                    row[19] =
                        msg.Headline != null
                            ? msg.Headline
                            : DBNull.Value;

                    row[20] =
                        msg.Body != null
                            ? msg.Body
                            : DBNull.Value;


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[21] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[21] =
                            DBNull.Value;
                    }

                    row[22] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreateRandomEndDataTable(List<ERandomEnd> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 19
                dt.Columns.Add(
                    MsgMWSchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMWSchema.TransactTime,
                    typeof(string));                         // 10

                dt.Columns.Add(
                    MsgMWSchema.ReApplyClassification,
                    typeof(string));                         // 11

                dt.Columns.Add(
                    MsgMWSchema.ReTentativeExecutionPrice,
                    typeof(decimal));                        // 12

                dt.Columns.Add(
                    MsgMWSchema.ReEstimatedHighestPrice,
                    typeof(decimal));                        // 13

                dt.Columns.Add(
                    MsgMWSchema.ReEHighestPriceDisparater,
                    typeof(decimal));                        // 14

                dt.Columns.Add(
                    MsgMWSchema.ReEstimatedLowestPrice,
                    typeof(decimal));                        // 15

                dt.Columns.Add(
                    MsgMWSchema.ReELowestPriceDisparater,
                    typeof(decimal));                        // 16

                dt.Columns.Add(
                    MsgMWSchema.LatestPrice,
                    typeof(decimal));                        // 17

                dt.Columns.Add(
                    MsgMWSchema.LatestPriceDisparateRatio,
                    typeof(decimal));                        // 18

                dt.Columns.Add(
                    MsgMWSchema.RandomEndReleaseTime,
                    typeof(DateTime));                       // 19


                // FOOTER
                // 20 -> 21
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 20

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 21


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // ====================================================
                    // HEADER
                    // ====================================================

                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // ====================================================
                    // PAYLOAD
                    // ====================================================

                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[10] =
                        msg.TransactTime != null
                            ? msg.TransactTime
                            : DBNull.Value;

                    row[11] =
                        msg.RandomEndApplyClassification != null
                            ? msg.RandomEndApplyClassification
                            : DBNull.Value;

                    row[12] =
                        msg.RandomEndTentativeExecutionPrice;

                    row[13] =
                        msg.RandomEndEstimatedHighestPrice;

                    row[14] =
                        msg.RandomEndEstimatedHighestPriceDisparateRatio;

                    row[15] =
                        msg.RandomEndEstimatedLowestPrice;

                    row[16] =
                        msg.RandomEndEstimatedLowestPriceDisparateRatio;

                    row[17] =
                        msg.LatestPrice;

                    row[18] =
                        msg.LatestPriceDisparateRatio;

                    if (DateTime.TryParseExact(
                            msg.RandomEndReleaseTimes,
                            "yyyyMMdd HH:mm:ss.fff",
                            CultureInfo.InvariantCulture,
                            DateTimeStyles.None,
                            out var releaseTime))
                    {
                        row[19] =
                            releaseTime;
                    }
                    else
                    {
                        row[19] =
                            DBNull.Value;
                    }


                    // ====================================================
                    // FOOTER
                    // ====================================================

                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[20] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[20] =
                            DBNull.Value;
                    }

                    row[21] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        private DataTable CreatePriceLimitExpansionDataTable(List<EPriceLimitExpansion> messages)
        {
            if (messages == null)
            {
                throw new ArgumentNullException(nameof(messages));
            }

            var dt = new DataTable
            {
                MinimumCapacity = messages.Count
            };

            try
            {
                // ========================================================
                // 1. DEFINE COLUMNS
                // ========================================================

                // HEADER
                // 0 -> 8
                dt.Columns.Add(
                    BaseMessageSchema.BeginString,
                    typeof(string));                         // 0

                dt.Columns.Add(
                    BaseMessageSchema.BodyLength,
                    typeof(int));                            // 1

                dt.Columns.Add(
                    BaseMessageSchema.MsgType,
                    typeof(string));                         // 2

                dt.Columns.Add(
                    BaseMessageSchema.SenderCompId,
                    typeof(string));                         // 3

                dt.Columns.Add(
                    BaseMessageSchema.TargetCompId,
                    typeof(string));                         // 4

                dt.Columns.Add(
                    BaseMessageSchema.MsgSeqNum,
                    typeof(long));                           // 5

                dt.Columns.Add(
                    BaseMessageSchema.SendingTime,
                    typeof(DateTime));                       // 6

                dt.Columns.Add(
                    BaseMessageSchema.MarketId,
                    typeof(string));                         // 7

                dt.Columns.Add(
                    BaseMessageSchema.BoardId,
                    typeof(string));                         // 8


                // PAYLOAD
                // 9 -> 13
                dt.Columns.Add(
                    MsgMXSchema.Symbol,
                    typeof(string));                         // 9

                dt.Columns.Add(
                    MsgMXSchema.HighLimitPrice,
                    typeof(decimal));                        // 10

                dt.Columns.Add(
                    MsgMXSchema.LowLimitPrice,
                    typeof(decimal));                        // 11

                dt.Columns.Add(
                    MsgMXSchema.PleUpLmtStep,
                    typeof(int));                            // 12

                dt.Columns.Add(
                    MsgMXSchema.PleLwLmtStep,
                    typeof(int));                            // 13


                // FOOTER
                // 14 -> 15
                dt.Columns.Add(
                    BaseMessageSchema.Checksum,
                    typeof(long));                           // 14

                dt.Columns.Add(
                    BaseMessageSchema.CreateTime,
                    typeof(DateTime));                       // 15


                // ========================================================
                // 2. LOAD DATA
                // ========================================================

                var batchCreateTime = DateTime.Now;

                dt.BeginLoadData();

                foreach (var msg in messages)
                {
                    var row = dt.NewRow();

                    // HEADER
                    row[0] =
                        msg.BeginString != null
                            ? msg.BeginString
                            : DBNull.Value;

                    row[1] =
                        (int)msg.BodyLength;

                    row[2] =
                        msg.MsgType != null
                            ? msg.MsgType
                            : DBNull.Value;

                    row[3] =
                        msg.SenderCompID != null
                            ? msg.SenderCompID
                            : DBNull.Value;

                    row[4] =
                        msg.TargetCompID != null
                            ? msg.TargetCompID
                            : DBNull.Value;

                    row[5] =
                        msg.MsgSeqNum;

                    row[6] =
                        ParseDashDateTimeToDbNull(
                            msg.SendingTime);

                    row[7] =
                        msg.MarketID != null
                            ? msg.MarketID
                            : DBNull.Value;

                    row[8] =
                        msg.BoardID != null
                            ? msg.BoardID
                            : DBNull.Value;


                    // PAYLOAD
                    row[9] =
                        msg.Symbol != null
                            ? msg.Symbol
                            : DBNull.Value;

                    row[10] =
                        msg.HighLimitPrice;

                    row[11] =
                        msg.LowLimitPrice;

                    row[12] =
                        msg.PleUpLmtStep;

                    row[13] =
                        msg.PleLwLmtStep;


                    // FOOTER
                    if (long.TryParse(
                            msg.CheckSum,
                            out var parsedCheckSum))
                    {
                        row[14] =
                            parsedCheckSum;
                    }
                    else
                    {
                        row[14] =
                            DBNull.Value;
                    }

                    row[15] =
                        batchCreateTime;

                    dt.Rows.Add(row);
                }

                dt.EndLoadData();

                return dt;
            }
            catch
            {
                dt.Dispose();
                throw;
            }
        }
        // --- CÁC HÀM HELPER TỐI ƯU ---
        /// <summary>
        /// Chuyển giá trị long "magic number" thành DBNull.Value.
        /// </summary>
        private static object ToDbNull(long? val)
        {
            return (val.HasValue && (val.Value == -9999999 /*|| val.Value == 0.0000*/)) ? DBNull.Value : (object?)val;
        }

        /// <summary>
        /// Chuyển giá trị double "magic number" thành DBNull.Value
        /// và chuyển đổi giá trị hợp lệ sang decimal.
        /// </summary>
        private static object ToDbNull(double? val)
        {
            return (val.HasValue && (val.Value == -9999999 /*|| val.Value == 0.0000*/)) ? DBNull.Value : (object?)val;
        }
        /// <summary>
        /// Chuyển đổi chuỗi thời gian dạng "yyyy-MM-dd HH:mm:ss.fff" sang DateTime hoặc DBNull.
        /// </summary>
        private object ParseDashDateTimeToDbNull(string dateTimeString)
        {
            if (DateTime.TryParseExact(dateTimeString, "yyyy-MM-dd HH:mm:ss.fff",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None,
                out DateTime parsedDate))
            {
                return parsedDate;
            }
            return DBNull.Value;
        }
        /// <summary>
        /// Chuyển đổi chuỗi thời gian dạng "yyyyMMdd HH:mm:ss.fff" sang DateTime hoặc DBNull.
        /// </summary>
        private object ParseCompactDateTimeToDbNull(string dateTimeString)
        {
            if (DateTime.TryParseExact(dateTimeString, "yyyyMMdd HH:mm:ss.fff",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None,
                out DateTime parsedDate))
            {
                return parsedDate;
            }
            return DBNull.Value;
        }
        /// <summary>
        /// Chuyển đổi chuỗi thời gian dạng "dd-MMM-yyyy" (ví dụ: "12-NOV-2025") sang DateTime hoặc DBNull.
        /// </summary>
        private object ParseDayMonYearToDbNull(string dateTimeString)
        {
            // Dùng InvariantCulture là quan trọng để "MMM" luôn hiểu là (JAN, FEB, MAR...)
            if (DateTime.TryParseExact(dateTimeString, "dd-MMM-yyyy",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None,
                out DateTime parsedDate))
            {
                return parsedDate;
            }
            return DBNull.Value;
        }
        /// <summary>
        /// Phân tích chuỗi ngày tháng dạng "yyyyMMdd" (ví dụ: "20250623").
        /// Trả về DBNull.Value nếu thất bại.
        /// </summary>
        private object ParseCompactDateToDbNull(string yyyyMMdd)
        {
            // Kiểm tra chuỗi rỗng hoặc null trước
            if (string.IsNullOrEmpty(yyyyMMdd))
            {
                return DBNull.Value;
            }

            if (DateTime.TryParseExact(yyyyMMdd, "yyyyMMdd",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None,
                out DateTime parsedDate))
            {
                return parsedDate;
            }

            // (Có thể thêm log lỗi ở đây nếu cần)
            return DBNull.Value;
        }
    }
}
