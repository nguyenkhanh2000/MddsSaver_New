using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
    public class HsxOracleDataSaver : IHsxOracleDataSaver
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<HsxOracleDataSaver> _logger;

        public HsxOracleDataSaver(IServiceProvider serviceProvider, ILogger<HsxOracleDataSaver> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }
        public async Task SaveBatchAsync(List<object> messages, CancellationToken stoppingToken)
        {
            try
            {
                // 1. Nhóm các msg theo type
                var groupedMessages = messages
                    .Where(m => m != null)
                    .GroupBy(m => m.GetType());
                // Tạo danh sách các Task để chạy song song
                var allTasks = new List<Task>();
                foreach (var group in groupedMessages)
                {
                    var type = group.Key;
                    var items = group.ToList(); // 'items' là List<object>

                    // Gọi các hàm private tương ứng
                    Task insertTask = type switch
                    {
                        var t when t == typeof(ESecurityDefinition) =>
                            BulkInsertSecurityDefinitionsAsync(items.Cast<ESecurityDefinition>().ToList(), stoppingToken),

                        var t when t == typeof(EPrice) =>
                            BulkInsertPriceAsync(items.Cast<EPrice>().ToList(), stoppingToken),

                        var t when t == typeof(EPriceRecovery) =>
                            BulkInsertPriceRecoveryAsync(items.Cast<EPriceRecovery>().ToList(), stoppingToken),

                        var t when t == typeof(ESecurityStatus) =>
                            BulkInsertSecurityStatusAsync(items.Cast<ESecurityStatus>().ToList(), stoppingToken),

                        var t when t == typeof(EIndex) =>
                            BulkInsertIndexAsync(items.Cast<EIndex>().ToList(), stoppingToken),

                        var t when t == typeof(EInvestorPerIndustry) =>
                            BulkInsertInvestorPerIndustryAsync(items.Cast<EInvestorPerIndustry>().ToList(), stoppingToken),

                        var t when t == typeof(EInvestorPerSymbol) =>
                            BulkInsertInvestorPerSymbolAsync(items.Cast<EInvestorPerSymbol>().ToList(), stoppingToken),

                        var t when t == typeof(ETopNMembersPerSymbol) =>
                            BulkInsertTopNMembersPerSymbolAsync(items.Cast<ETopNMembersPerSymbol>().ToList(), stoppingToken),

                        var t when t == typeof(ESecurityInformationNotification) =>
                            BulkInsertSecurityInfoNotificationAsync(items.Cast<ESecurityInformationNotification>().ToList(), stoppingToken),

                        var t when t == typeof(ESymbolClosingInformation) =>
                            BulkInsertSymbolClosingInfoAsync(items.Cast<ESymbolClosingInformation>().ToList(), stoppingToken),

                        var t when t == typeof(EOpenInterest) =>
                            BulkInsertOpenInterestAsync(items.Cast<EOpenInterest>().ToList(), stoppingToken),

                        var t when t == typeof(EVolatilityInterruption) =>
                            BulkInsertVolatilityInterruptionAsync(items.Cast<EVolatilityInterruption>().ToList(), stoppingToken),

                        var t when t == typeof(EDeemTradePrice) =>
                            BulkInsertDeemTradePriceAsync(items.Cast<EDeemTradePrice>().ToList(), stoppingToken),

                        var t when t == typeof(EForeignerOrderLimit) =>
                            BulkInsertForeignerOrderLimitAsync(items.Cast<EForeignerOrderLimit>().ToList(), stoppingToken),

                        var t when t == typeof(EMarketMakerInformation) =>
                            BulkInsertMarketMakerInfoAsync(items.Cast<EMarketMakerInformation>().ToList(), stoppingToken),

                        var t when t == typeof(ESymbolEvent) =>
                            BulkInsertSymbolEventAsync(items.Cast<ESymbolEvent>().ToList(), stoppingToken),

                        var t when t == typeof(EDrvProductEvent) =>
                            BulkInsertDrvProductEventAsync(items.Cast<EDrvProductEvent>().ToList(), stoppingToken),

                        var t when t == typeof(EIndexConstituentsInformation) =>
                            BulkInsertIndexConstituentsAsync(items.Cast<EIndexConstituentsInformation>().ToList(), stoppingToken),

                        var t when t == typeof(EETFiNav) =>
                            BulkInsertETFiNavAsync(items.Cast<EETFiNav>().ToList(), stoppingToken),

                        var t when t == typeof(EETFiIndex) =>
                            BulkInsertETFiIndexAsync(items.Cast<EETFiIndex>().ToList(), stoppingToken),

                        var t when t == typeof(EETFTrackingError) =>
                            BulkInsertETFTrackingErrorAsync(items.Cast<EETFTrackingError>().ToList(), stoppingToken),

                        var t when t == typeof(ETopNSymbolsWithTradingQuantity) =>
                            BulkInsertTopNSymbolsWithTradingQuantityAsync(items.Cast<ETopNSymbolsWithTradingQuantity>().ToList(), stoppingToken),

                        var t when t == typeof(ETopNSymbolsWithCurrentPrice) =>
                            BulkInsertTopNSymbolsWithCurrentPriceAsync(items.Cast<ETopNSymbolsWithCurrentPrice>().ToList(), stoppingToken),

                        var t when t == typeof(ETopNSymbolsWithHighRatioOfPrice) =>
                            BulkInsertTopNSymbolsWithHighRatioOfPriceAsync(items.Cast<ETopNSymbolsWithHighRatioOfPrice>().ToList(), stoppingToken),

                        var t when t == typeof(ETopNSymbolsWithLowRatioOfPrice) =>
                            BulkInsertTopNSymbolsWithLowRatioOfPriceAsync(items.Cast<ETopNSymbolsWithLowRatioOfPrice>().ToList(), stoppingToken),

                        var t when t == typeof(ETradingResultOfForeignInvestors) =>
                            BulkInsertTradingResultOfForeignInvestorsAsync(items.Cast<ETradingResultOfForeignInvestors>().ToList(), stoppingToken),

                        var t when t == typeof(EDisclosure) =>
                            BulkInsertDisclosureAsync(items.Cast<EDisclosure>().ToList(), stoppingToken),

                        var t when t == typeof(ERandomEnd) =>
                            BulkInsertRandomEndAsync(items.Cast<ERandomEnd>().ToList(), stoppingToken),

                        var t when t == typeof(EPriceLimitExpansion) =>
                            BulkInsertPriceLimitExpansionAsync(items.Cast<EPriceLimitExpansion>().ToList(), stoppingToken),

                        _ => Task.CompletedTask // Bỏ qua các type không xác định
                    };
                    allTasks.Add(insertTask);
                }

                // Chờ tất cả các hoạt động bulk insert hoàn tất
                await Task.WhenAll(allTasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SaveBatchAsync: Lỗi khi thực hiện bulk insert!");
                // Ném lại exception để hàm gọi có thể xử lý (NACK messages)
                throw;
            }
        }
        private async Task BulkInsertSecurityDefinitionsAsync(List<ESecurityDefinition> definitions, CancellationToken stoppingToken)
        {
            const string tableName = "Msg_d";
            // 3.1. Tự tạo scope và connection
            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();

                try
                {
                    // 3.2. Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateSecurityDefinitionDataTable(definitions);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        // 3.3. Khởi tạo OracleBulkCopy
                        // Mặc định, mỗi WriteToServerAsync là một giao dịch (transaction) riêng lẻ.
                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // 3.4. Ánh xạ cột
                            //MapSecurityDefinitionColumns(bulkCopy);
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // 3.5. Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi để SaveBatchAsync bắt được
                }
            } // Scope và connection được tự động giải phóng
        }
        private async Task BulkInsertPriceAsync(List<EPrice> price, CancellationToken stoppingToken)
        {
            const string tableName = "Msg_x";
            // 3.1. Tự tạo scope và connection
            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // 3.2. Chuyển đổi List<T> thành DataTable
                    var dataTable = CreatePriceDataTable(price);
                    // 3.3. Khởi tạo OracleBulkCopy
                    // Mặc định, mỗi WriteToServerAsync là một giao dịch (transaction) riêng lẻ.
                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        // 3.3. Khởi tạo OracleBulkCopy
                        // Mặc định, mỗi WriteToServerAsync là một giao dịch (transaction) riêng lẻ.
                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // 3.4. Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns) 
                            {
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // 3.5. Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi để SaveBatchAsync bắt được
                }
            }
        }
        private async Task BulkInsertPriceRecoveryAsync(List<EPriceRecovery> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_w"; 

            // 3.1. Tự tạo scope và connection
            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // 3.2. Chuyển đổi List<T> thành DataTable
                    // <-- Gọi hàm tạo DataTable mới
                    var dataTable = CreatePriceRecoveryDataTable(messages);

                    // 3.3. Khởi tạo OracleBulkCopy
                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // 3.4. Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Quan trọng: Cột trong DataTable (col.ColumnName)
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // 3.5. Thực thi (Giữ nguyên logic của bạn)
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi để SaveBatchAsync bắt được
                }
            }
        }
        private async Task BulkInsertSecurityStatusAsync(List<ESecurityStatus> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_f"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateSecurityStatusDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertIndexAsync(List<EIndex> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m1";

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateIndexDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertInvestorPerIndustryAsync(List<EInvestorPerIndustry> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m2"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateInvestorPerIndustryDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertInvestorPerSymbolAsync(List<EInvestorPerSymbol> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m3"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateInvestorPerSymbolDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTopNMembersPerSymbolAsync(List<ETopNMembersPerSymbol> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m4"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTopNMembersPerSymbolDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertSecurityInfoNotificationAsync(List<ESecurityInformationNotification> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m7"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateSecurityInfoNotificationDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertSymbolClosingInfoAsync(List<ESymbolClosingInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_m8"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateSymbolClosingInfoDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertOpenInterestAsync(List<EOpenInterest> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ma"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateOpenInterestDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertVolatilityInterruptionAsync(List<EVolatilityInterruption> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_md"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateVolatilityInterruptionDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertDeemTradePriceAsync(List<EDeemTradePrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_me"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateDeemTradePriceDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertForeignerOrderLimitAsync(List<EForeignerOrderLimit> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mf"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateForeignerOrderLimitDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertMarketMakerInfoAsync(List<EMarketMakerInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mh"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateMarketMakerInfoDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertSymbolEventAsync(List<ESymbolEvent> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mi"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateSymbolEventDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertDrvProductEventAsync(List<EDrvProductEvent> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mj"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateDrvProductEventDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertIndexConstituentsAsync(List<EIndexConstituentsInformation> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ml"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateIndexConstituentsDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertETFiNavAsync(List<EETFiNav> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mm"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateETFiNavDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertETFiIndexAsync(List<EETFiIndex> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mn"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateETFiIndexDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertETFTrackingErrorAsync(List<EETFTrackingError> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mo"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateETFTrackingErrorDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTopNSymbolsWithTradingQuantityAsync(List<ETopNSymbolsWithTradingQuantity> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mp"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTopNSymbolsWithTradingQuantityDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTopNSymbolsWithCurrentPriceAsync(List<ETopNSymbolsWithCurrentPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mq"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTopNSymbolsWithCurrentPriceDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTopNSymbolsWithHighRatioOfPriceAsync(List<ETopNSymbolsWithHighRatioOfPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mr"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTopNSymbolsWithHighRatioOfPriceDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTopNSymbolsWithLowRatioOfPriceAsync(List<ETopNSymbolsWithLowRatioOfPrice> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_ms"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTopNSymbolsWithLowRatioOfPriceDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertTradingResultOfForeignInvestorsAsync(List<ETradingResultOfForeignInvestors> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mt"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateTradingResultOfForeignInvestorsDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertDisclosureAsync(List<EDisclosure> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mu"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateDisclosureDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertRandomEndAsync(List<ERandomEnd> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mw"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreateRandomEndDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private async Task BulkInsertPriceLimitExpansionAsync(List<EPriceLimitExpansion> messages, CancellationToken stoppingToken)
        {
            const string tableName = "msg_mx"; // <-- Tên bảng đích

            using (var scope = _serviceProvider.CreateScope())
            {
                var dbConnection = scope.ServiceProvider.GetRequiredService<IDbConnection>();
                try
                {
                    // Chuyển đổi List<T> thành DataTable
                    var dataTable = CreatePriceLimitExpansionDataTable(messages);

                    using (var connection = (OracleConnection)dbConnection)
                    {
                        await connection.OpenAsync(stoppingToken);

                        using (var bulkCopy = new OracleBulkCopy(connection))
                        {
                            bulkCopy.DestinationTableName = tableName;
                            bulkCopy.BulkCopyTimeout = 600;

                            // Ánh xạ cột
                            foreach (DataColumn col in dataTable.Columns)
                            {
                                // Cột trong DataTable (col.ColumnName) 
                                // phải khớp 1:1 với tên cột trong CSDL (lowercase)
                                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                            }

                            // Thực thi
                            await Task.Run(() => bulkCopy.WriteToServer(dataTable), stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Lỗi khi Oracle Bulk Insert vào bảng {TableName}", tableName);
                    throw; // Ném lại lỗi
                }
            }
        }
        private DataTable CreateSecurityDefinitionDataTable(List<ESecurityDefinition> definitions)
        {
            var dt = new DataTable();

            // 1. ĐỊNH NGHĨA CỘT (DATATABLE)
            // Tên cột đã được CHUYỂN SANG LOWERCASE để khớp với schema 'msg_d'

            // Header
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int));
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long));
            dt.Columns.Add("sendingtime", typeof(DateTime));
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long));
            dt.Columns.Add("securityexchange", typeof(string));

            // Payload (Security Definition)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("tickercode", typeof(string));
            dt.Columns.Add("symbolshortcode", typeof(string));
            dt.Columns.Add("symbolname", typeof(string)); 
            dt.Columns.Add("symbolenname", typeof(string));
            dt.Columns.Add("productid", typeof(string));
            dt.Columns.Add("productgrpid", typeof(string));
            dt.Columns.Add("securitygroupid", typeof(string));
            dt.Columns.Add("putorcall", typeof(string));
            dt.Columns.Add("exercisestyle", typeof(string));
            dt.Columns.Add("maturitymonthyear", typeof(string));
            dt.Columns.Add("maturitydate", typeof(string));
            dt.Columns.Add("issuer", typeof(string));
            dt.Columns.Add("issuedate", typeof(string));
            dt.Columns.Add("contractmultiplier", typeof(decimal));
            dt.Columns.Add("couponrate", typeof(decimal));
            dt.Columns.Add("currency", typeof(string));
            dt.Columns.Add("listedshares", typeof(long));
            dt.Columns.Add("highlimitprice", typeof(decimal));
            dt.Columns.Add("lowlimitprice", typeof(decimal));
            dt.Columns.Add("strikeprice", typeof(decimal));
            dt.Columns.Add("securitystatus", typeof(string));
            dt.Columns.Add("contractsize", typeof(decimal));
            dt.Columns.Add("settlmethod", typeof(string));
            dt.Columns.Add("yield", typeof(decimal));
            dt.Columns.Add("referenceprice", typeof(decimal));
            dt.Columns.Add("evaluationprice", typeof(decimal));
            dt.Columns.Add("hgstorderprice", typeof(decimal));
            dt.Columns.Add("lwstorderprice", typeof(decimal));
            dt.Columns.Add("prevclosepx", typeof(decimal));
            dt.Columns.Add("symbolcloseinfopxtype", typeof(string));
            dt.Columns.Add("firsttradingdate", typeof(string));
            dt.Columns.Add("finaltradedate", typeof(string));
            dt.Columns.Add("finalsettledate", typeof(string));
            dt.Columns.Add("listingdate", typeof(string));
            dt.Columns.Add("retriggeringconditioncode", typeof(string));
            dt.Columns.Add("exclasstype", typeof(string));
            dt.Columns.Add("vwap", typeof(decimal));
            dt.Columns.Add("symboladminstatuscode", typeof(string));
            dt.Columns.Add("symboltradingmethodsc", typeof(string));
            dt.Columns.Add("symboltradingsantionsc", typeof(string));
            dt.Columns.Add("sectortypecode", typeof(string));
            dt.Columns.Add("redumptiondate", typeof(string));

            // Footer
            dt.Columns.Add("checksum", typeof(long));
            dt.Columns.Add("createtime", typeof(DateTime));

            // 2. ĐỔ DỮ LIỆU TỪ LIST VÀO DATATABLE
            // Logic gán giá trị giữ nguyên, chỉ thay đổi key "row[...]" sang lowercase
            foreach (var def in definitions)
            {
                var row = dt.NewRow();

                row["beginstring"] = (object)def.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)def.BodyLength;
                row["msgtype"] = (object)def.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)def.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)def.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = def.MsgSeqNum;

                row["sendingtime"] = ParseDashDateTimeToDbNull(def.SendingTime);

                row["marketid"] = (object)def.MarketID ?? DBNull.Value;
                row["boardid"] = (object)def.BoardID ?? DBNull.Value;
                row["totnumreports"] = def.TotNumReports; // Giả định non-null
                row["securityexchange"] = (object)def.SecurityExchange ?? DBNull.Value;

                row["symbol"] = (object)def.Symbol ?? DBNull.Value;
                row["tickercode"] = (object)def.TickerCode ?? DBNull.Value;
                row["symbolshortcode"] = (object)def.SymbolShortCode ?? DBNull.Value;
                row["symbolname"] = (object)def.SymbolName ?? DBNull.Value;
                row["symbolenname"] = (object)def.SymbolEnglishName ?? DBNull.Value; // Khớp property
                row["productid"] = (object)def.ProductID ?? DBNull.Value;
                row["productgrpid"] = (object)def.ProductGrpID ?? DBNull.Value;
                row["securitygroupid"] = (object)def.SecurityGroupID ?? DBNull.Value;
                row["putorcall"] = (object)def.PutOrCall ?? DBNull.Value;
                row["exercisestyle"] = (object)def.ExerciseStyle ?? DBNull.Value;
                row["maturitymonthyear"] = (object)def.MaturityMonthYear ?? DBNull.Value;
                row["maturitydate"] = (object)def.MaturityDate ?? DBNull.Value;
                row["issuer"] = (object)def.Issuer ?? DBNull.Value;
                row["issuedate"] = (object)def.IssueDate ?? DBNull.Value;
                row["contractmultiplier"] = (object)def.ContractMultiplier ?? DBNull.Value;
                row["couponrate"] = (object)def.CouponRate ?? DBNull.Value;
                row["currency"] = (object)def.Currency ?? DBNull.Value;
                row["listedshares"] = def.ListedShares; // Giả định non-null
                row["highlimitprice"] = (object)def.HighLimitPrice ?? DBNull.Value;
                row["lowlimitprice"] = (object)def.LowLimitPrice ?? DBNull.Value;
                row["strikeprice"] = (object)def.StrikePrice ?? DBNull.Value;
                row["securitystatus"] = (object)def.SecurityStatus ?? DBNull.Value;
                row["contractsize"] = (object)def.ContractSize ?? DBNull.Value;
                row["settlmethod"] = (object)def.SettlMethod ?? DBNull.Value;
                row["yield"] = (object)def.Yield ?? DBNull.Value;
                row["referenceprice"] = (object)def.ReferencePrice ?? DBNull.Value;
                row["evaluationprice"] = (object)def.EvaluationPrice ?? DBNull.Value;
                row["hgstorderprice"] = (object)def.HgstOrderPrice ?? DBNull.Value;
                row["lwstorderprice"] = (object)def.LwstOrderPrice ?? DBNull.Value;
                row["prevclosepx"] = (object)def.PrevClosePx ?? DBNull.Value;
                row["symbolcloseinfopxtype"] = (object)def.SymbolCloseInfoPxType ?? DBNull.Value;
                row["firsttradingdate"] = (object)def.FirstTradingDate ?? DBNull.Value;
                row["finaltradedate"] = (object)def.FinalTradeDate ?? DBNull.Value;
                row["finalsettledate"] = (object)def.FinalSettleDate ?? DBNull.Value;
                row["listingdate"] = (object)def.ListingDate ?? DBNull.Value;
                row["retriggeringconditioncode"] = (object)def.RandomEndTriggeringConditionCode ?? DBNull.Value; // Khớp property
                row["exclasstype"] = (object)def.ExClassType ?? DBNull.Value;
                row["vwap"] = (object)def.VWAP ?? DBNull.Value;
                row["symboladminstatuscode"] = (object)def.SymbolAdminStatusCode ?? DBNull.Value;
                row["symboltradingmethodsc"] = (object)def.SymbolTradingMethodStatusCode ?? DBNull.Value; // Khớp property
                row["symboltradingsantionsc"] = (object)def.SymbolTradingSantionStatusCode ?? DBNull.Value; // Khớp property
                row["sectortypecode"] = (object)def.SectorTypeCode ?? DBNull.Value;
                row["redumptiondate"] = (object)def.RedumptionDate ?? DBNull.Value;

                // Xử lý CheckSum an toàn hơn (giống các hàm bạn hỏi trước)
                if (long.TryParse(def.CheckSum, out long parsedCheckSum))
                {
                    // Giả định bạn *không* dùng magic number -9999999 cho bảng này
                    row["checksum"] = parsedCheckSum;
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }       
        private DataTable CreatePriceDataTable(List<EPrice> prices)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_x) ---

            // Thông tin header
            dt.Columns.Add("BeginString", typeof(string));
            dt.Columns.Add("BodyLength", typeof(int));
            dt.Columns.Add("MsgType", typeof(string));
            dt.Columns.Add("SenderCompID", typeof(string));
            dt.Columns.Add("TargetCompID", typeof(string));
            dt.Columns.Add("MsgSeqNum", typeof(long));
            dt.Columns.Add("SendingTime", typeof(DateTime));
            dt.Columns.Add("MarketID", typeof(string));
            dt.Columns.Add("BoardID", typeof(string));
            dt.Columns.Add("TradingSessionID", typeof(string));
            dt.Columns.Add("Symbol", typeof(string));

            // Thêm: tradedate và transacttime
            dt.Columns.Add("TradeDate", typeof(DateTime));
            dt.Columns.Add("TransactTime", typeof(string));

            // Các cột thống kê (Sửa kiểu dữ liệu)
            dt.Columns.Add("TotalVolumeTraded", typeof(long));
            dt.Columns.Add("GrossTradeAmt", typeof(decimal));
            dt.Columns.Add("SellTotOrderQty", typeof(long));
            dt.Columns.Add("BuyTotOrderQty", typeof(long));
            dt.Columns.Add("SellValidOrderCnt", typeof(long));
            dt.Columns.Add("BuyValidOrderCnt", typeof(long));

            // Thêm các cột giá 1-10 (Đã đổi tên và sửa kiểu)
            for (int i = 1; i <= 10; i++)
            {
                dt.Columns.Add($"Bp{i}", typeof(decimal));
                dt.Columns.Add($"Bq{i}", typeof(long));
                dt.Columns.Add($"Bp{i}_Noo", typeof(long));
                dt.Columns.Add($"Bp{i}_Mdey", typeof(decimal));
                dt.Columns.Add($"Bp{i}_Mdemms", typeof(long));
                dt.Columns.Add($"Bp{i}_Mdepno", typeof(int)); // Luôn NULL

                dt.Columns.Add($"Sp{i}", typeof(decimal));
                dt.Columns.Add($"Sq{i}", typeof(long));
                dt.Columns.Add($"Sp{i}_Noo", typeof(long));
                dt.Columns.Add($"Sp{i}_Mdey", typeof(decimal));
                dt.Columns.Add($"Sp{i}_Mdemms", typeof(long));
                dt.Columns.Add($"Sp{i}_Mdepno", typeof(int)); // Luôn NULL
            }

            // Thêm: Các cột giá cuối (mp, mq, op, lp, hp)
            dt.Columns.Add("Mp", typeof(decimal));
            dt.Columns.Add("Mq", typeof(long));
            dt.Columns.Add("Op", typeof(decimal));
            dt.Columns.Add("Lp", typeof(decimal));
            dt.Columns.Add("Hp", typeof(decimal));

            // Cột cuối
            dt.Columns.Add("CheckSum", typeof(long));
            dt.Columns.Add("CreateTime", typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU (Khớp với logic COPY) ---
            foreach (var price in prices)
            {
                DataRow row = dt.NewRow();

                row["BeginString"] = price.BeginString;
                row["BodyLength"] = (int)price.BodyLength;
                row["MsgType"] = price.MsgType;
                row["SenderCompID"] = price.SenderCompID;
                row["TargetCompID"] = price.TargetCompID;
                row["MsgSeqNum"] = price.MsgSeqNum;

                // Logic Parse SendingTime
                row["SendingTime"] = ParseDashDateTimeToDbNull(price.SendingTime);

                row["MarketID"] = price.MarketID;
                row["BoardID"] = price.BoardID;
                row["TradingSessionID"] = price.TradingSessionID;
                row["Symbol"] = price.Symbol;

                // Logic Parse TradeDate
                if (DateTime.TryParseExact(price.TradeDate, "yyyyMMdd",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime tradeDate))
                {
                    row["TradeDate"] = tradeDate;
                }
                else
                {
                    row["TradeDate"] = DBNull.Value;
                }

                row["TransactTime"] = price.TransactTime;

                // Các trường thống kê
                row["TotalVolumeTraded"] = ToDbNull(price.TotalVolumeTraded);
                row["GrossTradeAmt"] = ToDbNull(price.GrossTradeAmt);
                row["SellTotOrderQty"] = ToDbNull(price.SellTotOrderQty);
                row["BuyTotOrderQty"] = ToDbNull(price.BuyTotOrderQty);
                row["SellValidOrderCnt"] = ToDbNull(price.SellValidOrderCnt);
                row["BuyValidOrderCnt"] = ToDbNull(price.BuyValidOrderCnt);

                // Cấp 1
                row["Bp1"] = ToDbNull(price.BuyPrice1);
                row["Bq1"] = ToDbNull(price.BuyQuantity1);
                row["Bp1_Noo"] = ToDbNull((long)price.BuyPrice1_NOO);
                row["Bp1_Mdey"] = ToDbNull(price.BuyPrice1_MDEY);
                row["Bp1_Mdemms"] = ToDbNull(price.BuyPrice1_MDEMMS);
                row["Bp1_Mdepno"] = DBNull.Value; // Khớp: Luôn NULL
                row["Sp1"] = ToDbNull(price.SellPrice1);
                row["Sq1"] = ToDbNull(price.SellQuantity1);
                row["Sp1_Noo"] = ToDbNull((long)price.SellPrice1_NOO);
                row["Sp1_Mdey"] = ToDbNull(price.SellPrice1_MDEY);
                row["Sp1_Mdemms"] = ToDbNull(price.SellPrice1_MDEMMS);
                row["Sp1_Mdepno"] = DBNull.Value; // Khớp: Luôn NULL

                // Cấp 2
                row["Bp2"] = ToDbNull(price.BuyPrice2);
                row["Bq2"] = ToDbNull(price.BuyQuantity2);
                row["Bp2_Noo"] = ToDbNull((long)price.BuyPrice2_NOO);
                row["Bp2_Mdey"] = ToDbNull(price.BuyPrice2_MDEY);
                row["Bp2_Mdemms"] = ToDbNull(price.BuyPrice2_MDEMMS);
                row["Bp2_Mdepno"] = DBNull.Value;
                row["Sp2"] = ToDbNull(price.SellPrice2);
                row["Sq2"] = ToDbNull(price.SellQuantity2);
                row["Sp2_Noo"] = ToDbNull((long)price.SellPrice2_NOO);
                row["Sp2_Mdey"] = ToDbNull(price.SellPrice2_MDEY);
                row["Sp2_Mdemms"] = ToDbNull(price.SellPrice2_MDEMMS);
                row["Sp2_Mdepno"] = DBNull.Value;

                // Cấp 3
                row["Bp3"] = ToDbNull(price.BuyPrice3);
                row["Bq3"] = ToDbNull(price.BuyQuantity3);
                row["Bp3_Noo"] = ToDbNull((long)price.BuyPrice3_NOO);
                row["Bp3_Mdey"] = ToDbNull(price.BuyPrice3_MDEY);
                row["Bp3_Mdemms"] = ToDbNull(price.BuyPrice3_MDEMMS);
                row["Bp3_Mdepno"] = DBNull.Value;
                row["Sp3"] = ToDbNull(price.SellPrice3);
                row["Sq3"] = ToDbNull(price.SellQuantity3);
                row["Sp3_Noo"] = ToDbNull((long)price.SellPrice3_NOO);
                row["Sp3_Mdey"] = ToDbNull(price.SellPrice3_MDEY);
                row["Sp3_Mdemms"] = ToDbNull(price.SellPrice3_MDEMMS);
                row["Sp3_Mdepno"] = DBNull.Value;

                // Cấp 4
                row["Bp4"] = ToDbNull(price.BuyPrice4);
                row["Bq4"] = ToDbNull(price.BuyQuantity4);
                row["Bp4_Noo"] = ToDbNull((long)price.BuyPrice4_NOO);
                row["Bp4_Mdey"] = ToDbNull(price.BuyPrice4_MDEY);
                row["Bp4_Mdemms"] = ToDbNull(price.BuyPrice4_MDEMMS);
                row["Bp4_Mdepno"] = DBNull.Value;
                row["Sp4"] = ToDbNull(price.SellPrice4);
                row["Sq4"] = ToDbNull(price.SellQuantity4);
                row["Sp4_Noo"] = ToDbNull((long)price.SellPrice4_NOO);
                row["Sp4_Mdey"] = ToDbNull(price.SellPrice4_MDEY);
                row["Sp4_Mdemms"] = ToDbNull(price.SellPrice4_MDEMMS);
                row["Sp4_Mdepno"] = DBNull.Value;

                // Cấp 5
                row["Bp5"] = ToDbNull(price.BuyPrice5);
                row["Bq5"] = ToDbNull(price.BuyQuantity5);
                row["Bp5_Noo"] = ToDbNull((long)price.BuyPrice5_NOO);
                row["Bp5_Mdey"] = ToDbNull(price.BuyPrice5_MDEY);
                row["Bp5_Mdemms"] = ToDbNull(price.BuyPrice5_MDEMMS);
                row["Bp5_Mdepno"] = DBNull.Value;
                row["Sp5"] = ToDbNull(price.SellPrice5);
                row["Sq5"] = ToDbNull(price.SellQuantity5);
                row["Sp5_Noo"] = ToDbNull((long)price.SellPrice5_NOO);
                row["Sp5_Mdey"] = ToDbNull(price.SellPrice5_MDEY);
                row["Sp5_Mdemms"] = ToDbNull(price.SellPrice5_MDEMMS);
                row["Sp5_Mdepno"] = DBNull.Value;

                // Cấp 6
                row["Bp6"] = ToDbNull(price.BuyPrice6);
                row["Bq6"] = ToDbNull(price.BuyQuantity6);
                row["Bp6_Noo"] = ToDbNull((long)price.BuyPrice6_NOO);
                row["Bp6_Mdey"] = ToDbNull(price.BuyPrice6_MDEY);
                row["Bp6_Mdemms"] = ToDbNull(price.BuyPrice6_MDEMMS);
                row["Bp6_Mdepno"] = DBNull.Value;
                row["Sp6"] = ToDbNull(price.SellPrice6);
                row["Sq6"] = ToDbNull(price.SellQuantity6);
                row["Sp6_Noo"] = ToDbNull((long)price.SellPrice6_NOO);
                row["Sp6_Mdey"] = ToDbNull(price.SellPrice6_MDEY);
                row["Sp6_Mdemms"] = ToDbNull(price.SellPrice6_MDEMMS);
                row["Sp6_Mdepno"] = DBNull.Value;

                // Cấp 7
                row["Bp7"] = ToDbNull(price.BuyPrice7);
                row["Bq7"] = ToDbNull(price.BuyQuantity7);
                row["Bp7_Noo"] = ToDbNull((long)price.BuyPrice7_NOO);
                row["Bp7_Mdey"] = ToDbNull(price.BuyPrice7_MDEY);
                row["Bp7_Mdemms"] = ToDbNull(price.BuyPrice7_MDEMMS);
                row["Bp7_Mdepno"] = DBNull.Value;
                row["Sp7"] = ToDbNull(price.SellPrice7);
                row["Sq7"] = ToDbNull(price.SellQuantity7);
                row["Sp7_Noo"] = ToDbNull((long)price.SellPrice7_NOO);
                row["Sp7_Mdey"] = ToDbNull(price.SellPrice7_MDEY);
                row["Sp7_Mdemms"] = ToDbNull(price.SellPrice7_MDEMMS);
                row["Sp7_Mdepno"] = DBNull.Value;

                // Cấp 8
                row["Bp8"] = ToDbNull(price.BuyPrice8);
                row["Bq8"] = ToDbNull(price.BuyQuantity8);
                row["Bp8_Noo"] = ToDbNull((long)price.BuyPrice8_NOO);
                row["Bp8_Mdey"] = ToDbNull(price.BuyPrice8_MDEY);
                row["Bp8_Mdemms"] = ToDbNull(price.BuyPrice8_MDEMMS);
                row["Bp8_Mdepno"] = DBNull.Value;
                row["Sp8"] = ToDbNull(price.SellPrice8);
                row["Sq8"] = ToDbNull(price.SellQuantity8);
                row["Sp8_Noo"] = ToDbNull((long)price.SellPrice8_NOO);
                row["Sp8_Mdey"] = ToDbNull(price.SellPrice8_MDEY);
                row["Sp8_Mdemms"] = ToDbNull(price.SellPrice8_MDEMMS);
                row["Sp8_Mdepno"] = DBNull.Value;

                // Cấp 9
                row["Bp9"] = ToDbNull(price.BuyPrice9);
                row["Bq9"] = ToDbNull(price.BuyQuantity9);
                row["Bp9_Noo"] = ToDbNull((long)price.BuyPrice9_NOO);
                row["Bp9_Mdey"] = ToDbNull(price.BuyPrice9_MDEY);
                row["Bp9_Mdemms"] = ToDbNull(price.BuyPrice9_MDEMMS);
                row["Bp9_Mdepno"] = DBNull.Value;
                row["Sp9"] = ToDbNull(price.SellPrice9);
                row["Sq9"] = ToDbNull(price.SellQuantity9);
                row["Sp9_Noo"] = ToDbNull((long)price.SellPrice9_NOO);
                row["Sp9_Mdey"] = ToDbNull(price.SellPrice9_MDEY);
                row["Sp9_Mdemms"] = ToDbNull(price.SellPrice9_MDEMMS);
                row["Sp9_Mdepno"] = DBNull.Value;

                // Cấp 10
                row["Bp10"] = ToDbNull(price.BuyPrice10);
                row["Bq10"] = ToDbNull(price.BuyQuantity10);
                row["Bp10_Noo"] = ToDbNull((long)price.BuyPrice10_NOO);
                row["Bp10_Mdey"] = ToDbNull(price.BuyPrice10_MDEY);
                row["Bp10_Mdemms"] = ToDbNull(price.BuyPrice10_MDEMMS);
                row["Bp10_Mdepno"] = DBNull.Value;
                row["Sp10"] = ToDbNull(price.SellPrice10);
                row["Sq10"] = ToDbNull(price.SellQuantity10);
                row["Sp10_Noo"] = ToDbNull((long)price.SellPrice10_NOO);
                row["Sp10_Mdey"] = ToDbNull(price.SellPrice10_MDEY);
                row["Sp10_Mdemms"] = ToDbNull(price.SellPrice10_MDEMMS);
                row["Sp10_Mdepno"] = DBNull.Value;

                // Các trường giá cuối
                row["Mp"] = ToDbNull(price.MatchPrice);
                row["Mq"] = ToDbNull(price.MatchQuantity);
                row["Op"] = ToDbNull(price.OpenPrice);
                row["Lp"] = ToDbNull(price.LowestPrice);
                row["Hp"] = ToDbNull(price.HighestPrice);

                // Cột cuối (khớp logic COPY)
                row["CheckSum"] = ToDbNull(long.Parse(price.CheckSum));
                row["CreateTime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreatePriceRecoveryDataTable(List<EPriceRecovery> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_w) ---
            // Tên cột BẮT BUỘC là lowercase để khớp với schema msg_w

            // Thông tin header
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int));
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long));
            dt.Columns.Add("sendingtime", typeof(DateTime));
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("tradingsessionid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));

            // Các cột giá (mới so với msg_x)
            dt.Columns.Add("opnpx", typeof(decimal));
            dt.Columns.Add("trdsessnhighpx", typeof(decimal));
            dt.Columns.Add("trdsessnlowpx", typeof(decimal));
            dt.Columns.Add("symbolcloseinfopx", typeof(decimal));
            dt.Columns.Add("opnpxyld", typeof(decimal));
            dt.Columns.Add("trdsessnhighpxyld", typeof(decimal));
            dt.Columns.Add("trdsessnlowpxyld", typeof(decimal));
            dt.Columns.Add("clspxyld", typeof(decimal));

            // Các cột thống kê
            dt.Columns.Add("totalvolumetraded", typeof(long));
            dt.Columns.Add("grosstradeamt", typeof(decimal));
            dt.Columns.Add("selltotorderqty", typeof(long));
            dt.Columns.Add("buytotorderqty", typeof(long));
            dt.Columns.Add("sellvalidordercnt", typeof(long));
            dt.Columns.Add("buyvalidordercnt", typeof(long));

            // Các cột giá 1-10 (B/S)
            for (int i = 1; i <= 10; i++)
            {
                dt.Columns.Add($"bp{i}", typeof(decimal));
                dt.Columns.Add($"bq{i}", typeof(long));
                dt.Columns.Add($"bp{i}_noo", typeof(long));
                dt.Columns.Add($"bp{i}_mdey", typeof(decimal));
                dt.Columns.Add($"bp{i}_mdemms", typeof(long));
                dt.Columns.Add($"bp{i}_mdepno", typeof(int));

                dt.Columns.Add($"sp{i}", typeof(decimal));
                dt.Columns.Add($"sq{i}", typeof(long));
                dt.Columns.Add($"sp{i}_noo", typeof(long));
                dt.Columns.Add($"sp{i}_mdey", typeof(decimal));
                dt.Columns.Add($"sp{i}_mdemms", typeof(long));
                dt.Columns.Add($"sp{i}_mdepno", typeof(int));
            }

            // Cột cuối
            dt.Columns.Add("checksum", typeof(long));
            dt.Columns.Add("createtime", typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var priceRecovery in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row["beginstring"] = priceRecovery.BeginString;
                row["bodylength"] = (int)priceRecovery.BodyLength;
                row["msgtype"] = priceRecovery.MsgType;
                row["sendercompid"] = priceRecovery.SenderCompID;
                row["targetcompid"] = priceRecovery.TargetCompID;
                row["msgseqnum"] = priceRecovery.MsgSeqNum;

                row["sendingtime"] = ParseCompactDateTimeToDbNull(priceRecovery.SendingTime);

                row["marketid"] = priceRecovery.MarketID;
                row["boardid"] = priceRecovery.BoardID;
                row["tradingsessionid"] = priceRecovery.TradingSessionID;
                row["symbol"] = priceRecovery.Symbol;

                // Các trường giá (mới)
                row["opnpx"] = ToDbNull(priceRecovery.OpnPx);
                row["trdsessnhighpx"] = ToDbNull(priceRecovery.TrdSessnHighPx);
                row["trdsessnlowpx"] = ToDbNull(priceRecovery.TrdSessnLowPx);
                row["symbolcloseinfopx"] = ToDbNull(priceRecovery.SymbolCloseInfoPx);
                row["opnpxyld"] = ToDbNull(priceRecovery.OpnPxYld);
                row["trdsessnhighpxyld"] = ToDbNull(priceRecovery.TrdSessnHighPxYld);
                row["trdsessnlowpxyld"] = ToDbNull(priceRecovery.TrdSessnLowPxYld);
                row["clspxyld"] = ToDbNull(priceRecovery.ClsPxYld);

                // Các trường thống kê
                row["totalvolumetraded"] = ToDbNull(priceRecovery.TotalVolumeTraded);
                row["grosstradeamt"] = ToDbNull(priceRecovery.GrossTradeAmt);
                row["selltotorderqty"] = ToDbNull(priceRecovery.SellTotOrderQty);
                row["buytotorderqty"] = ToDbNull(priceRecovery.BuyTotOrderQty);
                row["sellvalidordercnt"] = ToDbNull(priceRecovery.SellValidOrderCnt);
                row["buyvalidordercnt"] = ToDbNull(priceRecovery.BuyValidOrderCnt);

                // Cấp 1
                row["bp1"] = ToDbNull(priceRecovery.BuyPrice1);
                row["bq1"] = ToDbNull(priceRecovery.BuyQuantity1);
                row["bp1_noo"] = ToDbNull((long)priceRecovery.BuyPrice1_NOO);
                row["bp1_mdey"] = ToDbNull(priceRecovery.BuyPrice1_MDEY);
                row["bp1_mdemms"] = ToDbNull(priceRecovery.BuyPrice1_MDEMMS);
                row["bp1_mdepno"] = DBNull.Value; 
                row["sp1"] = ToDbNull(priceRecovery.SellPrice1);
                row["sq1"] = ToDbNull(priceRecovery.SellQuantity1);
                row["sp1_noo"] = ToDbNull((long)priceRecovery.SellPrice1_NOO);
                row["sp1_mdey"] = ToDbNull(priceRecovery.SellPrice1_MDEY);
                row["sp1_mdemms"] = ToDbNull(priceRecovery.SellPrice1_MDEMMS);
                row["sp1_mdepno"] = DBNull.Value; 

                // Cấp 2
                row["bp2"] = ToDbNull(priceRecovery.BuyPrice2);
                row["bq2"] = ToDbNull(priceRecovery.BuyQuantity2);
                row["bp2_noo"] = ToDbNull((long)priceRecovery.BuyPrice2_NOO);
                row["bp2_mdey"] = ToDbNull(priceRecovery.BuyPrice2_MDEY);
                row["bp2_mdemms"] = ToDbNull(priceRecovery.BuyPrice2_MDEMMS);
                row["bp2_mdepno"] = DBNull.Value;
                row["sp2"] = ToDbNull(priceRecovery.SellPrice2);
                row["sq2"] = ToDbNull(priceRecovery.SellQuantity2);
                row["sp2_noo"] = ToDbNull((long)priceRecovery.SellPrice2_NOO);
                row["sp2_mdey"] = ToDbNull(priceRecovery.SellPrice2_MDEY);
                row["sp2_mdemms"] = ToDbNull(priceRecovery.SellPrice2_MDEMMS);
                row["sp2_mdepno"] = DBNull.Value;

                // Cấp 3
                row["bp3"] = ToDbNull(priceRecovery.BuyPrice3);
                row["bq3"] = ToDbNull(priceRecovery.BuyQuantity3);
                row["bp3_noo"] = ToDbNull((long)priceRecovery.BuyPrice3_NOO);
                row["bp3_mdey"] = ToDbNull(priceRecovery.BuyPrice3_MDEY);
                row["bp3_mdemms"] = ToDbNull(priceRecovery.BuyPrice3_MDEMMS);
                row["bp3_mdepno"] = DBNull.Value;
                row["sp3"] = ToDbNull(priceRecovery.SellPrice3);
                row["sq3"] = ToDbNull(priceRecovery.SellQuantity3);
                row["sp3_noo"] = ToDbNull((long)priceRecovery.SellPrice3_NOO);
                row["sp3_mdey"] = ToDbNull(priceRecovery.SellPrice3_MDEY);
                row["sp3_mdemms"] = ToDbNull(priceRecovery.SellPrice3_MDEMMS);
                row["sp3_mdepno"] = DBNull.Value;

                // Cấp 4
                row["bp4"] = ToDbNull(priceRecovery.BuyPrice4);
                row["bq4"] = ToDbNull(priceRecovery.BuyQuantity4);
                row["bp4_noo"] = ToDbNull((long)priceRecovery.BuyPrice4_NOO);
                row["bp4_mdey"] = ToDbNull(priceRecovery.BuyPrice4_MDEY);
                row["bp4_mdemms"] = ToDbNull(priceRecovery.BuyPrice4_MDEMMS);
                row["bp4_mdepno"] = DBNull.Value;
                row["sp4"] = ToDbNull(priceRecovery.SellPrice4);
                row["sq4"] = ToDbNull(priceRecovery.SellQuantity4);
                row["sp4_noo"] = ToDbNull((long)priceRecovery.SellPrice4_NOO);
                row["sp4_mdey"] = ToDbNull(priceRecovery.SellPrice4_MDEY);
                row["sp4_mdemms"] = ToDbNull(priceRecovery.SellPrice4_MDEMMS);
                row["sp4_mdepno"] = DBNull.Value;

                // Cấp 5
                row["bp5"] = ToDbNull(priceRecovery.BuyPrice5);
                row["bq5"] = ToDbNull(priceRecovery.BuyQuantity5);
                row["bp5_noo"] = ToDbNull((long)priceRecovery.BuyPrice5_NOO);
                row["bp5_mdey"] = ToDbNull(priceRecovery.BuyPrice5_MDEY);
                row["bp5_mdemms"] = ToDbNull(priceRecovery.BuyPrice5_MDEMMS);
                row["bp5_mdepno"] = DBNull.Value;
                row["sp5"] = ToDbNull(priceRecovery.SellPrice5);
                row["sq5"] = ToDbNull(priceRecovery.SellQuantity5);
                row["sp5_noo"] = ToDbNull((long)priceRecovery.SellPrice5_NOO);
                row["sp5_mdey"] = ToDbNull(priceRecovery.SellPrice5_MDEY);
                row["sp5_mdemms"] = ToDbNull(priceRecovery.SellPrice5_MDEMMS);
                row["sp5_mdepno"] = DBNull.Value;

                // Cấp 6
                row["bp6"] = ToDbNull(priceRecovery.BuyPrice6);
                row["bq6"] = ToDbNull(priceRecovery.BuyQuantity6);
                row["bp6_noo"] = ToDbNull((long)priceRecovery.BuyPrice6_NOO);
                row["bp6_mdey"] = ToDbNull(priceRecovery.BuyPrice6_MDEY);
                row["bp6_mdemms"] = ToDbNull(priceRecovery.BuyPrice6_MDEMMS);
                row["bp6_mdepno"] = DBNull.Value;
                row["sp6"] = ToDbNull(priceRecovery.SellPrice6);
                row["sq6"] = ToDbNull(priceRecovery.SellQuantity6);
                row["sp6_noo"] = ToDbNull((long)priceRecovery.SellPrice6_NOO);
                row["sp6_mdey"] = ToDbNull(priceRecovery.SellPrice6_MDEY);
                row["sp6_mdemms"] = ToDbNull(priceRecovery.SellPrice6_MDEMMS);
                row["sp6_mdepno"] = DBNull.Value;

                // Cấp 7
                row["bp7"] = ToDbNull(priceRecovery.BuyPrice7);
                row["bq7"] = ToDbNull(priceRecovery.BuyQuantity7);
                row["bp7_noo"] = ToDbNull((long)priceRecovery.BuyPrice7_NOO);
                row["bp7_mdey"] = ToDbNull(priceRecovery.BuyPrice7_MDEY);
                row["bp7_mdemms"] = ToDbNull(priceRecovery.BuyPrice7_MDEMMS);
                row["bp7_mdepno"] = DBNull.Value;
                row["sp7"] = ToDbNull(priceRecovery.SellPrice7);
                row["sq7"] = ToDbNull(priceRecovery.SellQuantity7);
                row["sp7_noo"] = ToDbNull((long)priceRecovery.SellPrice7_NOO);
                row["sp7_mdey"] = ToDbNull(priceRecovery.SellPrice7_MDEY);
                row["sp7_mdemms"] = ToDbNull(priceRecovery.SellPrice7_MDEMMS);
                row["sp7_mdepno"] = DBNull.Value;

                // Cấp 8
                row["bp8"] = ToDbNull(priceRecovery.BuyPrice8);
                row["bq8"] = ToDbNull(priceRecovery.BuyQuantity8);
                row["bp8_noo"] = ToDbNull((long)priceRecovery.BuyPrice8_NOO);
                row["bp8_mdey"] = ToDbNull(priceRecovery.BuyPrice8_MDEY);
                row["bp8_mdemms"] = ToDbNull(priceRecovery.BuyPrice8_MDEMMS);
                row["bp8_mdepno"] = DBNull.Value;
                row["sp8"] = ToDbNull(priceRecovery.SellPrice8);
                row["sq8"] = ToDbNull(priceRecovery.SellQuantity8);
                row["sp8_noo"] = ToDbNull((long)priceRecovery.SellPrice8_NOO);
                row["sp8_mdey"] = ToDbNull(priceRecovery.SellPrice8_MDEY);
                row["sp8_mdemms"] = ToDbNull(priceRecovery.SellPrice8_MDEMMS);
                row["sp8_mdepno"] = DBNull.Value;

                // Cấp 9
                row["bp9"] = ToDbNull(priceRecovery.BuyPrice9);
                row["bq9"] = ToDbNull(priceRecovery.BuyQuantity9);
                row["bp9_noo"] = ToDbNull((long)priceRecovery.BuyPrice9_NOO);
                row["bp9_mdey"] = ToDbNull(priceRecovery.BuyPrice9_MDEY);
                row["bp9_mdemms"] = ToDbNull(priceRecovery.BuyPrice9_MDEMMS);
                row["bp9_mdepno"] = DBNull.Value;
                row["sp9"] = ToDbNull(priceRecovery.SellPrice9);
                row["sq9"] = ToDbNull(priceRecovery.SellQuantity9);
                row["sp9_noo"] = ToDbNull((long)priceRecovery.SellPrice9_NOO);
                row["sp9_mdey"] = ToDbNull(priceRecovery.SellPrice9_MDEY);
                row["sp9_mdemms"] = ToDbNull(priceRecovery.SellPrice9_MDEMMS);
                row["sp9_mdepno"] = DBNull.Value;

                // Cấp 10
                row["bp10"] = ToDbNull(priceRecovery.BuyPrice10);
                row["bq10"] = ToDbNull(priceRecovery.BuyQuantity10);
                row["bp10_noo"] = ToDbNull((long)priceRecovery.BuyPrice10_NOO);
                row["bp10_mdey"] = ToDbNull(priceRecovery.BuyPrice10_MDEY);
                row["bp10_mdemms"] = ToDbNull(priceRecovery.BuyPrice10_MDEMMS);
                row["bp10_mdepno"] = DBNull.Value;
                row["sp10"] = ToDbNull(priceRecovery.SellPrice10);
                row["sq10"] = ToDbNull(priceRecovery.SellQuantity10);
                row["sp10_noo"] = ToDbNull((long)priceRecovery.SellPrice10_NOO);
                row["sp10_mdey"] = ToDbNull(priceRecovery.SellPrice10_MDEY);
                row["sp10_mdemms"] = ToDbNull(priceRecovery.SellPrice10_MDEMMS);
                row["sp10_mdepno"] = DBNull.Value;

                // Cột cuối
                row["checksum"] = ToDbNull(long.Parse(priceRecovery.CheckSum));
                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSecurityStatusDataTable(List<ESecurityStatus> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_f) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // Khớp với NUMBER(10,0)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long));
            dt.Columns.Add("sendingtime", typeof(DateTime));
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("tscprodgrpid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("boardevtid", typeof(string));
            dt.Columns.Add("sessopenclosecode", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("tradingsessionid", typeof(string));
            dt.Columns.Add("haltrsncode", typeof(long)); // Khớp với NUMBER(19,0)
            dt.Columns.Add("productid", typeof(string));
            dt.Columns.Add("checksum", typeof(long));
            dt.Columns.Add("createtime", typeof(DateTime));

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var status in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = status.BeginString;
                row["bodylength"] = (int)status.BodyLength;
                row["msgtype"] = status.MsgType;
                row["sendercompid"] = status.SenderCompID;
                row["targetcompid"] = status.TargetCompID;
                row["msgseqnum"] = ToDbNull(status.MsgSeqNum); // Gọi hàm static

                row["sendingtime"] = ParseDashDateTimeToDbNull(status.SendingTime);

                row["marketid"] = status.MarketID;
                row["tscprodgrpid"] = status.TscProdGrpId;
                row["boardid"] = status.BoardID;
                row["boardevtid"] = status.BoardEvtID;
                row["sessopenclosecode"] = status.SessOpenCloseCode;
                row["symbol"] = status.Symbol;
                row["tradingsessionid"] = status.TradingSessionID;
                // Chuyển đổi an toàn từ string sang long
                if (long.TryParse(status.HaltRsnCode, out long parsedHaltRsnCode))
                {
                    // Nếu chuyển đổi thành công, dùng hàm ToDbNull(long) để kiểm tra magic number
                    row["haltrsncode"] = ToDbNull(parsedHaltRsnCode);
                }
                else
                {
                    // Nếu string là null, rỗng, hoặc không phải là số, gán DBNull.Value
                    row["haltrsncode"] = DBNull.Value;
                }
                row["productid"] = status.ProductID;

                // Giả định CheckSum là string, giống EPrice
                row["checksum"] = ToDbNull(long.Parse(status.CheckSum));
                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateIndexDataTable(List<EIndex> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m1) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // Khớp với NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("tradingsessionid", typeof(string));
            dt.Columns.Add("marketindexclass", typeof(string));
            dt.Columns.Add("indexstypecode", typeof(string));
            dt.Columns.Add("currency", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("transdate", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("valueindexes", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("totalvolumetraded", typeof(long)); // NUMBER(19)
            dt.Columns.Add("grosstradeamt", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("contauctacctrdvol", typeof(long)); // NUMBER(19)
            dt.Columns.Add("contauctacctrdval", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("blktrdacctrdvol", typeof(long)); // NUMBER(19)
            dt.Columns.Add("blktrdacctrdval", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("fluctuationupperlimitic", typeof(int)); // NUMBER(10)
            dt.Columns.Add("fluctuationupic", typeof(int)); // NUMBER(10)
            dt.Columns.Add("fluctuationsteadinessic", typeof(int)); // NUMBER(10)
            dt.Columns.Add("fluctuationdownic", typeof(int)); // NUMBER(10)
            dt.Columns.Add("fluctuationlowerlimitic", typeof(int)); // NUMBER(10)
            dt.Columns.Add("fluctuationupiv", typeof(long)); // NUMBER(19)
            dt.Columns.Add("fluctuationdowniv", typeof(long)); // NUMBER(19)
            dt.Columns.Add("fluctuationsteadinessiv", typeof(long)); // NUMBER(19)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var index in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = index.BeginString;
                row["bodylength"] = (int)index.BodyLength;
                row["msgtype"] = index.MsgType;
                row["sendercompid"] = index.SenderCompID;
                row["targetcompid"] = index.TargetCompID;
                row["msgseqnum"] = ToDbNull(index.MsgSeqNum); // Gọi static ToDbNull(long)

                if (DateTime.TryParseExact(index.SendingTime, "yyyy-MM-dd HH:mm:ss.fff",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime sendingTime))
                {
                    row["sendingtime"] = sendingTime;
                }
                else
                {
                    row["sendingtime"] = DBNull.Value;
                }

                row["marketid"] = index.MarketID;
                row["tradingsessionid"] = index.TradingSessionID;
                row["marketindexclass"] = index.MarketIndexClass;
                row["indexstypecode"] = index.IndexsTypeCode;
                row["currency"] = index.Currency;
                row["transacttime"] = index.TransactTime;

                row["transdate"] = ParseDayMonYearToDbNull(index.TransDate);

                row["valueindexes"] = ToDbNull(index.ValueIndexes); 
                row["totalvolumetraded"] = ToDbNull(index.TotalVolumeTraded); 
                row["grosstradeamt"] = ToDbNull(index.GrossTradeAmt); 
                row["contauctacctrdvol"] = ToDbNull(index.ContauctAccTrdvol); 
                row["contauctacctrdval"] = ToDbNull(index.ContauctAccTrdval); 
                row["blktrdacctrdvol"] = ToDbNull(index.BlktrdAccTrdvol); 
                row["blktrdacctrdval"] = ToDbNull(index.BlktrdAccTrdval); 

                // Xử lý các cột NUMBER(10) (dùng int)
                row["fluctuationupperlimitic"] = ToDbNull(index.FluctuationUpperLimitIssueCount);
                row["fluctuationupic"] = ToDbNull(index.FluctuationUpIssueCount);
                row["fluctuationsteadinessic"] = ToDbNull(index.FluctuationSteadinessIssueCount);
                row["fluctuationdownic"] = ToDbNull(index.FluctuationDownIssueCount);
                row["fluctuationlowerlimitic"] = ToDbNull(index.FluctuationLowerLimitIssueCount);

                // Xử lý các cột NUMBER(19) (dùng long)
                row["fluctuationupiv"] = ToDbNull(index.FluctuationUpIssueVolume); 
                row["fluctuationdowniv"] = ToDbNull(index.FluctuationDownIssueVolume); 
                row["fluctuationsteadinessiv"] = ToDbNull(index.FluctuationSteadinessIssueVolume);

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(index.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateInvestorPerIndustryDataTable(List<EInvestorPerIndustry> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m2) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("marketindexclass", typeof(string));
            dt.Columns.Add("indexstypecode", typeof(string));
            dt.Columns.Add("currency", typeof(string));
            dt.Columns.Add("investcode", typeof(string));
            dt.Columns.Add("sellvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("selltradeamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("buyvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("buytradedamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("bondclassc", typeof(string));
            dt.Columns.Add("securitygid", typeof(string));
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = ToDbNull(msg.MsgSeqNum); // Gọi static ToDbNull(long)

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["transacttime"] = (object)msg.TransactTime ?? DBNull.Value;
                row["marketindexclass"] = (object)msg.MarketIndexClass ?? DBNull.Value;
                row["indexstypecode"] = (object)msg.IndexsTypeCode ?? DBNull.Value;
                row["currency"] = (object)msg.Currency ?? DBNull.Value;
                row["investcode"] = (object)msg.InvestCode ?? DBNull.Value;

                // Dùng static helper cho magic number
                row["sellvolume"] = ToDbNull(msg.SellVolume); 
                row["selltradeamount"] = ToDbNull(msg.SellTradeAmount);
                row["buyvolume"] = ToDbNull(msg.BuyVolume); 
                row["buytradedamount"] = ToDbNull(msg.BuyTradedAmount); 

                row["bondclassc"] = (object)msg.BondClassificationCode ?? DBNull.Value;
                row["securitygid"] = (object)msg.SecurityGroupID ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateInvestorPerSymbolDataTable(List<EInvestorPerSymbol> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m3) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("investcode", typeof(string));
            dt.Columns.Add("sellvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("selltradeamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("buyvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("buytradedamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = ToDbNull(msg.MsgSeqNum);

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["investcode"] = (object)msg.InvestCode ?? DBNull.Value;

                // Dùng static helper cho magic number
                row["sellvolume"] = ToDbNull(msg.SellVolume); 
                row["selltradeamount"] = ToDbNull(msg.SellTradeAmount); 
                row["buyvolume"] = ToDbNull(msg.BuyVolume); 
                row["buytradedamount"] = ToDbNull(msg.BuyTradedAmount); 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNMembersPerSymbolDataTable(List<ETopNMembersPerSymbol> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m4) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sellrankseq", typeof(int)); // NUMBER(10)
            dt.Columns.Add("sellmemberno", typeof(string));
            dt.Columns.Add("sellvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("selltradeamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("buyrankseq", typeof(int)); // NUMBER(10)
            dt.Columns.Add("buymemberno", typeof(string));
            dt.Columns.Add("buyvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("buytradedamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["totnumreports"] = msg.TotNumReports; 

                row["sellrankseq"] = msg.SellRankSeq;
                row["sellmemberno"] = (object)msg.SellMemberNo ?? DBNull.Value;
                row["sellvolume"] = msg.SellVolume; 
                row["selltradeamount"] = msg.SellTradeAmount; 

                row["buyrankseq"] = msg.BuyRankSeq;
                row["buymemberno"] = (object)msg.BuyMemberNo ?? DBNull.Value;
                row["buyvolume"] = msg.BuyVolume; 
                row["buytradedamount"] = msg.BuyTradedAmount; 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSecurityInfoNotificationDataTable(List<ESecurityInformationNotification> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m7) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("referenceprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("highlimitprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("lowlimitprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("evaluationprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("hgstorderprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("lwstorderprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("listedshares", typeof(long)); // NUMBER(19)
            dt.Columns.Add("exclasstype", typeof(string));
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"] = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["referenceprice"]  = msg.ReferencePrice;
                row["highlimitprice"]  = msg.HighLimitPrice;
                row["lowlimitprice"]   = msg.LowLimitPrice;
                row["evaluationprice"] = msg.EvaluationPrice;
                row["hgstorderprice"]  = msg.HgstOrderPrice;
                row["lwstorderprice"]  = msg.LwstOrderPrice;
                row["listedshares"]    = msg.ListedShares; 
                row["exclasstype"] = (object)msg.ExClassType ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSymbolClosingInfoDataTable(List<ESymbolClosingInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m8) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("symbolcloseinfopx", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("symbolcloseinfoyield", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("symbolcloseinfopxtype", typeof(string));
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum;

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"] = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["symbolcloseinfopx"] = ToDbNull(msg.SymbolCloseInfoPx);
                row["symbolcloseinfoyield"] = ToDbNull(msg.SymbolCloseInfoYield);

                row["symbolcloseinfopxtype"] = (object)msg.SymbolCloseInfoPxType ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateOpenInterestDataTable(List<EOpenInterest> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ma) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("tradedate", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("openinterestqty", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("settlementprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["tradedate"] = ParseDayMonYearToDbNull(msg.TradeDate);
      
                row["openinterestqty"] = ToDbNull(msg.OpenInterestQty);
                row["settlementprice"] = DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateVolatilityInterruptionDataTable(List<EVolatilityInterruption> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_md) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("vitypecode", typeof(string));
            dt.Columns.Add("vikindcode", typeof(string));
            dt.Columns.Add("staticvibaseprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("dynamicvibaseprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("viprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("staticvidispartiyratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("dynamicvidispartiyratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = ToDbNull(msg.MsgSeqNum); 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"] = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["vitypecode"] = (object)msg.VITypeCode ?? DBNull.Value;
                row["vikindcode"] = (object)msg.VIKindCode ?? DBNull.Value;

                row["staticvibaseprice"] = msg.StaticVIBasePrice;
                row["dynamicvibaseprice"] = msg.DynamicVIBasePrice;
                row["viprice"] = msg.VIPrice;
                row["staticvidispartiyratio"] = msg.StaticVIDispartiyRatio;
                row["dynamicvidispartiyratio"] = msg.DynamicVIDispartiyRatio;

                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateDeemTradePriceDataTable(List<EDeemTradePrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_me) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("expectedtradepx", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("expectedtradeqty", typeof(long)); // NUMBER(19)
            dt.Columns.Add("expectedtradeyield", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum;

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"] = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["expectedtradepx"]    = msg.ExpectedTradePx;
                row["expectedtradeqty"]   = msg.ExpectedTradeQty; 
                row["expectedtradeyield"] = msg.ExpectedTradeYield;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateForeignerOrderLimitDataTable(List<EForeignerOrderLimit> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mf) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("foreignerbuyposblqty", typeof(long)); // NUMBER(19)
            dt.Columns.Add("foreignerorderlimitqty", typeof(long)); // NUMBER(19)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["foreignerbuyposblqty"] = msg.ForeignerBuyPosblQty;
                row["foreignerorderlimitqty"] = msg.ForeignerOrderLimitQty;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateMarketMakerInfoDataTable(List<EMarketMakerInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mh) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("marketmakercontractcode", typeof(string));
            dt.Columns.Add("memberno", typeof(string));
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["marketmakercontractcode"] = (object)msg.MarketMakerContractCode ?? DBNull.Value;
                row["memberno"] = (object)msg.MemberNo ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSymbolEventDataTable(List<ESymbolEvent> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mi) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("eventkindcode", typeof(string));
            dt.Columns.Add("eventoccurrencereasoncode", typeof(string));
            dt.Columns.Add("eventstartdate", typeof(string)); // VARCHAR2(20)
            dt.Columns.Add("eventenddate", typeof(string)); // VARCHAR2(20)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["eventkindcode"] = (object)msg.EventKindCode ?? DBNull.Value;
                row["eventoccurrencereasoncode"] = (object)msg.EventOccurrenceReasonCode ?? DBNull.Value;

                // Các cột này là VARCHAR2 trong DB, nên gán thẳng chuỗi
                row["eventstartdate"] = (object)msg.EventStartDate ?? DBNull.Value;
                row["eventenddate"] = (object)msg.EventEndDate ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateDrvProductEventDataTable(List<EDrvProductEvent> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mj) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("productid", typeof(string));
            dt.Columns.Add("eventkindcode", typeof(string));
            dt.Columns.Add("eventoccurrencereasoncode", typeof(string));
            dt.Columns.Add("eventstartdate", typeof(string)); // VARCHAR2(20)
            dt.Columns.Add("eventenddate", typeof(string)); // VARCHAR2(20)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = ToDbNull(msg.MsgSeqNum); 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["productid"] = (object)msg.ProductID ?? DBNull.Value;
                row["eventkindcode"] = (object)msg.EventKindCode ?? DBNull.Value;
                row["eventoccurrencereasoncode"] = (object)msg.EventOccurrenceReasonCode ?? DBNull.Value;

                // Các cột này là VARCHAR2 trong DB, nên gán thẳng chuỗi
                row["eventstartdate"] = (object)msg.EventStartDate ?? DBNull.Value;
                row["eventenddate"] = (object)msg.EventEndDate ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateIndexConstituentsDataTable(List<EIndexConstituentsInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ml) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("marketindexclass", typeof(string));
            dt.Columns.Add("indexstypecode", typeof(string));
            dt.Columns.Add("currency", typeof(string));
            dt.Columns.Add("idxname", typeof(string)); // Khớp với CLOB
            dt.Columns.Add("idxenglishname", typeof(string)); // Khớp với CLOB
            dt.Columns.Add("totalmsgno", typeof(long)); // NUMBER(19)
            dt.Columns.Add("currentmsgno", typeof(long)); // NUMBER(19)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["marketindexclass"] = (object)msg.MarketIndexClass ?? DBNull.Value;
                row["indexstypecode"] = (object)msg.IndexsTypeCode ?? DBNull.Value;
                row["currency"] = (object)msg.Currency ?? DBNull.Value;

                row["idxname"] = (object)msg.IdxName ?? DBNull.Value;
                row["idxenglishname"] = (object)msg.IdxEnglishName ?? DBNull.Value;

                row["totalmsgno"] = msg.TotalMsgNo; 
                row["currentmsgno"] = msg.CurrentMsgNo; 

                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFiNavDataTable(List<EETFiNav> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mm) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("inavvalue", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["transacttime"] = (object)msg.TransactTime ?? DBNull.Value;
                row["inavvalue"] = msg.iNAVvalue;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFiIndexDataTable(List<EETFiIndex> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mn) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("valuesindexes", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = ToDbNull(msg.MsgSeqNum); // Gọi static ToDbNull(long)

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["transacttime"] = (object)msg.TransactTime ?? DBNull.Value;
                row["valuesindexes"] = msg.ValueIndexes;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); // Gọi static ToDbNull(long)
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFTrackingErrorDataTable(List<EETFTrackingError> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mo) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("tradedate", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("trackingerror", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("disparateratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum;

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["tradedate"] = ParseDayMonYearToDbNull(msg.TradeDate);
                row["trackingerror"] = msg.TrackingError;
                row["disparateratio"] = msg.DisparateRatio;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); // Gọi static ToDbNull(long)
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithTradingQuantityDataTable(List<ETopNSymbolsWithTradingQuantity> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mp) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long)); // NUMBER(19)
            dt.Columns.Add("rank", typeof(int)); // NUMBER(10)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("mdentrysize", typeof(long)); // NUMBER(19)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["totnumreports"] = msg.TotNumReports; 
                row["rank"] = msg.Rank;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;

                row["mdentrysize"] = msg.MDEntrySize;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); // Gọi static ToDbNull(long)
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithCurrentPriceDataTable(List<ETopNSymbolsWithCurrentPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mq) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long)); // NUMBER(19)
            dt.Columns.Add("rank", typeof(int)); // NUMBER(10)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("mdentrypx", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum;

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["totnumreports"] = msg.TotNumReports; 
                row["rank"] = msg.Rank;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["mdentrypx"] = msg.MDEntryPx; 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithHighRatioOfPriceDataTable(List<ETopNSymbolsWithHighRatioOfPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mr) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long)); // NUMBER(19)
            dt.Columns.Add("rank", typeof(int)); // NUMBER(10)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("pricefluctuationratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["totnumreports"] = msg.TotNumReports; 
                row["rank"] = msg.Rank;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["pricefluctuationratio"] = msg.PriceFluctuationRatio; 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); // Gọi static ToDbNull(long)
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithLowRatioOfPriceDataTable(List<ETopNSymbolsWithLowRatioOfPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ms) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("totnumreports", typeof(long)); // NUMBER(19)
            dt.Columns.Add("rank", typeof(int)); // NUMBER(10)
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("pricefluctuationratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum;
                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);
                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["totnumreports"] = msg.TotNumReports;
                row["rank"] = msg.Rank;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["pricefluctuationratio"] = msg.PriceFluctuationRatio; 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTradingResultOfForeignInvestorsDataTable(List<ETradingResultOfForeignInvestors> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mt) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("tradingsessionid", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("forninvesttypecode", typeof(string));
            dt.Columns.Add("sellvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("selltradeamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("buyvolume", typeof(long)); // NUMBER(19)
            dt.Columns.Add("buytradedamount", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("sellvolumetotal", typeof(long)); // NUMBER(19)
            dt.Columns.Add("selltradeamounttotal", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("buyvolumetotal", typeof(long)); // NUMBER(19)
            dt.Columns.Add("buytradedamounttotal", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 

                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"] = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["tradingsessionid"] = (object)msg.TradingSessionID ?? DBNull.Value;
                row["transacttime"] = (object)msg.TransactTime ?? DBNull.Value;
                row["forninvesttypecode"] = (object)msg.FornInvestTypeCode ?? DBNull.Value;

                // Dùng static helper cho magic number
                row["sellvolume"] = msg.SellVolume; 
                row["selltradeamount"] = msg.SellTradeAmount; 
                row["buyvolume"] = msg.BuyVolume; 
                row["buytradedamount"] = msg.BuyTradedAmount; 
                row["sellvolumetotal"] = msg.SellVolumeTotal; 
                row["selltradeamounttotal"] = msg.SellTradeAmountTotal; 
                row["buyvolumetotal"] = msg.BuyVolumeTotal; 
                row["buytradedamounttotal"] = msg.BuyTradeAmountTotal;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateDisclosureDataTable(List<EDisclosure> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mu) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("securityexchange", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("symbolname", typeof(string)); // CLOB
            dt.Columns.Add("disclosureid", typeof(string));
            dt.Columns.Add("totalmsgno", typeof(long)); // NUMBER(19)
            dt.Columns.Add("currentmsgno", typeof(long)); // NUMBER(19)
            dt.Columns.Add("lanquagecategory", typeof(string)); // Khớp với schema
            dt.Columns.Add("datacategory", typeof(string));
            dt.Columns.Add("publicinformationdate", typeof(string)); // VARCHAR2(10)
            dt.Columns.Add("transmissiondate", typeof(string)); // VARCHAR2(10)
            dt.Columns.Add("processtype", typeof(string));
            dt.Columns.Add("headline", typeof(string)); // CLOB
            dt.Columns.Add("body", typeof(string)); // CLOB
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"] = (int)msg.BodyLength;
                row["msgtype"] = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"] = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"] = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"] = msg.MsgSeqNum; 
                row["sendingtime"] = ParseDashDateTimeToDbNull(msg.SendingTime);

                row["marketid"] = (object)msg.MarketID ?? DBNull.Value;
                row["securityexchange"] = (object)msg.SecurityExchange ?? DBNull.Value;
                row["symbol"] = (object)msg.Symbol ?? DBNull.Value;
                row["symbolname"] = (object)msg.SymbolName ?? DBNull.Value;
                row["disclosureid"] = (object)msg.DisclosureID ?? DBNull.Value;
                row["totalmsgno"] = msg.TotalMsgNo; 
                row["currentmsgno"] = msg.CurrentMsgNo; 
                row["lanquagecategory"] = (object)msg.LanquageCategory ?? DBNull.Value;
                row["datacategory"] = (object)msg.DataCategory ?? DBNull.Value;
                row["publicinformationdate"] = (object)msg.PublicInformationDate ?? DBNull.Value;
                row["transmissiondate"] = (object)msg.TransmissionDate ?? DBNull.Value;
                row["processtype"] = (object)msg.ProcessType ?? DBNull.Value;
                row["headline"] = (object)msg.Headline ?? DBNull.Value; 
                row["body"] = (object)msg.Body ?? DBNull.Value; 

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateRandomEndDataTable(List<ERandomEnd> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mw) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("transacttime", typeof(string));
            dt.Columns.Add("reapplyclassification", typeof(string));
            dt.Columns.Add("retentativeexecutionprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("reestimatedhighestprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("reehighestpricedisparater", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("reestimatedlowestprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("reelowestpricedisparater", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("latestprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("latestpricedisparateratio", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("randomendreleasetime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"]               = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"]                = (int)msg.BodyLength;
                row["msgtype"]                   = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"]              = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"]              = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"]                 = msg.MsgSeqNum; 
                row["sendingtime"]               = ParseDashDateTimeToDbNull(msg.SendingTime);
                row["marketid"]                  = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"]                   = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"]                    = (object)msg.Symbol ?? DBNull.Value;
                row["transacttime"]              = (object)msg.TransactTime ?? DBNull.Value;
                row["reapplyclassification"]     = (object)msg.RandomEndApplyClassification ?? DBNull.Value;
                row["retentativeexecutionprice"] = msg.RandomEndTentativeExecutionPrice;
                row["reestimatedhighestprice"]   = msg.RandomEndEstimatedHighestPrice;
                row["reehighestpricedisparater"] = msg.RandomEndEstimatedHighestPriceDisparateRatio;
                row["reestimatedlowestprice"]    = msg.RandomEndEstimatedLowestPrice;
                row["reelowestpricedisparater"]  = msg.RandomEndEstimatedLowestPriceDisparateRatio;
                row["latestprice"]               = msg.LatestPrice;
                row["latestpricedisparateratio"] = msg.LatestPriceDisparateRatio;

                // Xử lý randomendreleasetime (TIMESTAMP(3))
                // Giả định nó là string "yyyyMMdd HH:mm:ss.fff" giống SendingTime
                if (DateTime.TryParseExact(msg.RandomEndReleaseTimes, "yyyyMMdd HH:mm:ss.fff",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime releaseTime))
                {
                    row["randomendreleasetime"] = releaseTime;
                }
                else
                {
                    row["randomendreleasetime"] = DBNull.Value;
                }

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); 
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreatePriceLimitExpansionDataTable(List<EPriceLimitExpansion> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mx) ---
            // Tên cột BẮT BUỘC là lowercase
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // NUMBER(10)
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("sendingtime", typeof(DateTime)); // TIMESTAMP(3)
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));
            dt.Columns.Add("highlimitprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("lowlimitprice", typeof(decimal)); // NUMBER(22,4)
            dt.Columns.Add("pleuplmtstep", typeof(int)); // NUMBER(10)
            dt.Columns.Add("plelwlmtstep", typeof(int)); // NUMBER(10)
            dt.Columns.Add("checksum", typeof(long)); // NUMBER(19)
            dt.Columns.Add("createtime", typeof(DateTime)); // TIMESTAMP(3)

            // --- 2. LOGIC HELPER ---
            // (Bỏ trống - Giả định bạn đã chuyển ToDbNull ra static)

            // --- 3. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                row["beginstring"]    = (object)msg.BeginString ?? DBNull.Value;
                row["bodylength"]     = (int)msg.BodyLength;
                row["msgtype"]        = (object)msg.MsgType ?? DBNull.Value;
                row["sendercompid"]   = (object)msg.SenderCompID ?? DBNull.Value;
                row["targetcompid"]   = (object)msg.TargetCompID ?? DBNull.Value;
                row["msgseqnum"]      = msg.MsgSeqNum; 
                row["sendingtime"]    = ParseDashDateTimeToDbNull(msg.SendingTime);
                row["marketid"]       = (object)msg.MarketID ?? DBNull.Value;
                row["boardid"]        = (object)msg.BoardID ?? DBNull.Value;
                row["symbol"]         = (object)msg.Symbol ?? DBNull.Value;
                row["highlimitprice"] = msg.HighLimitPrice; 
                row["lowlimitprice"]  = msg.LowLimitPrice;
                row["pleuplmtstep"]   = msg.PleUpLmtStep;
                row["plelwlmtstep"]   = msg.PleLwLmtStep;

                // Xử lý CheckSum (giả định là string, parse an toàn)
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row["checksum"] = ToDbNull(parsedCheckSum); // Gọi static ToDbNull(long)
                }
                else
                {
                    row["checksum"] = DBNull.Value;
                }

                row["createtime"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        // --- CÁC HÀM HELPER TỐI ƯU ---
        /// <summary>
        /// Chuyển giá trị long "magic number" thành DBNull.Value.
        /// </summary>
        private static object ToDbNull(long val)
        {
            const long InvalidLong = -9999999L;
            return val != InvalidLong ? (object)val : DBNull.Value;
        }

        /// <summary>
        /// Chuyển giá trị double "magic number" thành DBNull.Value
        /// và chuyển đổi giá trị hợp lệ sang decimal.
        /// </summary>
        private static object ToDbNull(double val)
        {
            const double InvalidDouble = -9999999.0d;

            // So sánh double an toàn
            if (Math.Abs(val - InvalidDouble) < 0.0001d)
            {
                return DBNull.Value;
            }

            // Giữ nguyên logic chuyển đổi sang decimal cho DataTable
            return (object)Convert.ToDecimal(val);
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
    }
}
