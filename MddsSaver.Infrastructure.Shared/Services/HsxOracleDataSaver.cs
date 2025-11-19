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

            // 1. ĐỊNH NGHĨA CỘT (DATATABLE) - Sử dụng hằng số const
            // Tên cột đã được CHUYỂN SANG LOWERCASE để khớp với schema 'msg_d'

            // Header (Từ Base)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (Từ MsgDSchema)
            dt.Columns.Add(MsgDSchema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgDSchema.SecurityExchange, typeof(string));
            dt.Columns.Add(MsgDSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgDSchema.TickerCode, typeof(string));
            dt.Columns.Add(MsgDSchema.SymbolShortCode, typeof(string));
            dt.Columns.Add(MsgDSchema.SymbolName, typeof(string));
            dt.Columns.Add(MsgDSchema.SymbolEnName, typeof(string));
            dt.Columns.Add(MsgDSchema.ProductId, typeof(string));
            dt.Columns.Add(MsgDSchema.ProductGrpId, typeof(string));
            dt.Columns.Add(MsgDSchema.SecurityGroupId, typeof(string));
            dt.Columns.Add(MsgDSchema.PutOrCall, typeof(string));
            dt.Columns.Add(MsgDSchema.ExerciseStyle, typeof(string));
            dt.Columns.Add(MsgDSchema.MaturityMonthYear, typeof(string));
            dt.Columns.Add(MsgDSchema.MaturityDate, typeof(string));
            dt.Columns.Add(MsgDSchema.Issuer, typeof(string));
            dt.Columns.Add(MsgDSchema.IssueDate, typeof(string));
            dt.Columns.Add(MsgDSchema.ContractMultiplier, typeof(decimal));
            dt.Columns.Add(MsgDSchema.CouponRate, typeof(decimal));
            dt.Columns.Add(MsgDSchema.Currency, typeof(string));
            dt.Columns.Add(MsgDSchema.ListedShares, typeof(long));
            dt.Columns.Add(MsgDSchema.HighLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.LowLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.StrikePrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.SecurityStatus, typeof(string));
            dt.Columns.Add(MsgDSchema.ContractSize, typeof(decimal));
            dt.Columns.Add(MsgDSchema.SettlMethod, typeof(string));
            dt.Columns.Add(MsgDSchema.Yield, typeof(decimal));
            dt.Columns.Add(MsgDSchema.ReferencePrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.EvaluationPrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.HgstOrderPrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.LwstOrderPrice, typeof(decimal));
            dt.Columns.Add(MsgDSchema.PrevClosePx, typeof(decimal));
            dt.Columns.Add(MsgDSchema.SymbolCloseInfoPxType, typeof(string));
            dt.Columns.Add(MsgDSchema.FirstTradingDate, typeof(string));
            dt.Columns.Add(MsgDSchema.FinalTradeDate, typeof(string));
            dt.Columns.Add(MsgDSchema.FinalSettleDate, typeof(string));
            dt.Columns.Add(MsgDSchema.ListingDate, typeof(string));
            dt.Columns.Add(MsgDSchema.ReTriggeringConditionCode, typeof(string));
            dt.Columns.Add(MsgDSchema.ExClassType, typeof(string));
            dt.Columns.Add(MsgDSchema.VWap, typeof(decimal));
            dt.Columns.Add(MsgDSchema.SymbolAdminStatusCode, typeof(string));
            dt.Columns.Add(MsgDSchema.SymbolTradingMethodSc, typeof(string));
            dt.Columns.Add(MsgDSchema.SymbolTradingSantionSc, typeof(string));
            dt.Columns.Add(MsgDSchema.SectorTypeCode, typeof(string));
            dt.Columns.Add(MsgDSchema.RedumptionDate, typeof(string));

            // Footer (Từ Base)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // 2. ĐỔ DỮ LIỆU TỪ LIST VÀO DATATABLE
            foreach (var def in definitions)
            {
                var row = dt.NewRow();

                row[BaseMessageSchema.BeginString]        = (object)def.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]         = (int)def.BodyLength;
                row[BaseMessageSchema.MsgType]            = (object)def.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]       = (object)def.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]       = (object)def.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]          = def.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]        = ParseDashDateTimeToDbNull(def.SendingTime);

                row[BaseMessageSchema.MarketId]           = (object)def.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]            = (object)def.BoardID ?? DBNull.Value;
                row[MsgDSchema.TotNumReports]             = def.TotNumReports; 
                row[MsgDSchema.SecurityExchange]          = (object)def.SecurityExchange ?? DBNull.Value;
                row[MsgDSchema.Symbol]                    = (object)def.Symbol ?? DBNull.Value;
                row[MsgDSchema.TickerCode]                = (object)def.TickerCode ?? DBNull.Value;
                row[MsgDSchema.SymbolShortCode]           = (object)def.SymbolShortCode ?? DBNull.Value;
                row[MsgDSchema.SymbolName]                = (object)def.SymbolName ?? DBNull.Value;
                row[MsgDSchema.SymbolEnName]              = (object)def.SymbolEnglishName ?? DBNull.Value;
                row[MsgDSchema.ProductId]                 = (object)def.ProductID ?? DBNull.Value;
                row[MsgDSchema.ProductGrpId]              = (object)def.ProductGrpID ?? DBNull.Value;
                row[MsgDSchema.SecurityGroupId]           = (object)def.SecurityGroupID ?? DBNull.Value;
                row[MsgDSchema.PutOrCall]                 = (object)def.PutOrCall ?? DBNull.Value;
                row[MsgDSchema.ExerciseStyle]             = (object)def.ExerciseStyle ?? DBNull.Value;
                row[MsgDSchema.MaturityMonthYear]         = (object)def.MaturityMonthYear ?? DBNull.Value;
                row[MsgDSchema.MaturityDate]              = (object)def.MaturityDate ?? DBNull.Value;
                row[MsgDSchema.Issuer]                    = (object)def.Issuer ?? DBNull.Value;
                row[MsgDSchema.IssueDate]                 = (object)def.IssueDate ?? DBNull.Value;
                row[MsgDSchema.ContractMultiplier]        = (object)def.ContractMultiplier ?? DBNull.Value;
                row[MsgDSchema.CouponRate]                = (object)def.CouponRate ?? DBNull.Value;
                row[MsgDSchema.Currency]                  = (object)def.Currency ?? DBNull.Value;
                row[MsgDSchema.ListedShares]              = def.ListedShares; // Giả định non-null
                row[MsgDSchema.HighLimitPrice]            = (object)def.HighLimitPrice ?? DBNull.Value;
                row[MsgDSchema.LowLimitPrice]             = (object)def.LowLimitPrice ?? DBNull.Value;
                row[MsgDSchema.StrikePrice]               = (object)def.StrikePrice ?? DBNull.Value;
                row[MsgDSchema.SecurityStatus]            = (object)def.SecurityStatus ?? DBNull.Value;
                row[MsgDSchema.ContractSize]              = (object)def.ContractSize ?? DBNull.Value;
                row[MsgDSchema.SettlMethod]               = (object)def.SettlMethod ?? DBNull.Value;
                row[MsgDSchema.Yield]                     = (object)def.Yield ?? DBNull.Value;
                row[MsgDSchema.ReferencePrice]            = (object)def.ReferencePrice ?? DBNull.Value;
                row[MsgDSchema.EvaluationPrice]           = (object)def.EvaluationPrice ?? DBNull.Value;
                row[MsgDSchema.HgstOrderPrice]            = (object)def.HgstOrderPrice ?? DBNull.Value;
                row[MsgDSchema.LwstOrderPrice]            = (object)def.LwstOrderPrice ?? DBNull.Value;
                row[MsgDSchema.PrevClosePx]               = (object)def.PrevClosePx ?? DBNull.Value;
                row[MsgDSchema.SymbolCloseInfoPxType]     = (object)def.SymbolCloseInfoPxType ?? DBNull.Value;
                row[MsgDSchema.FirstTradingDate]          = (object)def.FirstTradingDate ?? DBNull.Value;
                row[MsgDSchema.FinalTradeDate]            = (object)def.FinalTradeDate ?? DBNull.Value;
                row[MsgDSchema.FinalSettleDate]           = (object)def.FinalSettleDate ?? DBNull.Value;
                row[MsgDSchema.ListingDate]               = (object)def.ListingDate ?? DBNull.Value;
                row[MsgDSchema.ReTriggeringConditionCode] = (object)def.RandomEndTriggeringConditionCode ?? DBNull.Value;
                row[MsgDSchema.ExClassType]               = (object)def.ExClassType ?? DBNull.Value;
                row[MsgDSchema.VWap]                      = (object)def.VWAP ?? DBNull.Value;
                row[MsgDSchema.SymbolAdminStatusCode]     = (object)def.SymbolAdminStatusCode ?? DBNull.Value;
                row[MsgDSchema.SymbolTradingMethodSc]     = (object)def.SymbolTradingMethodStatusCode ?? DBNull.Value;
                row[MsgDSchema.SymbolTradingSantionSc]    = (object)def.SymbolTradingSantionStatusCode ?? DBNull.Value;
                row[MsgDSchema.SectorTypeCode]            = (object)def.SectorTypeCode ?? DBNull.Value;
                row[MsgDSchema.RedumptionDate]            = (object)def.RedumptionDate ?? DBNull.Value;

                // Xử lý CheckSum
                if (long.TryParse(def.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = parsedCheckSum;
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreatePriceDataTable(List<EPrice> prices)
        {
            try
            {
                DataTable dt = new DataTable();

                // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_x, dùng const) ---

                // Thông tin header (từ Base)
                dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
                dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
                dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
                dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
                dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
                dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
                dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
                dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
                dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

                // Payload (từ MsgXSchema)
                dt.Columns.Add(MsgXSchema.TradingSessionId, typeof(string));
                dt.Columns.Add(MsgXSchema.Symbol, typeof(string));
                dt.Columns.Add(MsgXSchema.TradeDate, typeof(DateTime));
                dt.Columns.Add(MsgXSchema.TransactTime, typeof(string));

                // Các cột thống kê
                dt.Columns.Add(MsgXSchema.TotalVolumeTraded, typeof(long));
                dt.Columns.Add(MsgXSchema.GrossTradeAmt, typeof(decimal));
                dt.Columns.Add(MsgXSchema.SellTotOrderQty, typeof(long));
                dt.Columns.Add(MsgXSchema.BuyTotOrderQty, typeof(long));
                dt.Columns.Add(MsgXSchema.SellValidOrderCnt, typeof(long));
                dt.Columns.Add(MsgXSchema.BuyValidOrderCnt, typeof(long));

                // Thêm các cột giá 1-10 (Sử dụng prefix/suffix const)
                for (int i = 1; i <= 10; i++)
                {
                    // Buy
                    dt.Columns.Add($"{MsgXSchema.BpPrefix}{i}", typeof(decimal));
                    dt.Columns.Add($"{MsgXSchema.BqPrefix}{i}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Noo}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdey}", typeof(decimal));
                    dt.Columns.Add($"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdemms}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.BpPrefix}{i}{MsgXSchema.Suffix_Mdepno}", typeof(int));

                    // Sell
                    dt.Columns.Add($"{MsgXSchema.SpPrefix}{i}", typeof(decimal));
                    dt.Columns.Add($"{MsgXSchema.SqPrefix}{i}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Noo}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdey}", typeof(decimal));
                    dt.Columns.Add($"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdemms}", typeof(long));
                    dt.Columns.Add($"{MsgXSchema.SpPrefix}{i}{MsgXSchema.Suffix_Mdepno}", typeof(int));
                }

                // Các cột giá cuối
                dt.Columns.Add(MsgXSchema.Mp, typeof(decimal));
                dt.Columns.Add(MsgXSchema.Mq, typeof(long));
                dt.Columns.Add(MsgXSchema.Op, typeof(decimal));
                dt.Columns.Add(MsgXSchema.Lp, typeof(decimal));
                dt.Columns.Add(MsgXSchema.Hp, typeof(decimal));

                // Cột cuối (từ Base)
                dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
                dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

                // --- 2. ĐIỀN DỮ LIỆU ---
                foreach (var price in prices)
                {
                    DataRow row = dt.NewRow();

                    // Header
                    row[BaseMessageSchema.BeginString]                        = price.BeginString;
                    row[BaseMessageSchema.BodyLength]                         = (int)price.BodyLength;
                    row[BaseMessageSchema.MsgType]                            = price.MsgType;
                    row[BaseMessageSchema.SenderCompId]                       = price.SenderCompID;
                    row[BaseMessageSchema.TargetCompId]                       = price.TargetCompID;
                    row[BaseMessageSchema.MsgSeqNum]                          = price.MsgSeqNum;
                    row[BaseMessageSchema.SendingTime]                        = ParseCompactDateTimeToDbNull(price.SendingTime);
                    row[BaseMessageSchema.MarketId]                           = price.MarketID;
                    row[BaseMessageSchema.BoardId]                            = price.BoardID;

                    // Payload
                    row[MsgXSchema.TradingSessionId]                          = price.TradingSessionID;
                    row[MsgXSchema.Symbol]                                    = price.Symbol;
                    row[MsgXSchema.TradeDate]                                 = ParseCompactDateToDbNull(price.TradeDate);
                    row[MsgXSchema.TransactTime]                              = price.TransactTime;

                    // Các trường thống kê
                    row[MsgXSchema.TotalVolumeTraded]                         = ToDbNull(price.TotalVolumeTraded);
                    row[MsgXSchema.GrossTradeAmt]                             = ToDbNull(price.GrossTradeAmt);
                    row[MsgXSchema.SellTotOrderQty]                           = ToDbNull(price.SellTotOrderQty);
                    row[MsgXSchema.BuyTotOrderQty]                            = ToDbNull(price.BuyTotOrderQty);
                    row[MsgXSchema.SellValidOrderCnt]                         = ToDbNull(price.SellValidOrderCnt);
                    row[MsgXSchema.BuyValidOrderCnt]                          = ToDbNull(price.BuyValidOrderCnt);

                    // --- Cấp 1 ---
                    row[$"{MsgXSchema.BpPrefix}1"]                            = ToDbNull(price.BuyPrice1);
                    row[$"{MsgXSchema.BqPrefix}1"]                            = ToDbNull(price.BuyQuantity1);
                    row[$"{MsgXSchema.BpPrefix}1{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice1_NOO);
                    row[$"{MsgXSchema.BpPrefix}1{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice1_MDEY);
                    row[$"{MsgXSchema.BpPrefix}1{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice1_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}1{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}1"]                            = ToDbNull(price.SellPrice1);
                    row[$"{MsgXSchema.SqPrefix}1"]                            = ToDbNull(price.SellQuantity1);
                    row[$"{MsgXSchema.SpPrefix}1{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice1_NOO);
                    row[$"{MsgXSchema.SpPrefix}1{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice1_MDEY);
                    row[$"{MsgXSchema.SpPrefix}1{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice1_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}1{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 2 ---
                    row[$"{MsgXSchema.BpPrefix}2"]                            = ToDbNull(price.BuyPrice2);
                    row[$"{MsgXSchema.BqPrefix}2"]                            = ToDbNull(price.BuyQuantity2);
                    row[$"{MsgXSchema.BpPrefix}2{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice2_NOO);
                    row[$"{MsgXSchema.BpPrefix}2{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice2_MDEY);
                    row[$"{MsgXSchema.BpPrefix}2{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice2_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}2{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}2"]                            = ToDbNull(price.SellPrice2);
                    row[$"{MsgXSchema.SqPrefix}2"]                            = ToDbNull(price.SellQuantity2);
                    row[$"{MsgXSchema.SpPrefix}2{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice2_NOO);
                    row[$"{MsgXSchema.SpPrefix}2{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice2_MDEY);
                    row[$"{MsgXSchema.SpPrefix}2{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice2_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}2{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 3 ---
                    row[$"{MsgXSchema.BpPrefix}3"]                            = ToDbNull(price.BuyPrice3);
                    row[$"{MsgXSchema.BqPrefix}3"]                            = ToDbNull(price.BuyQuantity3);
                    row[$"{MsgXSchema.BpPrefix}3{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice3_NOO);
                    row[$"{MsgXSchema.BpPrefix}3{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice3_MDEY);
                    row[$"{MsgXSchema.BpPrefix}3{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice3_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}3{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}3"]                            = ToDbNull(price.SellPrice3);
                    row[$"{MsgXSchema.SqPrefix}3"]                            = ToDbNull(price.SellQuantity3);
                    row[$"{MsgXSchema.SpPrefix}3{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice3_NOO);
                    row[$"{MsgXSchema.SpPrefix}3{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice3_MDEY);
                    row[$"{MsgXSchema.SpPrefix}3{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice3_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}3{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 4 ---
                    row[$"{MsgXSchema.BpPrefix}4"]                            = ToDbNull(price.BuyPrice4);
                    row[$"{MsgXSchema.BqPrefix}4"]                            = ToDbNull(price.BuyQuantity4);
                    row[$"{MsgXSchema.BpPrefix}4{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice4_NOO);
                    row[$"{MsgXSchema.BpPrefix}4{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice4_MDEY);
                    row[$"{MsgXSchema.BpPrefix}4{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice4_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}4{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}4"]                            = ToDbNull(price.SellPrice4);
                    row[$"{MsgXSchema.SqPrefix}4"]                            = ToDbNull(price.SellQuantity4);
                    row[$"{MsgXSchema.SpPrefix}4{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice4_NOO);
                    row[$"{MsgXSchema.SpPrefix}4{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice4_MDEY);
                    row[$"{MsgXSchema.SpPrefix}4{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice4_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}4{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 5 ---
                    row[$"{MsgXSchema.BpPrefix}5"]                            = ToDbNull(price.BuyPrice5);
                    row[$"{MsgXSchema.BqPrefix}5"]                            = ToDbNull(price.BuyQuantity5);
                    row[$"{MsgXSchema.BpPrefix}5{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice5_NOO);
                    row[$"{MsgXSchema.BpPrefix}5{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice5_MDEY);
                    row[$"{MsgXSchema.BpPrefix}5{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice5_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}5{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}5"]                            = ToDbNull(price.SellPrice5);
                    row[$"{MsgXSchema.SqPrefix}5"]                            = ToDbNull(price.SellQuantity5);
                    row[$"{MsgXSchema.SpPrefix}5{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice5_NOO);
                    row[$"{MsgXSchema.SpPrefix}5{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice5_MDEY);
                    row[$"{MsgXSchema.SpPrefix}5{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice5_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}5{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 6 ---
                    row[$"{MsgXSchema.BpPrefix}6"]                            = ToDbNull(price.BuyPrice6);
                    row[$"{MsgXSchema.BqPrefix}6"]                            = ToDbNull(price.BuyQuantity6);
                    row[$"{MsgXSchema.BpPrefix}6{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice6_NOO);
                    row[$"{MsgXSchema.BpPrefix}6{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice6_MDEY);
                    row[$"{MsgXSchema.BpPrefix}6{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice6_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}6{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}6"]                            = ToDbNull(price.SellPrice6);
                    row[$"{MsgXSchema.SqPrefix}6"]                            = ToDbNull(price.SellQuantity6);
                    row[$"{MsgXSchema.SpPrefix}6{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice6_NOO);
                    row[$"{MsgXSchema.SpPrefix}6{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice6_MDEY);
                    row[$"{MsgXSchema.SpPrefix}6{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice6_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}6{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 7 ---
                    row[$"{MsgXSchema.BpPrefix}7"]                            = ToDbNull(price.BuyPrice7);
                    row[$"{MsgXSchema.BqPrefix}7"]                            = ToDbNull(price.BuyQuantity7);
                    row[$"{MsgXSchema.BpPrefix}7{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice7_NOO);
                    row[$"{MsgXSchema.BpPrefix}7{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice7_MDEY);
                    row[$"{MsgXSchema.BpPrefix}7{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice7_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}7{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}7"]                            = ToDbNull(price.SellPrice7);
                    row[$"{MsgXSchema.SqPrefix}7"]                            = ToDbNull(price.SellQuantity7);
                    row[$"{MsgXSchema.SpPrefix}7{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice7_NOO);
                    row[$"{MsgXSchema.SpPrefix}7{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice7_MDEY);
                    row[$"{MsgXSchema.SpPrefix}7{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice7_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}7{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 8 ---
                    row[$"{MsgXSchema.BpPrefix}8"]                            = ToDbNull(price.BuyPrice8);
                    row[$"{MsgXSchema.BqPrefix}8"]                            = ToDbNull(price.BuyQuantity8);
                    row[$"{MsgXSchema.BpPrefix}8{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice8_NOO);
                    row[$"{MsgXSchema.BpPrefix}8{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice8_MDEY);
                    row[$"{MsgXSchema.BpPrefix}8{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice8_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}8{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}8"]                            = ToDbNull(price.SellPrice8);
                    row[$"{MsgXSchema.SqPrefix}8"]                            = ToDbNull(price.SellQuantity8);
                    row[$"{MsgXSchema.SpPrefix}8{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice8_NOO);
                    row[$"{MsgXSchema.SpPrefix}8{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice8_MDEY);
                    row[$"{MsgXSchema.SpPrefix}8{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice8_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}8{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 9 ---
                    row[$"{MsgXSchema.BpPrefix}9"]                            = ToDbNull(price.BuyPrice9);
                    row[$"{MsgXSchema.BqPrefix}9"]                            = ToDbNull(price.BuyQuantity9);
                    row[$"{MsgXSchema.BpPrefix}9{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.BuyPrice9_NOO);
                    row[$"{MsgXSchema.BpPrefix}9{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.BuyPrice9_MDEY);
                    row[$"{MsgXSchema.BpPrefix}9{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.BuyPrice9_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}9{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}9"]                            = ToDbNull(price.SellPrice9);
                    row[$"{MsgXSchema.SqPrefix}9"]                            = ToDbNull(price.SellQuantity9);
                    row[$"{MsgXSchema.SpPrefix}9{MsgXSchema.Suffix_Noo}"]     = ToDbNull((long)price.SellPrice9_NOO);
                    row[$"{MsgXSchema.SpPrefix}9{MsgXSchema.Suffix_Mdey}"]    = ToDbNull(price.SellPrice9_MDEY);
                    row[$"{MsgXSchema.SpPrefix}9{MsgXSchema.Suffix_Mdemms}"]  = ToDbNull(price.SellPrice9_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}9{MsgXSchema.Suffix_Mdepno}"]  = DBNull.Value;

                    // --- Cấp 10 ---
                    row[$"{MsgXSchema.BpPrefix}10"]                           = ToDbNull(price.BuyPrice10);
                    row[$"{MsgXSchema.BqPrefix}10"]                           = ToDbNull(price.BuyQuantity10);
                    row[$"{MsgXSchema.BpPrefix}10{MsgXSchema.Suffix_Noo}"]    = ToDbNull((long)price.BuyPrice10_NOO);
                    row[$"{MsgXSchema.BpPrefix}10{MsgXSchema.Suffix_Mdey}"]   = ToDbNull(price.BuyPrice10_MDEY);
                    row[$"{MsgXSchema.BpPrefix}10{MsgXSchema.Suffix_Mdemms}"] = ToDbNull(price.BuyPrice10_MDEMMS);
                    row[$"{MsgXSchema.BpPrefix}10{MsgXSchema.Suffix_Mdepno}"] = DBNull.Value;
                    row[$"{MsgXSchema.SpPrefix}10"]                           = ToDbNull(price.SellPrice10);
                    row[$"{MsgXSchema.SqPrefix}10"]                           = ToDbNull(price.SellQuantity10);
                    row[$"{MsgXSchema.SpPrefix}10{MsgXSchema.Suffix_Noo}"]    = ToDbNull((long)price.SellPrice10_NOO);
                    row[$"{MsgXSchema.SpPrefix}10{MsgXSchema.Suffix_Mdey}"]   = ToDbNull(price.SellPrice10_MDEY);
                    row[$"{MsgXSchema.SpPrefix}10{MsgXSchema.Suffix_Mdemms}"] = ToDbNull(price.SellPrice10_MDEMMS);
                    row[$"{MsgXSchema.SpPrefix}10{MsgXSchema.Suffix_Mdepno}"] = DBNull.Value;

                    // Các trường giá cuối
                    row[MsgXSchema.Mp]                                        = ToDbNull(price.MatchPrice);
                    row[MsgXSchema.Mq]                                        = ToDbNull(price.MatchQuantity);
                    row[MsgXSchema.Op]                                        = ToDbNull(price.OpenPrice);
                    row[MsgXSchema.Lp]                                        = ToDbNull(price.LowestPrice);
                    row[MsgXSchema.Hp]                                        = ToDbNull(price.HighestPrice);

                    // Footer
                    if (long.TryParse(price.CheckSum, out long parsedCheckSum))
                    {
                        row[BaseMessageSchema.Checksum]                       = parsedCheckSum;
                    }
                    else
                    {
                        row[BaseMessageSchema.Checksum]                       = DBNull.Value;
                    }

                    row[BaseMessageSchema.CreateTime]                         = DateTime.Now;

                    dt.Rows.Add(row);
                }

                return dt;
            }
            catch(Exception ex)
            {
                throw ex;
            }            
        }
        private DataTable CreatePriceRecoveryDataTable(List<EPriceRecovery> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_w, dùng const) ---

            // Thông tin header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Thông tin chung (từ BasePriceSchema)
            dt.Columns.Add(BasePriceSchema.TradingSessionId, typeof(string));
            dt.Columns.Add(BasePriceSchema.Symbol, typeof(string));

            // Các cột giá (từ MsgWSchema)
            dt.Columns.Add(MsgWSchema.OpnPx, typeof(decimal));
            dt.Columns.Add(MsgWSchema.TrdSessnHighPx, typeof(decimal));
            dt.Columns.Add(MsgWSchema.TrdSessnLowPx, typeof(decimal));
            dt.Columns.Add(MsgWSchema.SymbolCloseInfoPx, typeof(decimal));
            dt.Columns.Add(MsgWSchema.OpnPxYld, typeof(decimal));
            dt.Columns.Add(MsgWSchema.TrdSessnHighPxYld, typeof(decimal));
            dt.Columns.Add(MsgWSchema.TrdSessnLowPxYld, typeof(decimal));
            dt.Columns.Add(MsgWSchema.ClsPxYld, typeof(decimal));

            // Các cột thống kê (từ BasePriceSchema)
            dt.Columns.Add(BasePriceSchema.TotalVolumeTraded, typeof(long));
            dt.Columns.Add(BasePriceSchema.GrossTradeAmt, typeof(decimal));
            dt.Columns.Add(BasePriceSchema.SellTotOrderQty, typeof(long));
            dt.Columns.Add(BasePriceSchema.BuyTotOrderQty, typeof(long));
            dt.Columns.Add(BasePriceSchema.SellValidOrderCnt, typeof(long));
            dt.Columns.Add(BasePriceSchema.BuyValidOrderCnt, typeof(long));

            // Các cột giá 1-10 (B/S) (từ BasePriceSchema)
            for (int i = 1; i <= 10; i++)
            {
                dt.Columns.Add($"{BasePriceSchema.BpPrefix}{i}", typeof(decimal));
                dt.Columns.Add($"{BasePriceSchema.BqPrefix}{i}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Noo}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdey}", typeof(decimal));
                dt.Columns.Add($"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdemms}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.BpPrefix}{i}{BasePriceSchema.Suffix_Mdepno}", typeof(int));

                dt.Columns.Add($"{BasePriceSchema.SpPrefix}{i}", typeof(decimal));
                dt.Columns.Add($"{BasePriceSchema.SqPrefix}{i}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Noo}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdey}", typeof(decimal));
                dt.Columns.Add($"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdemms}", typeof(long));
                dt.Columns.Add($"{BasePriceSchema.SpPrefix}{i}{BasePriceSchema.Suffix_Mdepno}", typeof(int));
            }

            // Cột cuối (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var priceRecovery in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]                                  = priceRecovery.BeginString;
                row[BaseMessageSchema.BodyLength]                                   = (int)priceRecovery.BodyLength;
                row[BaseMessageSchema.MsgType]                                      = priceRecovery.MsgType;
                row[BaseMessageSchema.SenderCompId]                                 = priceRecovery.SenderCompID;
                row[BaseMessageSchema.TargetCompId]                                 = priceRecovery.TargetCompID;
                row[BaseMessageSchema.MsgSeqNum]                                    = priceRecovery.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]                                  = ParseCompactDateTimeToDbNull(priceRecovery.SendingTime);
                row[BaseMessageSchema.MarketId]                                     = priceRecovery.MarketID;
                row[BaseMessageSchema.BoardId]                                      = priceRecovery.BoardID;

                // Payload chung
                row[BasePriceSchema.TradingSessionId]                               = priceRecovery.TradingSessionID;
                row[BasePriceSchema.Symbol]                                         = priceRecovery.Symbol;

                // Các trường giá (mới)
                row[MsgWSchema.OpnPx]                                               = ToDbNull(priceRecovery.OpnPx);
                row[MsgWSchema.TrdSessnHighPx]                                      = ToDbNull(priceRecovery.TrdSessnHighPx);
                row[MsgWSchema.TrdSessnLowPx]                                       = ToDbNull(priceRecovery.TrdSessnLowPx);
                row[MsgWSchema.SymbolCloseInfoPx]                                   = ToDbNull(priceRecovery.SymbolCloseInfoPx);
                row[MsgWSchema.OpnPxYld]                                            = ToDbNull(priceRecovery.OpnPxYld);
                row[MsgWSchema.TrdSessnHighPxYld]                                   = ToDbNull(priceRecovery.TrdSessnHighPxYld);
                row[MsgWSchema.TrdSessnLowPxYld]                                    = ToDbNull(priceRecovery.TrdSessnLowPxYld);
                row[MsgWSchema.ClsPxYld]                                            = ToDbNull(priceRecovery.ClsPxYld);

                // Các trường thống kê
                row[BasePriceSchema.TotalVolumeTraded]                              = ToDbNull(priceRecovery.TotalVolumeTraded);
                row[BasePriceSchema.GrossTradeAmt]                                  = ToDbNull(priceRecovery.GrossTradeAmt);
                row[BasePriceSchema.SellTotOrderQty]                                = ToDbNull(priceRecovery.SellTotOrderQty);
                row[BasePriceSchema.BuyTotOrderQty]                                 = ToDbNull(priceRecovery.BuyTotOrderQty);
                row[BasePriceSchema.SellValidOrderCnt]                              = ToDbNull(priceRecovery.SellValidOrderCnt);
                row[BasePriceSchema.BuyValidOrderCnt]                               = ToDbNull(priceRecovery.BuyValidOrderCnt);

                // --- Cấp 1 ---
                row[$"{BasePriceSchema.BpPrefix}1"]                                 = ToDbNull(priceRecovery.BuyPrice1);
                row[$"{BasePriceSchema.BqPrefix}1"]                                 = ToDbNull(priceRecovery.BuyQuantity1);
                row[$"{BasePriceSchema.BpPrefix}1{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice1_NOO);
                row[$"{BasePriceSchema.BpPrefix}1{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice1_MDEY);
                row[$"{BasePriceSchema.BpPrefix}1{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice1_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}1{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}1"]                                 = ToDbNull(priceRecovery.SellPrice1);
                row[$"{BasePriceSchema.SqPrefix}1"]                                 = ToDbNull(priceRecovery.SellQuantity1);
                row[$"{BasePriceSchema.SpPrefix}1{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice1_NOO);
                row[$"{BasePriceSchema.SpPrefix}1{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice1_MDEY);
                row[$"{BasePriceSchema.SpPrefix}1{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice1_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}1{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 2 ---
                row[$"{BasePriceSchema.BpPrefix}2"]                                 = ToDbNull(priceRecovery.BuyPrice2);
                row[$"{BasePriceSchema.BqPrefix}2"]                                 = ToDbNull(priceRecovery.BuyQuantity2);
                row[$"{BasePriceSchema.BpPrefix}2{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice2_NOO);
                row[$"{BasePriceSchema.BpPrefix}2{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice2_MDEY);
                row[$"{BasePriceSchema.BpPrefix}2{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice2_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}2{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}2"]                                 = ToDbNull(priceRecovery.SellPrice2);
                row[$"{BasePriceSchema.SqPrefix}2"]                                 = ToDbNull(priceRecovery.SellQuantity2);
                row[$"{BasePriceSchema.SpPrefix}2{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice2_NOO);
                row[$"{BasePriceSchema.SpPrefix}2{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice2_MDEY);
                row[$"{BasePriceSchema.SpPrefix}2{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice2_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}2{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 3 ---
                row[$"{BasePriceSchema.BpPrefix}3"]                                 = ToDbNull(priceRecovery.BuyPrice3);
                row[$"{BasePriceSchema.BqPrefix}3"]                                 = ToDbNull(priceRecovery.BuyQuantity3);
                row[$"{BasePriceSchema.BpPrefix}3{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice3_NOO);
                row[$"{BasePriceSchema.BpPrefix}3{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice3_MDEY);
                row[$"{BasePriceSchema.BpPrefix}3{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice3_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}3{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}3"]                                 = ToDbNull(priceRecovery.SellPrice3);
                row[$"{BasePriceSchema.SqPrefix}3"]                                 = ToDbNull(priceRecovery.SellQuantity3);
                row[$"{BasePriceSchema.SpPrefix}3{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice3_NOO);
                row[$"{BasePriceSchema.SpPrefix}3{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice3_MDEY);
                row[$"{BasePriceSchema.SpPrefix}3{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice3_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}3{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 4 ---
                row[$"{BasePriceSchema.BpPrefix}4"]                                 = ToDbNull(priceRecovery.BuyPrice4);
                row[$"{BasePriceSchema.BqPrefix}4"]                                 = ToDbNull(priceRecovery.BuyQuantity4);
                row[$"{BasePriceSchema.BpPrefix}4{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice4_NOO);
                row[$"{BasePriceSchema.BpPrefix}4{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice4_MDEY);
                row[$"{BasePriceSchema.BpPrefix}4{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice4_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}4{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}4"]                                 = ToDbNull(priceRecovery.SellPrice4);
                row[$"{BasePriceSchema.SqPrefix}4"]                                 = ToDbNull(priceRecovery.SellQuantity4);
                row[$"{BasePriceSchema.SpPrefix}4{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice4_NOO);
                row[$"{BasePriceSchema.SpPrefix}4{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice4_MDEY);
                row[$"{BasePriceSchema.SpPrefix}4{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice4_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}4{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 5 ---
                row[$"{BasePriceSchema.BpPrefix}5"]                                 = ToDbNull(priceRecovery.BuyPrice5);
                row[$"{BasePriceSchema.BqPrefix}5"]                                 = ToDbNull(priceRecovery.BuyQuantity5);
                row[$"{BasePriceSchema.BpPrefix}5{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice5_NOO);
                row[$"{BasePriceSchema.BpPrefix}5{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice5_MDEY);
                row[$"{BasePriceSchema.BpPrefix}5{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice5_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}5{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}5"]                                 = ToDbNull(priceRecovery.SellPrice5);
                row[$"{BasePriceSchema.SqPrefix}5"]                                 = ToDbNull(priceRecovery.SellQuantity5);
                row[$"{BasePriceSchema.SpPrefix}5{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice5_NOO);
                row[$"{BasePriceSchema.SpPrefix}5{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice5_MDEY);
                row[$"{BasePriceSchema.SpPrefix}5{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice5_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}5{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 6 ---
                row[$"{BasePriceSchema.BpPrefix}6"]                                 = ToDbNull(priceRecovery.BuyPrice6);
                row[$"{BasePriceSchema.BqPrefix}6"]                                 = ToDbNull(priceRecovery.BuyQuantity6);
                row[$"{BasePriceSchema.BpPrefix}6{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice6_NOO);
                row[$"{BasePriceSchema.BpPrefix}6{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice6_MDEY);
                row[$"{BasePriceSchema.BpPrefix}6{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice6_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}6{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}6"]                                 = ToDbNull(priceRecovery.SellPrice6);
                row[$"{BasePriceSchema.SqPrefix}6"]                                 = ToDbNull(priceRecovery.SellQuantity6);
                row[$"{BasePriceSchema.SpPrefix}6{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice6_NOO);
                row[$"{BasePriceSchema.SpPrefix}6{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice6_MDEY);
                row[$"{BasePriceSchema.SpPrefix}6{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice6_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}6{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 7 ---
                row[$"{BasePriceSchema.BpPrefix}7"]                                 = ToDbNull(priceRecovery.BuyPrice7);
                row[$"{BasePriceSchema.BqPrefix}7"]                                 = ToDbNull(priceRecovery.BuyQuantity7);
                row[$"{BasePriceSchema.BpPrefix}7{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice7_NOO);
                row[$"{BasePriceSchema.BpPrefix}7{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice7_MDEY);
                row[$"{BasePriceSchema.BpPrefix}7{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice7_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}7{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}7"]                                 = ToDbNull(priceRecovery.SellPrice7);
                row[$"{BasePriceSchema.SqPrefix}7"]                                 = ToDbNull(priceRecovery.SellQuantity7);
                row[$"{BasePriceSchema.SpPrefix}7{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice7_NOO);
                row[$"{BasePriceSchema.SpPrefix}7{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice7_MDEY);
                row[$"{BasePriceSchema.SpPrefix}7{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice7_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}7{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 8 ---
                row[$"{BasePriceSchema.BpPrefix}8"]                                 = ToDbNull(priceRecovery.BuyPrice8);
                row[$"{BasePriceSchema.BqPrefix}8"]                                 = ToDbNull(priceRecovery.BuyQuantity8);
                row[$"{BasePriceSchema.BpPrefix}8{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice8_NOO);
                row[$"{BasePriceSchema.BpPrefix}8{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice8_MDEY);
                row[$"{BasePriceSchema.BpPrefix}8{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice8_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}8{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}8"]                                 = ToDbNull(priceRecovery.SellPrice8);
                row[$"{BasePriceSchema.SqPrefix}8"]                                 = ToDbNull(priceRecovery.SellQuantity8);
                row[$"{BasePriceSchema.SpPrefix}8{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice8_NOO);
                row[$"{BasePriceSchema.SpPrefix}8{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice8_MDEY);
                row[$"{BasePriceSchema.SpPrefix}8{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice8_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}8{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 9 ---
                row[$"{BasePriceSchema.BpPrefix}9"]                                 = ToDbNull(priceRecovery.BuyPrice9);
                row[$"{BasePriceSchema.BqPrefix}9"]                                 = ToDbNull(priceRecovery.BuyQuantity9);
                row[$"{BasePriceSchema.BpPrefix}9{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.BuyPrice9_NOO);
                row[$"{BasePriceSchema.BpPrefix}9{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.BuyPrice9_MDEY);
                row[$"{BasePriceSchema.BpPrefix}9{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.BuyPrice9_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}9{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}9"]                                 = ToDbNull(priceRecovery.SellPrice9);
                row[$"{BasePriceSchema.SqPrefix}9"]                                 = ToDbNull(priceRecovery.SellQuantity9);
                row[$"{BasePriceSchema.SpPrefix}9{BasePriceSchema.Suffix_Noo}"]     = ToDbNull((long)priceRecovery.SellPrice9_NOO);
                row[$"{BasePriceSchema.SpPrefix}9{BasePriceSchema.Suffix_Mdey}"]    = ToDbNull(priceRecovery.SellPrice9_MDEY);
                row[$"{BasePriceSchema.SpPrefix}9{BasePriceSchema.Suffix_Mdemms}"]  = ToDbNull(priceRecovery.SellPrice9_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}9{BasePriceSchema.Suffix_Mdepno}"]  = DBNull.Value;

                // --- Cấp 10 ---
                row[$"{BasePriceSchema.BpPrefix}10"]                                = ToDbNull(priceRecovery.BuyPrice10);
                row[$"{BasePriceSchema.BqPrefix}10"]                                = ToDbNull(priceRecovery.BuyQuantity10);
                row[$"{BasePriceSchema.BpPrefix}10{BasePriceSchema.Suffix_Noo}"]    = ToDbNull((long)priceRecovery.BuyPrice10_NOO);
                row[$"{BasePriceSchema.BpPrefix}10{BasePriceSchema.Suffix_Mdey}"]   = ToDbNull(priceRecovery.BuyPrice10_MDEY);
                row[$"{BasePriceSchema.BpPrefix}10{BasePriceSchema.Suffix_Mdemms}"] = ToDbNull(priceRecovery.BuyPrice10_MDEMMS);
                row[$"{BasePriceSchema.BpPrefix}10{BasePriceSchema.Suffix_Mdepno}"] = DBNull.Value;
                row[$"{BasePriceSchema.SpPrefix}10"]                                = ToDbNull(priceRecovery.SellPrice10);
                row[$"{BasePriceSchema.SqPrefix}10"]                                = ToDbNull(priceRecovery.SellQuantity10);
                row[$"{BasePriceSchema.SpPrefix}10{BasePriceSchema.Suffix_Noo}"]    = ToDbNull((long)priceRecovery.SellPrice10_NOO);
                row[$"{BasePriceSchema.SpPrefix}10{BasePriceSchema.Suffix_Mdey}"]   = ToDbNull(priceRecovery.SellPrice10_MDEY);
                row[$"{BasePriceSchema.SpPrefix}10{BasePriceSchema.Suffix_Mdemms}"] = ToDbNull(priceRecovery.SellPrice10_MDEMMS);
                row[$"{BasePriceSchema.SpPrefix}10{BasePriceSchema.Suffix_Mdepno}"] = DBNull.Value;

                // Footer (Đã sửa lỗi Parse)
                if (long.TryParse(priceRecovery.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]                                 = parsedCheckSum;
                }
                else
                {
                    row[BaseMessageSchema.Checksum]                                 = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]                                   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSecurityStatusDataTable(List<ESecurityStatus> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_f, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgFSchema)
            dt.Columns.Add(MsgFSchema.TscProdGrpId, typeof(string));
            dt.Columns.Add(MsgFSchema.BoardEvtId, typeof(string));
            dt.Columns.Add(MsgFSchema.SessOpenCloseCode, typeof(string));
            dt.Columns.Add(MsgFSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgFSchema.TradingSessionId, typeof(string));
            dt.Columns.Add(MsgFSchema.HaltRsnCode, typeof(long));
            dt.Columns.Add(MsgFSchema.ProductId, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var status in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = status.BeginString;
                row[BaseMessageSchema.BodyLength]   = (int)status.BodyLength;
                row[BaseMessageSchema.MsgType]      = status.MsgType;
                row[BaseMessageSchema.SenderCompId] = status.SenderCompID;
                row[BaseMessageSchema.TargetCompId] = status.TargetCompID;
                row[BaseMessageSchema.MsgSeqNum]    = ToDbNull(status.MsgSeqNum);
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(status.SendingTime);
                row[BaseMessageSchema.MarketId]     = status.MarketID;
                row[BaseMessageSchema.BoardId]      = status.BoardID;

                // Payload
                row[MsgFSchema.TscProdGrpId]        = status.TscProdGrpId;
                row[MsgFSchema.BoardEvtId]          = status.BoardEvtID;
                row[MsgFSchema.SessOpenCloseCode]   = status.SessOpenCloseCode;
                row[MsgFSchema.Symbol]              = status.Symbol;
                row[MsgFSchema.TradingSessionId]    = status.TradingSessionID;

                // Chuyển đổi an toàn HaltRsnCode
                if (long.TryParse(status.HaltRsnCode, out long parsedHaltRsnCode))
                {
                    row[MsgFSchema.HaltRsnCode]     = ToDbNull(parsedHaltRsnCode);
                }
                else
                {
                    row[MsgFSchema.HaltRsnCode]     = DBNull.Value;
                }

                row[MsgFSchema.ProductId]           = status.ProductID;

                // Footer 
                if (long.TryParse(status.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateIndexDataTable(List<EIndex> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m1, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgM1Schema)
            dt.Columns.Add(MsgM1Schema.TradingSessionId, typeof(string));
            dt.Columns.Add(MsgM1Schema.MarketIndexClass, typeof(string));
            dt.Columns.Add(MsgM1Schema.IndexsTypeCode, typeof(string));
            dt.Columns.Add(MsgM1Schema.Currency, typeof(string));
            dt.Columns.Add(MsgM1Schema.TransactTime, typeof(string));
            dt.Columns.Add(MsgM1Schema.TransDate, typeof(DateTime));
            dt.Columns.Add(MsgM1Schema.ValueIndexes, typeof(decimal));
            dt.Columns.Add(MsgM1Schema.TotalVolumeTraded, typeof(long));
            dt.Columns.Add(MsgM1Schema.GrossTradeAmt, typeof(decimal));
            dt.Columns.Add(MsgM1Schema.ContauctAccTrdvol, typeof(long));
            dt.Columns.Add(MsgM1Schema.ContauctAccTrdval, typeof(decimal));
            dt.Columns.Add(MsgM1Schema.BlktrdAccTrdvol, typeof(long));
            dt.Columns.Add(MsgM1Schema.BlktrdAccTrdval, typeof(decimal));
            dt.Columns.Add(MsgM1Schema.FluctuationUpperLimitIc, typeof(int));
            dt.Columns.Add(MsgM1Schema.FluctuationUpIc, typeof(int));
            dt.Columns.Add(MsgM1Schema.FluctuationSteadinessIc, typeof(int));
            dt.Columns.Add(MsgM1Schema.FluctuationDownIc, typeof(int));
            dt.Columns.Add(MsgM1Schema.FluctuationLowerLimitIc, typeof(int));
            dt.Columns.Add(MsgM1Schema.FluctuationUpIv, typeof(long));
            dt.Columns.Add(MsgM1Schema.FluctuationDownIv, typeof(long));
            dt.Columns.Add(MsgM1Schema.FluctuationSteadinessIv, typeof(long));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var index in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]       = index.BeginString;
                row[BaseMessageSchema.BodyLength]        = (int)index.BodyLength;
                row[BaseMessageSchema.MsgType]           = index.MsgType;
                row[BaseMessageSchema.SenderCompId]      = index.SenderCompID;
                row[BaseMessageSchema.TargetCompId]      = index.TargetCompID;
                row[BaseMessageSchema.MsgSeqNum]         = index.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]       = ParseDashDateTimeToDbNull(index.SendingTime);
                row[BaseMessageSchema.MarketId]          = index.MarketID;

                // Payload
                row[MsgM1Schema.TradingSessionId]        = index.TradingSessionID;
                row[MsgM1Schema.MarketIndexClass]        = index.MarketIndexClass;
                row[MsgM1Schema.IndexsTypeCode]          = index.IndexsTypeCode;
                row[MsgM1Schema.Currency]                = index.Currency;
                row[MsgM1Schema.TransactTime]            = index.TransactTime;
                row[MsgM1Schema.TransDate]               = ParseDayMonYearToDbNull(index.TransDate);
                row[MsgM1Schema.ValueIndexes]            = ToDbNull(index.ValueIndexes);
                row[MsgM1Schema.TotalVolumeTraded]       = ToDbNull(index.TotalVolumeTraded);
                row[MsgM1Schema.GrossTradeAmt]           = ToDbNull(index.GrossTradeAmt);
                row[MsgM1Schema.ContauctAccTrdvol]       = ToDbNull(index.ContauctAccTrdvol);
                row[MsgM1Schema.ContauctAccTrdval]       = ToDbNull(index.ContauctAccTrdval);
                row[MsgM1Schema.BlktrdAccTrdvol]         = ToDbNull(index.BlktrdAccTrdvol);
                row[MsgM1Schema.BlktrdAccTrdval]         = ToDbNull(index.BlktrdAccTrdval);

                row[MsgM1Schema.FluctuationUpperLimitIc] = ToDbNull(index.FluctuationUpperLimitIssueCount);
                row[MsgM1Schema.FluctuationUpIc]         = ToDbNull(index.FluctuationUpIssueCount);
                row[MsgM1Schema.FluctuationSteadinessIc] = ToDbNull(index.FluctuationSteadinessIssueCount);
                row[MsgM1Schema.FluctuationDownIc]       = ToDbNull(index.FluctuationDownIssueCount);
                row[MsgM1Schema.FluctuationLowerLimitIc] = ToDbNull(index.FluctuationLowerLimitIssueCount);

                row[MsgM1Schema.FluctuationUpIv]         = ToDbNull(index.FluctuationUpIssueVolume);
                row[MsgM1Schema.FluctuationDownIv]       = ToDbNull(index.FluctuationDownIssueVolume);
                row[MsgM1Schema.FluctuationSteadinessIv] = ToDbNull(index.FluctuationSteadinessIssueVolume);

                // Footer
                if (long.TryParse(index.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]      = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]      = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]        = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateInvestorPerIndustryDataTable(List<EInvestorPerIndustry> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m2, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgM2Schema)
            dt.Columns.Add(MsgM2Schema.TransactTime, typeof(string));
            dt.Columns.Add(MsgM2Schema.MarketIndexClass, typeof(string));
            dt.Columns.Add(MsgM2Schema.IndexsTypeCode, typeof(string));
            dt.Columns.Add(MsgM2Schema.Currency, typeof(string));
            dt.Columns.Add(MsgM2Schema.InvestCode, typeof(string));
            dt.Columns.Add(MsgM2Schema.SellVolume, typeof(long));
            dt.Columns.Add(MsgM2Schema.SellTradeAmount, typeof(decimal));
            dt.Columns.Add(MsgM2Schema.BuyVolume, typeof(long));
            dt.Columns.Add(MsgM2Schema.BuyTradedAmount, typeof(decimal));
            dt.Columns.Add(MsgM2Schema.BondClassificationCode, typeof(string));
            dt.Columns.Add(MsgM2Schema.SecurityGroupId, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]      = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]       = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]          = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]     = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]     = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]        = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]      = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]         = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgM2Schema.TransactTime]           = (object)msg.TransactTime ?? DBNull.Value;
                row[MsgM2Schema.MarketIndexClass]       = (object)msg.MarketIndexClass ?? DBNull.Value;
                row[MsgM2Schema.IndexsTypeCode]         = (object)msg.IndexsTypeCode ?? DBNull.Value;
                row[MsgM2Schema.Currency]               = (object)msg.Currency ?? DBNull.Value;
                row[MsgM2Schema.InvestCode]             = (object)msg.InvestCode ?? DBNull.Value;

                row[MsgM2Schema.SellVolume]             = msg.SellVolume;
                row[MsgM2Schema.SellTradeAmount]        = msg.SellTradeAmount;
                row[MsgM2Schema.BuyVolume]              = msg.BuyVolume;
                row[MsgM2Schema.BuyTradedAmount]        = msg.BuyTradedAmount;

                row[MsgM2Schema.BondClassificationCode] = (object)msg.BondClassificationCode ?? DBNull.Value;
                row[MsgM2Schema.SecurityGroupId]        = (object)msg.SecurityGroupID ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]     = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]     = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]       = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateInvestorPerSymbolDataTable(List<EInvestorPerSymbol> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m3, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            // (Bảng này không dùng 'boardid' từ Base)

            // Payload (từ MsgM3Schema)
            dt.Columns.Add(MsgM3Schema.Symbol, typeof(string));
            dt.Columns.Add(MsgM3Schema.InvestCode, typeof(string));
            dt.Columns.Add(MsgM3Schema.SellVolume, typeof(long));
            dt.Columns.Add(MsgM3Schema.SellTradeAmount, typeof(decimal));
            dt.Columns.Add(MsgM3Schema.BuyVolume, typeof(long));
            dt.Columns.Add(MsgM3Schema.BuyTradedAmount, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgM3Schema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgM3Schema.InvestCode]         = (object)msg.InvestCode ?? DBNull.Value;

                row[MsgM3Schema.SellVolume]         = msg.SellVolume;
                row[MsgM3Schema.SellTradeAmount]    = msg.SellTradeAmount;
                row[MsgM3Schema.BuyVolume]          = msg.BuyVolume;
                row[MsgM3Schema.BuyTradedAmount]    = msg.BuyTradedAmount;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNMembersPerSymbolDataTable(List<ETopNMembersPerSymbol> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m4, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            // (Bảng này không dùng 'boardid' từ Base)

            // Payload (từ MsgM4Schema)
            dt.Columns.Add(MsgM4Schema.Symbol, typeof(string));
            dt.Columns.Add(MsgM4Schema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgM4Schema.SellRankSeq, typeof(int));
            dt.Columns.Add(MsgM4Schema.SellMemberNo, typeof(string));
            dt.Columns.Add(MsgM4Schema.SellVolume, typeof(long));
            dt.Columns.Add(MsgM4Schema.SellTradeAmount, typeof(decimal));
            dt.Columns.Add(MsgM4Schema.BuyRankSeq, typeof(int));
            dt.Columns.Add(MsgM4Schema.BuyMemberNo, typeof(string));
            dt.Columns.Add(MsgM4Schema.BuyVolume, typeof(long));
            dt.Columns.Add(MsgM4Schema.BuyTradedAmount, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgM4Schema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgM4Schema.TotNumReports]      = msg.TotNumReports; 

                row[MsgM4Schema.SellRankSeq]        = msg.SellRankSeq; 
                row[MsgM4Schema.SellMemberNo]       = (object)msg.SellMemberNo ?? DBNull.Value;
                row[MsgM4Schema.SellVolume]         = msg.SellVolume; 
                row[MsgM4Schema.SellTradeAmount]    = msg.SellTradeAmount; 

                row[MsgM4Schema.BuyRankSeq]         = msg.BuyRankSeq;
                row[MsgM4Schema.BuyMemberNo]        = (object)msg.BuyMemberNo ?? DBNull.Value;
                row[MsgM4Schema.BuyVolume]          = msg.BuyVolume; 
                row[MsgM4Schema.BuyTradedAmount]    = msg.BuyTradedAmount; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSecurityInfoNotificationDataTable(List<ESecurityInformationNotification> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m7, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgM7Schema)
            dt.Columns.Add(MsgM7Schema.Symbol, typeof(string));
            dt.Columns.Add(MsgM7Schema.ReferencePrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.HighLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.LowLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.EvaluationPrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.HgstOrderPrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.LwstOrderPrice, typeof(decimal));
            dt.Columns.Add(MsgM7Schema.ListedShares, typeof(long));
            dt.Columns.Add(MsgM7Schema.ExClassType, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]      = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgM7Schema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgM7Schema.ReferencePrice]     = msg.ReferencePrice; 
                row[MsgM7Schema.HighLimitPrice]     = msg.HighLimitPrice; 
                row[MsgM7Schema.LowLimitPrice]      = msg.LowLimitPrice; 
                row[MsgM7Schema.EvaluationPrice]    = msg.EvaluationPrice;
                row[MsgM7Schema.HgstOrderPrice]     = msg.HgstOrderPrice; 
                row[MsgM7Schema.LwstOrderPrice]     = msg.LwstOrderPrice; 
                row[MsgM7Schema.ListedShares]       = msg.ListedShares;
                row[MsgM7Schema.ExClassType]        = (object)msg.ExClassType ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSymbolClosingInfoDataTable(List<ESymbolClosingInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_m8, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgM8Schema)
            dt.Columns.Add(MsgM8Schema.Symbol, typeof(string));
            dt.Columns.Add(MsgM8Schema.SymbolCloseInfoPx, typeof(decimal));
            dt.Columns.Add(MsgM8Schema.SymbolCloseInfoYield, typeof(decimal));
            dt.Columns.Add(MsgM8Schema.SymbolCloseInfoPxType, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]     = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]      = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]         = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]    = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]    = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]       = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]     = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]        = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]         = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgM8Schema.Symbol]                = (object)msg.Symbol ?? DBNull.Value;
                row[MsgM8Schema.SymbolCloseInfoPx]     = msg.SymbolCloseInfoPx;
                row[MsgM8Schema.SymbolCloseInfoYield]  = msg.SymbolCloseInfoYield;
                row[MsgM8Schema.SymbolCloseInfoPxType] = (object)msg.SymbolCloseInfoPxType ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]    = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]    = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]      = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateOpenInterestDataTable(List<EOpenInterest> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ma, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMASchema)
            dt.Columns.Add(MsgMASchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMASchema.TradeDate, typeof(DateTime));
            dt.Columns.Add(MsgMASchema.OpenInterestQty, typeof(decimal));
            dt.Columns.Add(MsgMASchema.SettlementPrice, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMASchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMASchema.TradeDate]          = ParseDayMonYearToDbNull(msg.TradeDate);
                row[MsgMASchema.OpenInterestQty]    = msg.OpenInterestQty;
                row[MsgMASchema.SettlementPrice]    = DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateVolatilityInterruptionDataTable(List<EVolatilityInterruption> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_md, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgMDSchema)
            dt.Columns.Add(MsgMDSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMDSchema.VITypeCode, typeof(string));
            dt.Columns.Add(MsgMDSchema.VIKindCode, typeof(string));
            dt.Columns.Add(MsgMDSchema.StaticVIBasePrice, typeof(decimal));
            dt.Columns.Add(MsgMDSchema.DynamicVIBasePrice, typeof(decimal));
            dt.Columns.Add(MsgMDSchema.VIPrice, typeof(decimal));
            dt.Columns.Add(MsgMDSchema.StaticVIDispartiyRatio, typeof(decimal));
            dt.Columns.Add(MsgMDSchema.DynamicVIDispartiyRatio, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]       = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]        = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]           = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]      = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]      = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]         = ToDbNull(msg.MsgSeqNum);
                row[BaseMessageSchema.SendingTime]       = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]          = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]           = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgMDSchema.Symbol]                  = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMDSchema.VITypeCode]              = (object)msg.VITypeCode ?? DBNull.Value;
                row[MsgMDSchema.VIKindCode]              = (object)msg.VIKindCode ?? DBNull.Value;
                row[MsgMDSchema.StaticVIBasePrice]       = msg.StaticVIBasePrice; 
                row[MsgMDSchema.DynamicVIBasePrice]      = msg.DynamicVIBasePrice; 
                row[MsgMDSchema.VIPrice]                 = msg.VIPrice; 
                row[MsgMDSchema.StaticVIDispartiyRatio]  = msg.StaticVIDispartiyRatio; 
                row[MsgMDSchema.DynamicVIDispartiyRatio] = msg.DynamicVIDispartiyRatio; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]      = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]      = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]        = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateDeemTradePriceDataTable(List<EDeemTradePrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_me, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgMESchema)
            dt.Columns.Add(MsgMESchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMESchema.ExpectedTradePx, typeof(decimal));
            dt.Columns.Add(MsgMESchema.ExpectedTradeQty, typeof(long));
            dt.Columns.Add(MsgMESchema.ExpectedTradeYield, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]      = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgMESchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMESchema.ExpectedTradePx]    = msg.ExpectedTradePx; 
                row[MsgMESchema.ExpectedTradeQty]   = msg.ExpectedTradeQty; 
                row[MsgMESchema.ExpectedTradeYield] = msg.ExpectedTradeYield; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateForeignerOrderLimitDataTable(List<EForeignerOrderLimit> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mf, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMFSchema)
            dt.Columns.Add(MsgMFSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMFSchema.ForeignerBuyPosblQty, typeof(long));
            dt.Columns.Add(MsgMFSchema.ForeignerOrderLimitQty, typeof(long));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]      = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]       = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]          = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]     = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]     = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]        = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]      = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]         = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMFSchema.Symbol]                 = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMFSchema.ForeignerBuyPosblQty]   = msg.ForeignerBuyPosblQty; 
                row[MsgMFSchema.ForeignerOrderLimitQty] = msg.ForeignerOrderLimitQty; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]     = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]     = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]       = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateMarketMakerInfoDataTable(List<EMarketMakerInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mh, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMHSchema)
            dt.Columns.Add(MsgMHSchema.MarketMakerContractCode, typeof(string));
            dt.Columns.Add(MsgMHSchema.MemberNo, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]       = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]        = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]           = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]      = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]      = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]         = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]       = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]          = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMHSchema.MarketMakerContractCode] = (object)msg.MarketMakerContractCode ?? DBNull.Value;
                row[MsgMHSchema.MemberNo]                = (object)msg.MemberNo ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]      = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]      = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]        = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateSymbolEventDataTable(List<ESymbolEvent> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mi, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            // (Bảng này không dùng 'boardid' từ Base)

            // Payload (từ MsgMISchema)
            dt.Columns.Add(MsgMISchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMISchema.EventKindCode, typeof(string));
            dt.Columns.Add(MsgMISchema.EventOccurrenceReasonCode, typeof(string));
            dt.Columns.Add(MsgMISchema.EventStartDate, typeof(string));
            dt.Columns.Add(MsgMISchema.EventEndDate, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]         = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]          = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]             = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]        = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]        = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]           = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]         = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]            = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMISchema.Symbol]                    = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMISchema.EventKindCode]             = (object)msg.EventKindCode ?? DBNull.Value;
                row[MsgMISchema.EventOccurrenceReasonCode] = (object)msg.EventOccurrenceReasonCode ?? DBNull.Value;
                row[MsgMISchema.EventStartDate]            = (object)msg.EventStartDate ?? DBNull.Value;
                row[MsgMISchema.EventEndDate]              = (object)msg.EventEndDate ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]        = ToDbNull(parsedCheckSum);
                }
                else
                    row[BaseMessageSchema.Checksum]        = DBNull.Value;

                row[BaseMessageSchema.CreateTime]          = DateTime.Now;

                dt.Rows.Add(row);
            }
            return dt;
        }
        private DataTable CreateDrvProductEventDataTable(List<EDrvProductEvent> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mj, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));

            // Payload (từ MsgMJSchema)
            dt.Columns.Add(MsgMJSchema.ProductId, typeof(string));
            dt.Columns.Add(MsgMJSchema.EventKindCode, typeof(string));
            dt.Columns.Add(MsgMJSchema.EventOccurrenceReasonCode, typeof(string));
            dt.Columns.Add(MsgMJSchema.EventStartDate, typeof(string));
            dt.Columns.Add(MsgMJSchema.EventEndDate, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]         = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]          = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]             = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]        = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]        = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]           = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]         = ParseDashDateTimeToDbNull(msg.SendingTime);

                // Payload
                row[MsgMJSchema.ProductId]                 = (object)msg.ProductID ?? DBNull.Value;
                row[MsgMJSchema.EventKindCode]             = (object)msg.EventKindCode ?? DBNull.Value;
                row[MsgMJSchema.EventOccurrenceReasonCode] = (object)msg.EventOccurrenceReasonCode ?? DBNull.Value;
                row[MsgMJSchema.EventStartDate]            = (object)msg.EventStartDate ?? DBNull.Value;
                row[MsgMJSchema.EventEndDate]              = (object)msg.EventEndDate ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]        = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]        = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]          = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateIndexConstituentsDataTable(List<EIndexConstituentsInformation> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ml, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMLSchema)
            dt.Columns.Add(MsgMLSchema.MarketIndexClass, typeof(string));
            dt.Columns.Add(MsgMLSchema.IndexsTypeCode, typeof(string));
            dt.Columns.Add(MsgMLSchema.Currency, typeof(string));
            dt.Columns.Add(MsgMLSchema.IdxName, typeof(string));
            dt.Columns.Add(MsgMLSchema.IdxEnglishName, typeof(string));
            dt.Columns.Add(MsgMLSchema.TotalMsgNo, typeof(long));
            dt.Columns.Add(MsgMLSchema.CurrentMsgNo, typeof(long));
            dt.Columns.Add(MsgMLSchema.Symbol, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMLSchema.MarketIndexClass]   = (object)msg.MarketIndexClass ?? DBNull.Value;
                row[MsgMLSchema.IndexsTypeCode]     = (object)msg.IndexsTypeCode ?? DBNull.Value;
                row[MsgMLSchema.Currency]           = (object)msg.Currency ?? DBNull.Value;
                row[MsgMLSchema.IdxName]            = (object)msg.IdxName ?? DBNull.Value;
                row[MsgMLSchema.IdxEnglishName]     = (object)msg.IdxEnglishName ?? DBNull.Value;
                row[MsgMLSchema.TotalMsgNo]         = msg.TotalMsgNo; 
                row[MsgMLSchema.CurrentMsgNo]       = msg.CurrentMsgNo; 
                row[MsgMLSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFiNavDataTable(List<EETFiNav> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mm, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            // (Bảng này không dùng 'boardid' từ Base)

            // Payload (từ MsgMMSchema)
            dt.Columns.Add(MsgMMSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMMSchema.TransactTime, typeof(string));
            dt.Columns.Add(MsgMMSchema.INAVValue, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMMSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMMSchema.TransactTime]       = (object)msg.TransactTime ?? DBNull.Value;
                row[MsgMMSchema.INAVValue]          = msg.iNAVvalue;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFiIndexDataTable(List<EETFiIndex> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mn, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMNSchema)
            dt.Columns.Add(MsgMNSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMNSchema.TransactTime, typeof(string));
            dt.Columns.Add(MsgMNSchema.ValuesIndexes, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = ToDbNull(msg.MsgSeqNum);
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMNSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMNSchema.TransactTime]       = (object)msg.TransactTime ?? DBNull.Value;
                row[MsgMNSchema.ValuesIndexes]      = msg.ValueIndexes;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateETFTrackingErrorDataTable(List<EETFTrackingError> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mo, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMOSchema)
            dt.Columns.Add(MsgMOSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMOSchema.TradeDate, typeof(DateTime));
            dt.Columns.Add(MsgMOSchema.TrackingError, typeof(decimal));
            dt.Columns.Add(MsgMOSchema.DisparateRatio, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMOSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMOSchema.TradeDate]          = ParseDayMonYearToDbNull(msg.TradeDate);
                row[MsgMOSchema.TrackingError]      = msg.TrackingError; 
                row[MsgMOSchema.DisparateRatio]     = msg.DisparateRatio; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithTradingQuantityDataTable(List<ETopNSymbolsWithTradingQuantity> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mp, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            // (Bảng này KHÔNG dùng 'boardid')

            // Payload (từ MsgMPSchema)
            dt.Columns.Add(MsgMPSchema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgMPSchema.Rank, typeof(int));
            dt.Columns.Add(MsgMPSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMPSchema.MDEntrySize, typeof(long));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMPSchema.TotNumReports]      = msg.TotNumReports;
                row[MsgMPSchema.Rank]               = msg.Rank;
                row[MsgMPSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMPSchema.MDEntrySize]        = msg.MDEntrySize;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithCurrentPriceDataTable(List<ETopNSymbolsWithCurrentPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mq, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMQSchema)
            dt.Columns.Add(MsgMQSchema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgMQSchema.Rank, typeof(int));
            dt.Columns.Add(MsgMQSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMQSchema.MDEntryPx, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMQSchema.TotNumReports]      = msg.TotNumReports;
                row[MsgMQSchema.Rank]               = msg.Rank;
                row[MsgMQSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMQSchema.MDEntryPx]          = msg.MDEntryPx;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithHighRatioOfPriceDataTable(List<ETopNSymbolsWithHighRatioOfPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mr, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMRSchema)
            dt.Columns.Add(MsgMRSchema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgMRSchema.Rank, typeof(int));
            dt.Columns.Add(MsgMRSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMRSchema.PriceFluctuationRatio, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]     = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]      = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]         = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]    = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]    = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]       = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]     = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]        = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMRSchema.TotNumReports]         = msg.TotNumReports;
                row[MsgMRSchema.Rank]                  = msg.Rank;
                row[MsgMRSchema.Symbol]                = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMRSchema.PriceFluctuationRatio] = msg.PriceFluctuationRatio;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]    = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]    = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]      = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTopNSymbolsWithLowRatioOfPriceDataTable(List<ETopNSymbolsWithLowRatioOfPrice> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_ms, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMSSchema)
            dt.Columns.Add(MsgMSSchema.TotNumReports, typeof(long));
            dt.Columns.Add(MsgMSSchema.Rank, typeof(int));
            dt.Columns.Add(MsgMSSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMSSchema.PriceFluctuationRatio, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]     = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]      = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]         = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]    = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]    = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]       = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]     = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]        = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMSSchema.TotNumReports]         = msg.TotNumReports;
                row[MsgMSSchema.Rank]                  = msg.Rank;
                row[MsgMSSchema.Symbol]                = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMSSchema.PriceFluctuationRatio] = msg.PriceFluctuationRatio;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]    = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]    = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]      = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateTradingResultOfForeignInvestorsDataTable(List<ETradingResultOfForeignInvestors> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mt, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgMTSchema)
            dt.Columns.Add(MsgMTSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMTSchema.TradingSessionId, typeof(string));
            dt.Columns.Add(MsgMTSchema.TransactTime, typeof(string));
            dt.Columns.Add(MsgMTSchema.FornInvestTypeCode, typeof(string));
            dt.Columns.Add(MsgMTSchema.SellVolume, typeof(long));
            dt.Columns.Add(MsgMTSchema.SellTradeAmount, typeof(decimal));
            dt.Columns.Add(MsgMTSchema.BuyVolume, typeof(long));
            dt.Columns.Add(MsgMTSchema.BuyTradedAmount, typeof(decimal));
            dt.Columns.Add(MsgMTSchema.SellVolumeTotal, typeof(long));
            dt.Columns.Add(MsgMTSchema.SellTradeAmountTotal, typeof(decimal));
            dt.Columns.Add(MsgMTSchema.BuyVolumeTotal, typeof(long));
            dt.Columns.Add(MsgMTSchema.BuyTradedAmountTotal, typeof(decimal));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]    = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]     = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]        = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]   = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]   = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]      = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]    = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]       = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]        = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgMTSchema.Symbol]               = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMTSchema.TradingSessionId]     = (object)msg.TradingSessionID ?? DBNull.Value;
                row[MsgMTSchema.TransactTime]         = (object)msg.TransactTime ?? DBNull.Value;
                row[MsgMTSchema.FornInvestTypeCode]   = (object)msg.FornInvestTypeCode ?? DBNull.Value;

                row[MsgMTSchema.SellVolume]           = msg.SellVolume;
                row[MsgMTSchema.SellTradeAmount]      = msg.SellTradeAmount;
                row[MsgMTSchema.BuyVolume]            = msg.BuyVolume;
                row[MsgMTSchema.BuyTradedAmount]      = msg.BuyTradedAmount;
                row[MsgMTSchema.SellVolumeTotal]      = msg.SellVolumeTotal;
                row[MsgMTSchema.SellTradeAmountTotal] = msg.SellTradeAmountTotal;
                row[MsgMTSchema.BuyVolumeTotal]       = msg.BuyVolumeTotal;
                row[MsgMTSchema.BuyTradedAmountTotal] = msg.BuyTradeAmountTotal;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]   = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]   = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]     = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateDisclosureDataTable(List<EDisclosure> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mu, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));

            // Payload (từ MsgMUSchema)
            dt.Columns.Add(MsgMUSchema.SecurityExchange, typeof(string));
            dt.Columns.Add(MsgMUSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMUSchema.SymbolName, typeof(string));
            dt.Columns.Add(MsgMUSchema.DisclosureId, typeof(string));
            dt.Columns.Add(MsgMUSchema.TotalMsgNo, typeof(long));
            dt.Columns.Add(MsgMUSchema.CurrentMsgNo, typeof(long));
            dt.Columns.Add(MsgMUSchema.LanquageCategory, typeof(string));
            dt.Columns.Add(MsgMUSchema.DataCategory, typeof(string));
            dt.Columns.Add(MsgMUSchema.PublicInformationDate, typeof(string));
            dt.Columns.Add(MsgMUSchema.TransmissionDate, typeof(string));
            dt.Columns.Add(MsgMUSchema.ProcessType, typeof(string));
            dt.Columns.Add(MsgMUSchema.Headline, typeof(string));
            dt.Columns.Add(MsgMUSchema.Body, typeof(string));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]     = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]      = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]         = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]    = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]    = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]       = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]     = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]        = (object)msg.MarketID ?? DBNull.Value;

                // Payload
                row[MsgMUSchema.SecurityExchange]      = (object)msg.SecurityExchange ?? DBNull.Value;
                row[MsgMUSchema.Symbol]                = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMUSchema.SymbolName]            = (object)msg.SymbolName ?? DBNull.Value;
                row[MsgMUSchema.DisclosureId]          = (object)msg.DisclosureID ?? DBNull.Value;
                row[MsgMUSchema.TotalMsgNo]            = msg.TotalMsgNo;
                row[MsgMUSchema.CurrentMsgNo]          = msg.CurrentMsgNo;
                row[MsgMUSchema.LanquageCategory]      = (object)msg.LanquageCategory ?? DBNull.Value;
                row[MsgMUSchema.DataCategory]          = (object)msg.DataCategory ?? DBNull.Value;
                row[MsgMUSchema.PublicInformationDate] = (object)msg.PublicInformationDate ?? DBNull.Value;
                row[MsgMUSchema.TransmissionDate]      = (object)msg.TransmissionDate ?? DBNull.Value;
                row[MsgMUSchema.ProcessType]           = (object)msg.ProcessType ?? DBNull.Value;
                row[MsgMUSchema.Headline]              = (object)msg.Headline ?? DBNull.Value;
                row[MsgMUSchema.Body]                  = (object)msg.Body ?? DBNull.Value;

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]    = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]    = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]      = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreateRandomEndDataTable(List<ERandomEnd> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mw, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgMWSchema)
            dt.Columns.Add(MsgMWSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMWSchema.TransactTime, typeof(string));
            dt.Columns.Add(MsgMWSchema.ReApplyClassification, typeof(string));
            dt.Columns.Add(MsgMWSchema.ReTentativeExecutionPrice, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.ReEstimatedHighestPrice, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.ReEHighestPriceDisparater, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.ReEstimatedLowestPrice, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.ReELowestPriceDisparater, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.LatestPrice, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.LatestPriceDisparateRatio, typeof(decimal));
            dt.Columns.Add(MsgMWSchema.RandomEndReleaseTime, typeof(DateTime));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]         = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]          = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]             = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId]        = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId]        = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]           = msg.MsgSeqNum; 
                row[BaseMessageSchema.SendingTime]         = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]            = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]             = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgMWSchema.Symbol]                    = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMWSchema.TransactTime]              = (object)msg.TransactTime ?? DBNull.Value;
                row[MsgMWSchema.ReApplyClassification]     = (object)msg.RandomEndApplyClassification ?? DBNull.Value;
                row[MsgMWSchema.ReTentativeExecutionPrice] = msg.RandomEndTentativeExecutionPrice; 
                row[MsgMWSchema.ReEstimatedHighestPrice]   = msg.RandomEndEstimatedHighestPrice; 
                row[MsgMWSchema.ReEHighestPriceDisparater] = msg.RandomEndEstimatedHighestPriceDisparateRatio; 
                row[MsgMWSchema.ReEstimatedLowestPrice]    = msg.RandomEndEstimatedLowestPrice; 
                row[MsgMWSchema.ReELowestPriceDisparater]  = msg.RandomEndEstimatedLowestPriceDisparateRatio; 
                row[MsgMWSchema.LatestPrice]               = msg.LatestPrice; 
                row[MsgMWSchema.LatestPriceDisparateRatio] = msg.LatestPriceDisparateRatio;

                if (DateTime.TryParseExact(msg.RandomEndReleaseTimes, "yyyyMMdd HH:mm:ss.fff",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime releaseTime))
                {
                    row[MsgMWSchema.RandomEndReleaseTime]  = releaseTime;
                }
                else
                {
                    row[MsgMWSchema.RandomEndReleaseTime]  = DBNull.Value;
                }

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum]        = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum]        = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]          = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreatePriceLimitExpansionDataTable(List<EPriceLimitExpansion> messages)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_mx, dùng const) ---

            // Header (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.BeginString, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BodyLength, typeof(int));
            dt.Columns.Add(BaseMessageSchema.MsgType, typeof(string));
            dt.Columns.Add(BaseMessageSchema.SenderCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.TargetCompId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.MsgSeqNum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.SendingTime, typeof(DateTime));
            dt.Columns.Add(BaseMessageSchema.MarketId, typeof(string));
            dt.Columns.Add(BaseMessageSchema.BoardId, typeof(string));

            // Payload (từ MsgMXSchema)
            dt.Columns.Add(MsgMXSchema.Symbol, typeof(string));
            dt.Columns.Add(MsgMXSchema.HighLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgMXSchema.LowLimitPrice, typeof(decimal));
            dt.Columns.Add(MsgMXSchema.PleUpLmtStep, typeof(int));
            dt.Columns.Add(MsgMXSchema.PleLwLmtStep, typeof(int));

            // Footer (từ BaseMessageSchema)
            dt.Columns.Add(BaseMessageSchema.Checksum, typeof(long));
            dt.Columns.Add(BaseMessageSchema.CreateTime, typeof(DateTime));

            // --- 2. ĐIỀN DỮ LIỆU ---
            foreach (var msg in messages)
            {
                DataRow row = dt.NewRow();

                // Header
                row[BaseMessageSchema.BeginString]  = (object)msg.BeginString ?? DBNull.Value;
                row[BaseMessageSchema.BodyLength]   = (int)msg.BodyLength;
                row[BaseMessageSchema.MsgType]      = (object)msg.MsgType ?? DBNull.Value;
                row[BaseMessageSchema.SenderCompId] = (object)msg.SenderCompID ?? DBNull.Value;
                row[BaseMessageSchema.TargetCompId] = (object)msg.TargetCompID ?? DBNull.Value;
                row[BaseMessageSchema.MsgSeqNum]    = msg.MsgSeqNum;
                row[BaseMessageSchema.SendingTime]  = ParseDashDateTimeToDbNull(msg.SendingTime);
                row[BaseMessageSchema.MarketId]     = (object)msg.MarketID ?? DBNull.Value;
                row[BaseMessageSchema.BoardId]      = (object)msg.BoardID ?? DBNull.Value;

                // Payload
                row[MsgMXSchema.Symbol]             = (object)msg.Symbol ?? DBNull.Value;
                row[MsgMXSchema.HighLimitPrice]     = msg.HighLimitPrice;
                row[MsgMXSchema.LowLimitPrice]      = msg.LowLimitPrice; 
                row[MsgMXSchema.PleUpLmtStep]       = msg.PleUpLmtStep; 
                row[MsgMXSchema.PleLwLmtStep]       = msg.PleLwLmtStep; 

                // Footer
                if (long.TryParse(msg.CheckSum, out long parsedCheckSum))
                {
                    row[BaseMessageSchema.Checksum] = ToDbNull(parsedCheckSum);
                }
                else
                {
                    row[BaseMessageSchema.Checksum] = DBNull.Value;
                }

                row[BaseMessageSchema.CreateTime]   = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
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
