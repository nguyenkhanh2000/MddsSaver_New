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

                        // var t when t == typeof(EPriceRecovery) =>
                        //    BulkInsertPriceRecoveryAsync(items.Cast<EPriceRecovery>().ToList(), stoppingToken),

                        // ... và cho tất cả các loại message khác ...

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
                                //bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                                if (col.ColumnName.Equals("date", StringComparison.OrdinalIgnoreCase))
                                {
                                    // Cột "date" phải được ánh xạ với dấu ngoặc kép
                                    // để Oracle hiểu nó là case-sensitive (chữ thường).
                                    bulkCopy.ColumnMappings.Add(col.ColumnName, "DATE");
                                }
                                else
                                {
                                    // Các cột khác ánh xạ bình thường (chữ thường trong C#
                                    // sẽ tự động khớp với chữ HOA trong Oracle)
                                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                                }
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
                                //bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                                if (col.ColumnName.Equals("date", StringComparison.OrdinalIgnoreCase))
                                {
                                    // Cột "date" phải được ánh xạ với dấu ngoặc kép
                                    // để Oracle hiểu nó là case-sensitive (chữ thường).
                                    bulkCopy.ColumnMappings.Add(col.ColumnName, "\"date\"");
                                }
                                else
                                {
                                    // Các cột khác ánh xạ bình thường (chữ thường trong C#
                                    // sẽ tự động khớp với chữ HOA trong Oracle)
                                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                                }
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
        private DataTable CreateSecurityDefinitionDataTable(List<ESecurityDefinition> definitions)
        {
            var dt = new DataTable();

            // 1. ĐỊNH NGHĨA CỘT (DATATABLE)
            // Tên cột ở đây sẽ được dùng trong phần "row[...]"
            // và là "SourceColumn" trong ColumnMappings

            // Header
            dt.Columns.Add("BeginString", typeof(string));
            dt.Columns.Add("BodyLength", typeof(int));
            dt.Columns.Add("MsgType", typeof(string));
            dt.Columns.Add("SenderCompID", typeof(string));
            dt.Columns.Add("TargetCompID", typeof(string));
            dt.Columns.Add("MsgSeqNum", typeof(long));
            dt.Columns.Add("SendingTime", typeof(DateTime));
            dt.Columns.Add("MarketID", typeof(string));
            dt.Columns.Add("BoardID", typeof(string));
            dt.Columns.Add("TotNumReports", typeof(long));
            dt.Columns.Add("SecurityExchange", typeof(string));

            // Payload (Security Definition)
            dt.Columns.Add("Symbol", typeof(string));
            dt.Columns.Add("TickerCode", typeof(string));
            dt.Columns.Add("SymbolShortCode", typeof(string));
            dt.Columns.Add("SymbolName", typeof(string));
            //dt.Columns.Add("SymbolEnglishName", typeof(string));
            dt.Columns.Add("ProductID", typeof(string));
            dt.Columns.Add("ProductGrpID", typeof(string));
            dt.Columns.Add("SecurityGroupID", typeof(string));
            dt.Columns.Add("PutOrCall", typeof(string));
            dt.Columns.Add("ExerciseStyle", typeof(string));
            dt.Columns.Add("MaturityMonthYear", typeof(string));
            dt.Columns.Add("MaturityDate", typeof(string));
            dt.Columns.Add("Issuer", typeof(string));
            dt.Columns.Add("IssueDate", typeof(string));
            dt.Columns.Add("ContractMultiplier", typeof(decimal));
            dt.Columns.Add("CouponRate", typeof(decimal));
            dt.Columns.Add("Currency", typeof(string));
            dt.Columns.Add("ListedShares", typeof(long));
            dt.Columns.Add("HighLimitPrice", typeof(decimal));
            dt.Columns.Add("LowLimitPrice", typeof(decimal));
            dt.Columns.Add("StrikePrice", typeof(decimal));
            dt.Columns.Add("SecurityStatus", typeof(string));
            dt.Columns.Add("ContractSize", typeof(decimal));
            dt.Columns.Add("SettlMethod", typeof(string));
            dt.Columns.Add("Yield", typeof(decimal));
            dt.Columns.Add("ReferencePrice", typeof(decimal));
            dt.Columns.Add("EvaluationPrice", typeof(decimal));
            dt.Columns.Add("HgstOrderPrice", typeof(decimal));
            dt.Columns.Add("LwstOrderPrice", typeof(decimal));
            dt.Columns.Add("PrevClosePx", typeof(decimal));
            dt.Columns.Add("SymbolCloseInfoPxType", typeof(string));
            dt.Columns.Add("FirstTradingDate", typeof(string));
            dt.Columns.Add("FinalTradeDate", typeof(string));
            dt.Columns.Add("FinalSettleDate", typeof(string));
            dt.Columns.Add("ListingDate", typeof(string));
            dt.Columns.Add("RETriggeringConditionCode", typeof(string)); // Property `RandomEndTriggeringConditionCode`
            dt.Columns.Add("ExClassType", typeof(string));
            dt.Columns.Add("VWAP", typeof(decimal));
            dt.Columns.Add("SymbolAdminStatusCode", typeof(string));
            dt.Columns.Add("SymbolTradingMethodSC", typeof(string)); // Property `SymbolTradingMethodStatusCode`
            dt.Columns.Add("SymbolTradingSantionSC", typeof(string)); // Property `SymbolTradingSantionStatusCode`
            dt.Columns.Add("SectorTypeCode", typeof(string));
            dt.Columns.Add("RedumptionDate", typeof(string));

            // Footer
            dt.Columns.Add("CheckSum", typeof(long));
            dt.Columns.Add("date", typeof(DateTime));

            // 2. ĐỔ DỮ LIỆU TỪ LIST VÀO DATATABLE
            foreach (var def in definitions)
            {
                var row = dt.NewRow();

                // Sử dụng (object)def.Property ?? DBNull.Value để xử lý null cho các kiểu string và decimal?
                // Các kiểu non-nullable (int, long, DateTime) gán trực tiếp

                row["BeginString"] = (object)def.BeginString ?? DBNull.Value;
                row["BodyLength"] = (int)def.BodyLength;
                row["MsgType"] = (object)def.MsgType ?? DBNull.Value;
                row["SenderCompID"] = (object)def.SenderCompID ?? DBNull.Value;
                row["TargetCompID"] = (object)def.TargetCompID ?? DBNull.Value;
                row["MsgSeqNum"] = def.MsgSeqNum;
                //row["SendingTime"] = DateTime.ParseExact(def.SendingTime, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                if (DateTime.TryParseExact(def.SendingTime, "yyyyMMdd HH:mm:ss.fff",
                        System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.None,
                        out DateTime sendingTime))
                {
                    row["SendingTime"] = sendingTime;
                }
                else
                {
                    // Nếu lỗi format, để NULL
                    row["SendingTime"] = DBNull.Value;
                }
                row["MarketID"] = (object)def.MarketID ?? DBNull.Value;
                row["BoardID"] = (object)def.BoardID ?? DBNull.Value;
                row["TotNumReports"] = def.TotNumReports; // Giả định non-null
                row["SecurityExchange"] = (object)def.SecurityExchange ?? DBNull.Value;

                row["Symbol"] = (object)def.Symbol ?? DBNull.Value;
                row["TickerCode"] = (object)def.TickerCode ?? DBNull.Value;
                row["SymbolShortCode"] = (object)def.SymbolShortCode ?? DBNull.Value;
                row["SymbolName"] = (object)def.SymbolName ?? DBNull.Value;
                //row["SymbolEnglishName"] = (object)def.SymbolEnglishName ?? DBNull.Value;
                row["ProductID"] = (object)def.ProductID ?? DBNull.Value;
                row["ProductGrpID"] = (object)def.ProductGrpID ?? DBNull.Value;
                row["SecurityGroupID"] = (object)def.SecurityGroupID ?? DBNull.Value;
                row["PutOrCall"] = (object)def.PutOrCall ?? DBNull.Value;
                row["ExerciseStyle"] = (object)def.ExerciseStyle ?? DBNull.Value;
                row["MaturityMonthYear"] = (object)def.MaturityMonthYear ?? DBNull.Value;
                row["MaturityDate"] = (object)def.MaturityDate ?? DBNull.Value;
                row["Issuer"] = (object)def.Issuer ?? DBNull.Value;
                row["IssueDate"] = (object)def.IssueDate ?? DBNull.Value;
                row["ContractMultiplier"] = (object)def.ContractMultiplier ?? DBNull.Value;
                row["CouponRate"] = (object)def.CouponRate ?? DBNull.Value;
                row["Currency"] = (object)def.Currency ?? DBNull.Value;
                row["ListedShares"] = def.ListedShares; // Giả định non-null
                row["HighLimitPrice"] = (object)def.HighLimitPrice ?? DBNull.Value;
                row["LowLimitPrice"] = (object)def.LowLimitPrice ?? DBNull.Value;
                row["StrikePrice"] = (object)def.StrikePrice ?? DBNull.Value;
                row["SecurityStatus"] = (object)def.SecurityStatus ?? DBNull.Value;
                row["ContractSize"] = (object)def.ContractSize ?? DBNull.Value;
                row["SettlMethod"] = (object)def.SettlMethod ?? DBNull.Value;
                row["Yield"] = (object)def.Yield ?? DBNull.Value;
                row["ReferencePrice"] = (object)def.ReferencePrice ?? DBNull.Value;
                row["EvaluationPrice"] = (object)def.EvaluationPrice ?? DBNull.Value;
                row["HgstOrderPrice"] = (object)def.HgstOrderPrice ?? DBNull.Value;
                row["LwstOrderPrice"] = (object)def.LwstOrderPrice ?? DBNull.Value;
                row["PrevClosePx"] = (object)def.PrevClosePx ?? DBNull.Value;
                row["SymbolCloseInfoPxType"] = (object)def.SymbolCloseInfoPxType ?? DBNull.Value;
                row["FirstTradingDate"] = (object)def.FirstTradingDate ?? DBNull.Value;
                row["FinalTradeDate"] = (object)def.FinalTradeDate ?? DBNull.Value;
                row["FinalSettleDate"] = (object)def.FinalSettleDate ?? DBNull.Value;
                row["ListingDate"] = (object)def.ListingDate ?? DBNull.Value;
                row["RETriggeringConditionCode"] = (object)def.RandomEndTriggeringConditionCode ?? DBNull.Value;
                row["ExClassType"] = (object)def.ExClassType ?? DBNull.Value;
                row["VWAP"] = (object)def.VWAP ?? DBNull.Value;
                row["SymbolAdminStatusCode"] = (object)def.SymbolAdminStatusCode ?? DBNull.Value;
                row["SymbolTradingMethodSC"] = (object)def.SymbolTradingMethodStatusCode ?? DBNull.Value;
                row["SymbolTradingSantionSC"] = (object)def.SymbolTradingSantionStatusCode ?? DBNull.Value;
                row["SectorTypeCode"] = (object)def.SectorTypeCode ?? DBNull.Value;
                row["RedumptionDate"] = (object)def.RedumptionDate ?? DBNull.Value;

                row["CheckSum"] = long.Parse(def.CheckSum);
                row["date"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        private DataTable CreatePriceDataTable(List<EPrice> prices)
        {
            DataTable dt = new DataTable();

            // --- 1. ĐỊNH NGHĨA CỘT (Khớp với msg_x) ---

            // Thông tin header
            dt.Columns.Add("beginstring", typeof(string));
            dt.Columns.Add("bodylength", typeof(int)); // Sửa: kiểu int
            dt.Columns.Add("msgtype", typeof(string));
            dt.Columns.Add("sendercompid", typeof(string));
            dt.Columns.Add("targetcompid", typeof(string));
            dt.Columns.Add("msgseqnum", typeof(long)); // Sửa: kiểu long
            dt.Columns.Add("sendingtime", typeof(DateTime));
            dt.Columns.Add("marketid", typeof(string));
            dt.Columns.Add("boardid", typeof(string));
            dt.Columns.Add("tradingsessionid", typeof(string));
            dt.Columns.Add("symbol", typeof(string));

            // Thêm: tradedate và transacttime
            dt.Columns.Add("tradedate", typeof(DateTime));
            dt.Columns.Add("transacttime", typeof(string));

            // Các cột thống kê (Sửa kiểu dữ liệu)
            dt.Columns.Add("totalvolumetraded", typeof(long));
            dt.Columns.Add("grosstradeamt", typeof(decimal));
            dt.Columns.Add("selltotorderqty", typeof(long));
            dt.Columns.Add("buytotorderqty", typeof(long));
            dt.Columns.Add("sellvalidordercnt", typeof(long));
            dt.Columns.Add("buyvalidordercnt", typeof(long));

            // Thêm các cột giá 1-10 (Đã đổi tên và sửa kiểu)
            for (int i = 1; i <= 10; i++)
            {
                dt.Columns.Add($"bp{i}", typeof(decimal));
                dt.Columns.Add($"bq{i}", typeof(long));
                dt.Columns.Add($"bp{i}_noo", typeof(long));
                dt.Columns.Add($"bp{i}_mdey", typeof(decimal));
                dt.Columns.Add($"bp{i}_mdemms", typeof(long));
                dt.Columns.Add($"bp{i}_mdepno", typeof(int)); // Cột này luôn NULL

                dt.Columns.Add($"sp{i}", typeof(decimal));
                dt.Columns.Add($"sq{i}", typeof(long));
                dt.Columns.Add($"sp{i}_noo", typeof(long));
                dt.Columns.Add($"sp{i}_mdey", typeof(decimal));
                dt.Columns.Add($"sp{i}_mdemms", typeof(long));
                dt.Columns.Add($"sp{i}_mdepno", typeof(int)); // Cột này luôn NULL
            }

            // Thêm: Các cột giá cuối (mp, mq, op, lp, hp)
            dt.Columns.Add("mp", typeof(decimal));
            dt.Columns.Add("mq", typeof(long));
            dt.Columns.Add("op", typeof(decimal));
            dt.Columns.Add("lp", typeof(decimal));
            dt.Columns.Add("hp", typeof(decimal));

            // Cột cuối
            dt.Columns.Add("checksum", typeof(long));
            dt.Columns.Add("date", typeof(DateTime));

            // --- 2. LOGIC HELPER ---
            // Giả lập logic của ToDbNullIfInvalid
            object longToDbNull(long val) => val != -9999999L ? (object)val : DBNull.Value;
            //object doubleToDbNull(decimal val) => val != -9999999m ? (object)val : DBNull.Value;
            object doubleToDbNull(double val)
            {
                // Dùng Math.Abs để so sánh double an toàn hơn
                if (Math.Abs(val - (-9999999.0d)) < 0.0001d)
                {
                    return DBNull.Value;
                }
                return (object)Convert.ToDecimal(val); // Chuyển sang decimal
            }

            // --- 3. ĐIỀN DỮ LIỆU (Khớp với logic COPY) ---
            foreach (var price in prices)
            {
                DataRow row = dt.NewRow();

                row["beginstring"] = price.BeginString;
                row["bodylength"] = (int)price.BodyLength; // Khớp: (int) cast
                row["msgtype"] = price.MsgType;
                row["sendercompid"] = price.SenderCompID;
                row["targetcompid"] = price.TargetCompID;
                row["msgseqnum"] = price.MsgSeqNum;

                // Logic Parse SendingTime
                if (DateTime.TryParseExact(price.SendingTime, "yyyyMMdd HH:mm:ss.fff",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime sendingTime))
                {
                    row["sendingtime"] = sendingTime;
                }
                else
                {
                    row["sendingtime"] = DBNull.Value;
                }

                row["marketid"] = price.MarketID;
                row["boardid"] = price.BoardID;
                row["tradingsessionid"] = price.TradingSessionID;
                row["symbol"] = price.Symbol;

                // Logic Parse TradeDate
                if (DateTime.TryParseExact(price.TradeDate, "yyyyMMdd",
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime tradeDate))
                {
                    row["tradedate"] = tradeDate;
                }
                else
                {
                    row["tradedate"] = DBNull.Value;
                }

                row["transacttime"] = price.TransactTime;

                // Các trường thống kê
                row["totalvolumetraded"] = longToDbNull(price.TotalVolumeTraded);
                row["grosstradeamt"] = doubleToDbNull(price.GrossTradeAmt);
                row["selltotorderqty"] = longToDbNull(price.SellTotOrderQty);
                row["buytotorderqty"] = longToDbNull(price.BuyTotOrderQty);
                row["sellvalidordercnt"] = longToDbNull(price.SellValidOrderCnt);
                row["buyvalidordercnt"] = longToDbNull(price.BuyValidOrderCnt);

                // Cấp 1
                row["bp1"] = doubleToDbNull(price.BuyPrice1);
                row["bq1"] = longToDbNull(price.BuyQuantity1);
                row["bp1_noo"] = longToDbNull((long)price.BuyPrice1_NOO);
                row["bp1_mdey"] = doubleToDbNull(price.BuyPrice1_MDEY);
                row["bp1_mdemms"] = longToDbNull(price.BuyPrice1_MDEMMS);
                row["bp1_mdepno"] = DBNull.Value; // Khớp: Luôn NULL
                row["sp1"] = doubleToDbNull(price.SellPrice1);
                row["sq1"] = longToDbNull(price.SellQuantity1);
                row["sp1_noo"] = longToDbNull((long)price.SellPrice1_NOO);
                row["sp1_mdey"] = doubleToDbNull(price.SellPrice1_MDEY);
                row["sp1_mdemms"] = longToDbNull(price.SellPrice1_MDEMMS);
                row["sp1_mdepno"] = DBNull.Value; // Khớp: Luôn NULL

                // Cấp 2
                row["bp2"] = doubleToDbNull(price.BuyPrice2);
                row["bq2"] = longToDbNull(price.BuyQuantity2);
                row["bp2_noo"] = longToDbNull((long)price.BuyPrice2_NOO);
                row["bp2_mdey"] = doubleToDbNull(price.BuyPrice2_MDEY);
                row["bp2_mdemms"] = longToDbNull(price.BuyPrice2_MDEMMS);
                row["bp2_mdepno"] = DBNull.Value;
                row["sp2"] = doubleToDbNull(price.SellPrice2);
                row["sq2"] = longToDbNull(price.SellQuantity2);
                row["sp2_noo"] = longToDbNull((long)price.SellPrice2_NOO);
                row["sp2_mdey"] = doubleToDbNull(price.SellPrice2_MDEY);
                row["sp2_mdemms"] = longToDbNull(price.SellPrice2_MDEMMS);
                row["sp2_mdepno"] = DBNull.Value;

                // Cấp 3
                row["bp3"] = doubleToDbNull(price.BuyPrice3);
                row["bq3"] = longToDbNull(price.BuyQuantity3);
                row["bp3_noo"] = longToDbNull((long)price.BuyPrice3_NOO);
                row["bp3_mdey"] = doubleToDbNull(price.BuyPrice3_MDEY);
                row["bp3_mdemms"] = longToDbNull(price.BuyPrice3_MDEMMS);
                row["bp3_mdepno"] = DBNull.Value;
                row["sp3"] = doubleToDbNull(price.SellPrice3);
                row["sq3"] = longToDbNull(price.SellQuantity3);
                row["sp3_noo"] = longToDbNull((long)price.SellPrice3_NOO);
                row["sp3_mdey"] = doubleToDbNull(price.SellPrice3_MDEY);
                row["sp3_mdemms"] = longToDbNull(price.SellPrice3_MDEMMS);
                row["sp3_mdepno"] = DBNull.Value;

                // Cấp 4
                row["bp4"] = doubleToDbNull(price.BuyPrice4);
                row["bq4"] = longToDbNull(price.BuyQuantity4);
                row["bp4_noo"] = longToDbNull((long)price.BuyPrice4_NOO);
                row["bp4_mdey"] = doubleToDbNull(price.BuyPrice4_MDEY);
                row["bp4_mdemms"] = longToDbNull(price.BuyPrice4_MDEMMS);
                row["bp4_mdepno"] = DBNull.Value;
                row["sp4"] = doubleToDbNull(price.SellPrice4);
                row["sq4"] = longToDbNull(price.SellQuantity4);
                row["sp4_noo"] = longToDbNull((long)price.SellPrice4_NOO);
                row["sp4_mdey"] = doubleToDbNull(price.SellPrice4_MDEY);
                row["sp4_mdemms"] = longToDbNull(price.SellPrice4_MDEMMS);
                row["sp4_mdepno"] = DBNull.Value;

                // Cấp 5
                row["bp5"] = doubleToDbNull(price.BuyPrice5);
                row["bq5"] = longToDbNull(price.BuyQuantity5);
                row["bp5_noo"] = longToDbNull((long)price.BuyPrice5_NOO);
                row["bp5_mdey"] = doubleToDbNull(price.BuyPrice5_MDEY);
                row["bp5_mdemms"] = longToDbNull(price.BuyPrice5_MDEMMS);
                row["bp5_mdepno"] = DBNull.Value;
                row["sp5"] = doubleToDbNull(price.SellPrice5);
                row["sq5"] = longToDbNull(price.SellQuantity5);
                row["sp5_noo"] = longToDbNull((long)price.SellPrice5_NOO);
                row["sp5_mdey"] = doubleToDbNull(price.SellPrice5_MDEY);
                row["sp5_mdemms"] = longToDbNull(price.SellPrice5_MDEMMS);
                row["sp5_mdepno"] = DBNull.Value;

                // Cấp 6
                row["bp6"] = doubleToDbNull(price.BuyPrice6);
                row["bq6"] = longToDbNull(price.BuyQuantity6);
                row["bp6_noo"] = longToDbNull((long)price.BuyPrice6_NOO);
                row["bp6_mdey"] = doubleToDbNull(price.BuyPrice6_MDEY);
                row["bp6_mdemms"] = longToDbNull(price.BuyPrice6_MDEMMS);
                row["bp6_mdepno"] = DBNull.Value;
                row["sp6"] = doubleToDbNull(price.SellPrice6);
                row["sq6"] = longToDbNull(price.SellQuantity6);
                row["sp6_noo"] = longToDbNull((long)price.SellPrice6_NOO);
                row["sp6_mdey"] = doubleToDbNull(price.SellPrice6_MDEY);
                row["sp6_mdemms"] = longToDbNull(price.SellPrice6_MDEMMS);
                row["sp6_mdepno"] = DBNull.Value;

                // Cấp 7
                row["bp7"] = doubleToDbNull(price.BuyPrice7);
                row["bq7"] = longToDbNull(price.BuyQuantity7);
                row["bp7_noo"] = longToDbNull((long)price.BuyPrice7_NOO);
                row["bp7_mdey"] = doubleToDbNull(price.BuyPrice7_MDEY);
                row["bp7_mdemms"] = longToDbNull(price.BuyPrice7_MDEMMS);
                row["bp7_mdepno"] = DBNull.Value;
                row["sp7"] = doubleToDbNull(price.SellPrice7);
                row["sq7"] = longToDbNull(price.SellQuantity7);
                row["sp7_noo"] = longToDbNull((long)price.SellPrice7_NOO);
                row["sp7_mdey"] = doubleToDbNull(price.SellPrice7_MDEY);
                row["sp7_mdemms"] = longToDbNull(price.SellPrice7_MDEMMS);
                row["sp7_mdepno"] = DBNull.Value;

                // Cấp 8
                row["bp8"] = doubleToDbNull(price.BuyPrice8);
                row["bq8"] = longToDbNull(price.BuyQuantity8);
                row["bp8_noo"] = longToDbNull((long)price.BuyPrice8_NOO);
                row["bp8_mdey"] = doubleToDbNull(price.BuyPrice8_MDEY);
                row["bp8_mdemms"] = longToDbNull(price.BuyPrice8_MDEMMS);
                row["bp8_mdepno"] = DBNull.Value;
                row["sp8"] = doubleToDbNull(price.SellPrice8);
                row["sq8"] = longToDbNull(price.SellQuantity8);
                row["sp8_noo"] = longToDbNull((long)price.SellPrice8_NOO);
                row["sp8_mdey"] = doubleToDbNull(price.SellPrice8_MDEY);
                row["sp8_mdemms"] = longToDbNull(price.SellPrice8_MDEMMS);
                row["sp8_mdepno"] = DBNull.Value;

                // Cấp 9
                row["bp9"] = doubleToDbNull(price.BuyPrice9);
                row["bq9"] = longToDbNull(price.BuyQuantity9);
                row["bp9_noo"] = longToDbNull((long)price.BuyPrice9_NOO);
                row["bp9_mdey"] = doubleToDbNull(price.BuyPrice9_MDEY);
                row["bp9_mdemms"] = longToDbNull(price.BuyPrice9_MDEMMS);
                row["bp9_mdepno"] = DBNull.Value;
                row["sp9"] = doubleToDbNull(price.SellPrice9);
                row["sq9"] = longToDbNull(price.SellQuantity9);
                row["sp9_noo"] = longToDbNull((long)price.SellPrice9_NOO);
                row["sp9_mdey"] = doubleToDbNull(price.SellPrice9_MDEY);
                row["sp9_mdemms"] = longToDbNull(price.SellPrice9_MDEMMS);
                row["sp9_mdepno"] = DBNull.Value;

                // Cấp 10
                row["bp10"] = doubleToDbNull(price.BuyPrice10);
                row["bq10"] = longToDbNull(price.BuyQuantity10);
                row["bp10_noo"] = longToDbNull((long)price.BuyPrice10_NOO);
                row["bp10_mdey"] = doubleToDbNull(price.BuyPrice10_MDEY);
                row["bp10_mdemms"] = longToDbNull(price.BuyPrice10_MDEMMS);
                row["bp10_mdepno"] = DBNull.Value;
                row["sp10"] = doubleToDbNull(price.SellPrice10);
                row["sq10"] = longToDbNull(price.SellQuantity10);
                row["sp10_noo"] = longToDbNull((long)price.SellPrice10_NOO);
                row["sp10_mdey"] = doubleToDbNull(price.SellPrice10_MDEY);
                row["sp10_mdemms"] = longToDbNull(price.SellPrice10_MDEMMS);
                row["sp10_mdepno"] = DBNull.Value;

                // Các trường giá cuối
                row["mp"] = doubleToDbNull(price.MatchPrice);
                row["mq"] = longToDbNull(price.MatchQuantity);
                row["op"] = doubleToDbNull(price.OpenPrice);
                row["lp"] = doubleToDbNull(price.LowestPrice);
                row["hp"] = doubleToDbNull(price.HighestPrice);

                // Cột cuối (khớp logic COPY)
                row["checksum"] = longToDbNull(long.Parse(price.CheckSum));
                row["date"] = DateTime.Now;

                dt.Rows.Add(row);
            }

            return dt;
        }
        //private DataTable CreatePriceDataTable(List<EPrice> lst_price)
        //{
        //    DataTable dt = new DataTable();

        //    dt.Columns.Add("BeginString", typeof(string));
        //    dt.Columns.Add("BodyLength", typeof(string));
        //    dt.Columns.Add("MsgType", typeof(string));
        //    dt.Columns.Add("SenderCompID", typeof(string));
        //    dt.Columns.Add("TargetCompID", typeof(string));
        //    dt.Columns.Add("MsgSeqNum", typeof(string));
        //    dt.Columns.Add("SendingTime", typeof(DateTime));
        //    dt.Columns.Add("CreateTime", typeof(DateTime));
        //    dt.Columns.Add("MarketID", typeof(string));
        //    dt.Columns.Add("BoardID", typeof(string));
        //    dt.Columns.Add("TradingSessionID", typeof(string));
        //    dt.Columns.Add("Symbol", typeof(string));
        //    dt.Columns.Add("TradeDate", typeof(string));
        //    dt.Columns.Add("TransactTime", typeof(string));
        //    dt.Columns.Add("TotalVolumeTraded", typeof(string));
        //    dt.Columns.Add("GrossTradeAmt", typeof(string));
        //    dt.Columns.Add("BuyTotOrderQty", typeof(string));
        //    dt.Columns.Add("BuyValidOrderCnt", typeof(string));
        //    dt.Columns.Add("SellTotOrderQty", typeof(string));
        //    dt.Columns.Add("SellValidOrderCnt", typeof(string));
        //    dt.Columns.Add("NoMDEntries", typeof(string));

        //    dt.Columns.Add("BuyPrice1", typeof(string));
        //    dt.Columns.Add("BuyQuantity1", typeof(string));
        //    dt.Columns.Add("BuyPrice1_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice1_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice1_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice2", typeof(string));
        //    dt.Columns.Add("BuyQuantity2", typeof(string));
        //    dt.Columns.Add("BuyPrice2_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice2_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice2_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice3", typeof(string));
        //    dt.Columns.Add("BuyQuantity3", typeof(string));
        //    dt.Columns.Add("BuyPrice3_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice3_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice3_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice4", typeof(string));
        //    dt.Columns.Add("BuyQuantity4", typeof(string));
        //    dt.Columns.Add("BuyPrice4_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice4_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice4_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice5", typeof(string));
        //    dt.Columns.Add("BuyQuantity5", typeof(string));
        //    dt.Columns.Add("BuyPrice5_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice5_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice5_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice6", typeof(string));
        //    dt.Columns.Add("BuyQuantity6", typeof(string));
        //    dt.Columns.Add("BuyPrice6_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice6_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice6_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice7", typeof(string));
        //    dt.Columns.Add("BuyQuantity7", typeof(string));
        //    dt.Columns.Add("BuyPrice7_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice7_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice7_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice8", typeof(string));
        //    dt.Columns.Add("BuyQuantity8", typeof(string));
        //    dt.Columns.Add("BuyPrice8_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice8_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice8_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice9", typeof(string));
        //    dt.Columns.Add("BuyQuantity9", typeof(string));
        //    dt.Columns.Add("BuyPrice9_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice9_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice9_MDEMMS", typeof(string));
        //    dt.Columns.Add("BuyPrice10", typeof(string));
        //    dt.Columns.Add("BuyQuantity10", typeof(string));
        //    dt.Columns.Add("BuyPrice10_NOO", typeof(string));
        //    dt.Columns.Add("BuyPrice10_MDEY", typeof(string));
        //    dt.Columns.Add("BuyPrice10_MDEMMS", typeof(string));


        //    dt.Columns.Add("SellPrice1", typeof(string));
        //    dt.Columns.Add("SellQuantity1", typeof(string));
        //    dt.Columns.Add("SellPrice1_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice1_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice1_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice2", typeof(string));
        //    dt.Columns.Add("SellQuantity2", typeof(string));
        //    dt.Columns.Add("SellPrice2_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice2_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice2_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice3", typeof(string));
        //    dt.Columns.Add("SellQuantity3", typeof(string));
        //    dt.Columns.Add("SellPrice3_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice3_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice3_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice4", typeof(string));
        //    dt.Columns.Add("SellQuantity4", typeof(string));
        //    dt.Columns.Add("SellPrice4_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice4_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice4_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice5", typeof(string));
        //    dt.Columns.Add("SellQuantity5", typeof(string));
        //    dt.Columns.Add("SellPrice5_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice5_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice5_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice6", typeof(string));
        //    dt.Columns.Add("SellQuantity6", typeof(string));
        //    dt.Columns.Add("SellPrice6_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice6_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice6_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice7", typeof(string));
        //    dt.Columns.Add("SellQuantity7", typeof(string));
        //    dt.Columns.Add("SellPrice7_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice7_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice7_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice8", typeof(string));
        //    dt.Columns.Add("SellQuantity8", typeof(string));
        //    dt.Columns.Add("SellPrice8_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice8_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice8_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice9", typeof(string));
        //    dt.Columns.Add("SellQuantity9", typeof(string));
        //    dt.Columns.Add("SellPrice9_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice9_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice9_MDEMMS", typeof(string));
        //    dt.Columns.Add("SellPrice10", typeof(string));
        //    dt.Columns.Add("SellQuantity10", typeof(string));
        //    dt.Columns.Add("SellPrice10_NOO", typeof(string));
        //    dt.Columns.Add("SellPrice10_MDEY", typeof(string));
        //    dt.Columns.Add("SellPrice10_MDEMMS", typeof(string));
        //    dt.Columns.Add("MatchPrice", typeof(string));
        //    dt.Columns.Add("MatchQuantity", typeof(string));
        //    dt.Columns.Add("OpenPrice", typeof(string));
        //    dt.Columns.Add("ClosePrice", typeof(string));
        //    dt.Columns.Add("HighestPrice", typeof(string));
        //    dt.Columns.Add("LowestPrice", typeof(string));
        //    //dt.Columns.Add("RepeatingDataFix", typeof(string));
        //    //dt.Columns.Add("RepeatingDataJson", typeof(string));
        //    dt.Columns.Add("CheckSum", typeof(string));
        //    foreach (var item in lst_price)
        //    {
        //        DataRow row = dt.NewRow();
        //        row["BeginString"] = item.BeginString;
        //        row["BodyLength"] = item.BodyLength;
        //        row["MsgType"] = item.MsgType;
        //        row["SenderCompID"] = item.SenderCompID;
        //        row["TargetCompID"] = item.TargetCompID;
        //        row["MsgSeqNum"] = item.MsgSeqNum;
        //        // Parse chuỗi "20250404 09:38:55.584" thành DateTime
        //        if (DateTime.TryParseExact(item.SendingTime, "yyyyMMdd HH:mm:ss.fff",
        //            System.Globalization.CultureInfo.InvariantCulture,
        //            System.Globalization.DateTimeStyles.None,
        //            out DateTime sendingTime))
        //        {
        //            row["SendingTime"] = sendingTime;
        //        }
        //        else
        //        {
        //            // Nếu lỗi format, để NULL
        //            row["SendingTime"] = DBNull.Value;
        //        }
        //        row["CreateTime"] = DateTime.Now;
        //        row["MarketID"] = item.MarketID;
        //        row["BoardID"] = item.BoardID;
        //        row["TradingSessionID"] = item.TradingSessionID;
        //        row["Symbol"] = item.Symbol;
        //        row["TradeDate"] = item.TradeDate;
        //        row["TransactTime"] = item.TransactTime;

        //        row["TotalVolumeTraded"] = item.TotalVolumeTraded != -9999999 ? (object)item.TotalVolumeTraded : DBNull.Value;
        //        row["GrossTradeAmt"] = item.GrossTradeAmt != 0 ? (object)item.GrossTradeAmt : DBNull.Value;
        //        row["BuyTotOrderQty"] = item.BuyTotOrderQty != -9999999 ? (object)item.BuyTotOrderQty : DBNull.Value;
        //        row["BuyValidOrderCnt"] = item.BuyValidOrderCnt != -9999999 ? (object)item.BuyValidOrderCnt : DBNull.Value;
        //        row["SellTotOrderQty"] = item.SellTotOrderQty != -9999999 ? (object)item.SellTotOrderQty : DBNull.Value;
        //        row["SellValidOrderCnt"] = item.SellValidOrderCnt != -9999999 ? (object)item.SellValidOrderCnt : DBNull.Value;
        //        row["NoMDEntries"] = item.NoMDEntries != -9999999 ? (object)item.NoMDEntries : DBNull.Value;


        //        row["BuyPrice1"] = item.BuyPrice1 != -9999999 ? (object)item.BuyPrice1 : DBNull.Value;
        //        row["BuyQuantity1"] = item.BuyQuantity1 != -9999999 ? (object)item.BuyQuantity1 : DBNull.Value;
        //        row["BuyPrice1_NOO"] = item.BuyPrice1_NOO;
        //        row["BuyPrice1_MDEY"] = item.BuyPrice1_MDEY;
        //        row["BuyPrice1_MDEMMS"] = item.BuyPrice1_MDEMMS;
        //        row["BuyPrice2"] = item.BuyPrice2 != -9999999 ? (object)item.BuyPrice2 : DBNull.Value;
        //        row["BuyQuantity2"] = item.BuyQuantity2 != -9999999 ? (object)item.BuyQuantity2 : DBNull.Value;
        //        row["BuyPrice2_NOO"] = item.BuyPrice2_NOO;
        //        row["BuyPrice2_MDEY"] = item.BuyPrice2_MDEY;
        //        row["BuyPrice2_MDEMMS"] = item.BuyPrice2_MDEMMS;
        //        row["BuyPrice3"] = item.BuyPrice3 != -9999999 ? (object)item.BuyPrice3 : DBNull.Value;
        //        row["BuyQuantity3"] = item.BuyQuantity3 != -9999999 ? (object)item.BuyQuantity3 : DBNull.Value;
        //        row["BuyPrice3_NOO"] = item.BuyPrice3_NOO;
        //        row["BuyPrice3_MDEY"] = item.BuyPrice3_MDEY;
        //        row["BuyPrice3_MDEMMS"] = item.BuyPrice3_MDEMMS;

        //        row["BuyPrice4"] = item.BuyPrice4 != -9999999 ? (object)item.BuyPrice4 : DBNull.Value;
        //        row["BuyQuantity4"] = item.BuyQuantity4 != -9999999 ? (object)item.BuyQuantity4 : DBNull.Value;
        //        row["BuyPrice4_NOO"] = item.BuyPrice4_NOO;
        //        row["BuyPrice4_MDEY"] = item.BuyPrice4_MDEY;
        //        row["BuyPrice4_MDEMMS"] = item.BuyPrice4_MDEMMS;
        //        row["BuyPrice5"] = item.BuyPrice5 != -9999999 ? (object)item.BuyPrice5 : DBNull.Value;
        //        row["BuyQuantity5"] = item.BuyQuantity5 != -9999999 ? (object)item.BuyQuantity5 : DBNull.Value;
        //        row["BuyPrice5_NOO"] = item.BuyPrice5_NOO;
        //        row["BuyPrice5_MDEY"] = item.BuyPrice5_MDEY;
        //        row["BuyPrice5_MDEMMS"] = item.BuyPrice5_MDEMMS;
        //        row["BuyPrice6"] = item.BuyPrice6 != -9999999 ? (object)item.BuyPrice6 : DBNull.Value;
        //        row["BuyQuantity6"] = item.BuyQuantity6 != -9999999 ? (object)item.BuyQuantity6 : DBNull.Value;
        //        row["BuyPrice6_NOO"] = item.BuyPrice6_NOO;
        //        row["BuyPrice6_MDEY"] = item.BuyPrice6_MDEY;
        //        row["BuyPrice6_MDEMMS"] = item.BuyPrice6_MDEMMS;

        //        row["BuyPrice7"] = item.BuyPrice7 != -9999999 ? (object)item.BuyPrice7 : DBNull.Value;
        //        row["BuyQuantity7"] = item.BuyQuantity7 != -9999999 ? (object)item.BuyQuantity7 : DBNull.Value;
        //        row["BuyPrice7_NOO"] = item.BuyPrice7_NOO;
        //        row["BuyPrice7_MDEY"] = item.BuyPrice7_MDEY;
        //        row["BuyPrice7_MDEMMS"] = item.BuyPrice7_MDEMMS;
        //        row["BuyPrice8"] = item.BuyPrice8 != -9999999 ? (object)item.BuyPrice8 : DBNull.Value;
        //        row["BuyQuantity8"] = item.BuyQuantity8 != -9999999 ? (object)item.BuyQuantity8 : DBNull.Value;
        //        row["BuyPrice8_NOO"] = item.BuyPrice8_NOO;
        //        row["BuyPrice8_MDEY"] = item.BuyPrice8_MDEY;
        //        row["BuyPrice8_MDEMMS"] = item.BuyPrice8_MDEMMS;
        //        row["BuyPrice9"] = item.BuyPrice9 != -9999999 ? (object)item.BuyPrice9 : DBNull.Value;
        //        row["BuyQuantity9"] = item.BuyQuantity9 != -9999999 ? (object)item.BuyQuantity9 : DBNull.Value;
        //        row["BuyPrice9_NOO"] = item.BuyPrice9_NOO;
        //        row["BuyPrice9_MDEY"] = item.BuyPrice9_MDEY;
        //        row["BuyPrice9_MDEMMS"] = item.BuyPrice9_MDEMMS;
        //        row["BuyPrice10"] = item.BuyPrice10 != -9999999 ? (object)item.BuyPrice10 : DBNull.Value;
        //        row["BuyQuantity10"] = item.BuyQuantity10 != -9999999 ? (object)item.BuyQuantity10 : DBNull.Value;
        //        row["BuyPrice10_NOO"] = item.BuyPrice10_NOO;
        //        row["BuyPrice10_MDEY"] = item.BuyPrice10_MDEY;
        //        row["BuyPrice10_MDEMMS"] = item.BuyPrice10_MDEMMS;

        //        row["SellPrice1"] = item.SellPrice1 != -9999999 ? (object)item.SellPrice1 : DBNull.Value;
        //        row["SellQuantity1"] = item.SellQuantity1 != -9999999 ? (object)item.SellQuantity1 : DBNull.Value;
        //        row["SellPrice1_NOO"] = item.SellPrice1_NOO;
        //        row["SellPrice1_MDEY"] = item.SellPrice1_MDEY;
        //        row["SellPrice1_MDEMMS"] = item.SellPrice1_MDEMMS;
        //        row["SellPrice2"] = item.SellPrice2 != -9999999 ? (object)item.SellPrice2 : DBNull.Value;
        //        row["SellQuantity2"] = item.SellQuantity2 != -9999999 ? (object)item.SellQuantity2 : DBNull.Value;
        //        row["SellPrice2_NOO"] = item.SellPrice2_NOO;
        //        row["SellPrice2_MDEY"] = item.SellPrice2_MDEY;
        //        row["SellPrice2_MDEMMS"] = item.SellPrice2_MDEMMS;
        //        row["SellPrice3"] = item.SellPrice3 != -9999999 ? (object)item.SellPrice3 : DBNull.Value;
        //        row["SellQuantity3"] = item.SellQuantity3 != -9999999 ? (object)item.SellQuantity3 : DBNull.Value;
        //        row["SellPrice3_NOO"] = item.SellPrice3_NOO;
        //        row["SellPrice3_MDEY"] = item.SellPrice3_MDEY;
        //        row["SellPrice3_MDEMMS"] = item.SellPrice3_MDEMMS;
        //        row["SellPrice4"] = item.SellPrice4 != -9999999 ? (object)item.SellPrice4 : DBNull.Value;
        //        row["SellQuantity4"] = item.SellQuantity4 != -9999999 ? (object)item.SellQuantity4 : DBNull.Value;
        //        row["SellPrice4_NOO"] = item.SellPrice4_NOO;
        //        row["SellPrice4_MDEY"] = item.SellPrice4_MDEY;
        //        row["SellPrice4_MDEMMS"] = item.SellPrice4_MDEMMS;
        //        row["SellPrice5"] = item.SellPrice5 != -9999999 ? (object)item.SellPrice5 : DBNull.Value;
        //        row["SellQuantity5"] = item.SellQuantity5 != -9999999 ? (object)item.SellQuantity5 : DBNull.Value;
        //        row["SellPrice5_NOO"] = item.SellPrice5_NOO;
        //        row["SellPrice5_MDEY"] = item.SellPrice5_MDEY;
        //        row["SellPrice5_MDEMMS"] = item.SellPrice5_MDEMMS;
        //        row["SellPrice6"] = item.SellPrice6 != -9999999 ? (object)item.SellPrice6 : DBNull.Value;
        //        row["SellQuantity6"] = item.SellQuantity6 != -9999999 ? (object)item.SellQuantity6 : DBNull.Value;
        //        row["SellPrice6_NOO"] = item.SellPrice6_NOO;
        //        row["SellPrice6_MDEY"] = item.SellPrice6_MDEY;
        //        row["SellPrice6_MDEMMS"] = item.SellPrice6_MDEMMS;
        //        row["SellPrice7"] = item.SellPrice7 != -9999999 ? (object)item.SellPrice7 : DBNull.Value;
        //        row["SellQuantity7"] = item.SellQuantity7 != -9999999 ? (object)item.SellQuantity7 : DBNull.Value;
        //        row["SellPrice7_NOO"] = item.SellPrice7_NOO;
        //        row["SellPrice7_MDEY"] = item.SellPrice7_MDEY;
        //        row["SellPrice7_MDEMMS"] = item.SellPrice7_MDEMMS;
        //        row["SellPrice8"] = item.SellPrice8 != -9999999 ? (object)item.SellPrice8 : DBNull.Value;
        //        row["SellQuantity8"] = item.SellQuantity8 != -9999999 ? (object)item.SellQuantity8 : DBNull.Value;
        //        row["SellPrice8_NOO"] = item.SellPrice8_NOO;
        //        row["SellPrice8_MDEY"] = item.SellPrice8_MDEY;
        //        row["SellPrice8_MDEMMS"] = item.SellPrice8_MDEMMS;
        //        row["SellPrice9"] = item.SellPrice9 != -9999999 ? (object)item.SellPrice9 : DBNull.Value;
        //        row["SellQuantity9"] = item.SellQuantity9 != -9999999 ? (object)item.SellQuantity9 : DBNull.Value;
        //        row["SellPrice9_NOO"] = item.SellPrice9_NOO;
        //        row["SellPrice9_MDEY"] = item.SellPrice9_MDEY;
        //        row["SellPrice9_MDEMMS"] = item.SellPrice9_MDEMMS;
        //        row["SellPrice10"] = item.SellPrice10 != -9999999 ? (object)item.SellPrice10 : DBNull.Value;
        //        row["SellQuantity10"] = item.SellQuantity10 != -9999999 ? (object)item.SellQuantity10 : DBNull.Value;
        //        row["SellPrice10_NOO"] = item.SellPrice10_NOO;
        //        row["SellPrice10_MDEY"] = item.SellPrice10_MDEY;
        //        row["SellPrice10_MDEMMS"] = item.SellPrice10_MDEMMS;

        //        row["MatchPrice"] = item.MatchPrice != -9999999 ? (object)item.MatchPrice : DBNull.Value;
        //        row["MatchQuantity"] = item.MatchQuantity != -9999999 ? (object)item.MatchQuantity : DBNull.Value;
        //        row["OpenPrice"] = item.OpenPrice != -9999999 ? (object)item.OpenPrice : DBNull.Value;
        //        row["ClosePrice"] = item.ClosePrice != -9999999 ? (object)item.ClosePrice : DBNull.Value;
        //        row["HighestPrice"] = item.HighestPrice != -9999999 ? (object)item.HighestPrice : DBNull.Value;
        //        row["LowestPrice"] = item.LowestPrice != -9999999 ? (object)item.LowestPrice : DBNull.Value;
        //        row["CheckSum"] = item.CheckSum;
        //        dt.Rows.Add(row);
        //    }
        //    return dt;
        //}
        private void MapSecurityDefinitionColumns(OracleBulkCopy bulkCopy)
        {
            // Cú pháp: bulkCopy.ColumnMappings.Add("SourceColumn" (trong DataTable), "DestinationColumn" (trong Oracle DB));

            // Header
            bulkCopy.ColumnMappings.Add("BeginString", "BEGINSTRING");
            bulkCopy.ColumnMappings.Add("BodyLength", "BODYLENGTH");
            bulkCopy.ColumnMappings.Add("MsgType", "MSGTYPE");
            bulkCopy.ColumnMappings.Add("SenderCompID", "SENDERCOMPID");
            bulkCopy.ColumnMappings.Add("TargetCompID", "TARGETCOMPID");
            bulkCopy.ColumnMappings.Add("MsgSeqNum", "MSGSEQNUM");
            bulkCopy.ColumnMappings.Add("SendingTime", "SENDINGTIME");
            bulkCopy.ColumnMappings.Add("MarketID", "MARKETID");
            bulkCopy.ColumnMappings.Add("BoardID", "BOARDID");
            bulkCopy.ColumnMappings.Add("TotNumReports", "TOTNUMREPORTS");
            bulkCopy.ColumnMappings.Add("SecurityExchange", "SECURITYEXCHANGE");

            // Payload (Security Definition)
            bulkCopy.ColumnMappings.Add("Symbol", "SYMBOL");
            bulkCopy.ColumnMappings.Add("TickerCode", "TICKERCODE");
            bulkCopy.ColumnMappings.Add("SymbolShortCode", "SYMBOLSHORTCODE");
            bulkCopy.ColumnMappings.Add("SymbolName", "SYMBOLNAME");
            bulkCopy.ColumnMappings.Add("SymbolEnglishName", "SYMBOLENNAME");
            bulkCopy.ColumnMappings.Add("ProductID", "PRODUCTID");
            bulkCopy.ColumnMappings.Add("ProductGrpID", "PRODUCTGRPID");
            bulkCopy.ColumnMappings.Add("SecurityGroupID", "SECURITYGROUPID");
            bulkCopy.ColumnMappings.Add("PutOrCall", "PUTORCALL");
            bulkCopy.ColumnMappings.Add("ExerciseStyle", "EXERCISESTYLE");
            bulkCopy.ColumnMappings.Add("MaturityMonthYear", "MATURITYMONTHYEAR");
            bulkCopy.ColumnMappings.Add("MaturityDate", "MATURITYDATE");
            bulkCopy.ColumnMappings.Add("Issuer", "ISSUER");
            bulkCopy.ColumnMappings.Add("IssueDate", "ISSUEDATE");
            bulkCopy.ColumnMappings.Add("ContractMultiplier", "CONTRACTMULTIPLIER");
            bulkCopy.ColumnMappings.Add("CouponRate", "COUPONRATE");
            bulkCopy.ColumnMappings.Add("Currency", "CURRENCY");
            bulkCopy.ColumnMappings.Add("ListedShares", "LISTEDSHARES");
            bulkCopy.ColumnMappings.Add("HighLimitPrice", "HIGHLIMITPRICE");
            bulkCopy.ColumnMappings.Add("LowLimitPrice", "LOWLIMITPRICE");
            bulkCopy.ColumnMappings.Add("StrikePrice", "STRIKEPRICE");
            bulkCopy.ColumnMappings.Add("SecurityStatus", "SECURITYSTATUS");
            bulkCopy.ColumnMappings.Add("ContractSize", "CONTRACTSIZE");
            bulkCopy.ColumnMappings.Add("SettlMethod", "SETTLMETHOD");
            bulkCopy.ColumnMappings.Add("Yield", "YIELD");
            bulkCopy.ColumnMappings.Add("ReferencePrice", "REFERENCEPRICE");
            bulkCopy.ColumnMappings.Add("EvaluationPrice", "EVALUATIONPRICE");
            bulkCopy.ColumnMappings.Add("HgstOrderPrice", "HGSTORDERPRICE");
            bulkCopy.ColumnMappings.Add("LwstOrderPrice", "LWSTORDERPRICE");
            bulkCopy.ColumnMappings.Add("PrevClosePx", "PREVCLOSEPX");
            bulkCopy.ColumnMappings.Add("SymbolCloseInfoPxType", "SYMBOLCLOSEINFOPXTYPE");
            bulkCopy.ColumnMappings.Add("FirstTradingDate", "FIRSTTRADINGDATE");
            bulkCopy.ColumnMappings.Add("FinalTradeDate", "FINALTRADEDATE");
            bulkCopy.ColumnMappings.Add("FinalSettleDate", "FINALSETTLEDATE");
            bulkCopy.ColumnMappings.Add("ListingDate", "LISTINGDATE");
            bulkCopy.ColumnMappings.Add("RandomEndTriggeringConditionCode", "RETRIGGERINGCONDITIONCODE"); // Ánh xạ từ tên cột Postgres
            bulkCopy.ColumnMappings.Add("ExClassType", "EXCLASSTYPE");
            bulkCopy.ColumnMappings.Add("VWAP", "VWAP");
            bulkCopy.ColumnMappings.Add("SymbolAdminStatusCode", "SYMBOLADMINSTATUSCODE");
            bulkCopy.ColumnMappings.Add("SymbolTradingMethodStatusCode", "SYMBOLTRADINGMETHODSC"); 
            bulkCopy.ColumnMappings.Add("SymbolTradingSantionStatusCode", "SYMBOLTRADINGSANTIONSC"); // Ánh xạ từ tên cột Postgres
            bulkCopy.ColumnMappings.Add("SectorTypeCode", "SECTORTYPECODE");
            bulkCopy.ColumnMappings.Add("RedumptionDate", "REDUMPTIONDATE");

            // Footer
            bulkCopy.ColumnMappings.Add("CheckSum", "CHECKSUM");
            bulkCopy.ColumnMappings.Add("Date", "DATE");
        }
    }
}
