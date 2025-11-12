using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Interfaces;
using MddsSaver.Core.Shared.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using System.Diagnostics;

namespace MddsSaver.Infrastructure.Shared.Services
{
    public class HsxRedisDataSaver : IHsxRedisDataSaver
    {
        private readonly ILogger<HsxRedisDataSaver> _logger;
        private readonly IRedisRepository _redisRepository;
        private readonly IRedisSentinelRepository _redisSentinelRepository;
        private Dictionary<string, string> d_dic_stockno = new Dictionary<string, string>();//dic lưu stock no của mess d
        public HsxRedisDataSaver(ILogger<HsxRedisDataSaver> logger, IRedisRepository redisRepo, IRedisSentinelRepository redisSentinelRep)
        {
            _logger = logger;
            _redisRepository = redisRepo;
            _redisSentinelRepository = redisSentinelRep;
        }
        public async Task SaveBatchAsync(List<object> messages, CancellationToken stoppingToken)
        {
            try
            {
                // 1. TẠO DANH SÁCH LỆNH
                var commands = new List<RedisCommand>();
                var commands_Sentinel = new List<RedisCommand>();

                foreach (var msg in messages)
                {
                    var typeMsg = msg.GetType();
                    if (typeMsg == typeof(ESecurityDefinition)) // msg_d
                    {
                        var data = (ESecurityDefinition)msg;
                        if(data.BoardID == "G1" && !d_dic_stockno.ContainsKey(data.Symbol))
                        {
                            // Lấy ra key msg lưu db
                            d_dic_stockno[data.Symbol] = data.TickerCode;
                            string stockno = JsonConvert.SerializeObject(d_dic_stockno);
                            var model = new SetStringCommand()
                            {
                                Key = EGlobalConfig.__TEMPLATE_REDIS_KEY_STOCK_NO_HSX,
                                Value = stockno,
                                Period = EGlobalConfig.intPeriod
                            };
                            commands.Add(model);                            
                        }
                    }
                    if (typeMsg == typeof(EPrice))  // msg_X
                    {
                        var eP = (EPrice)msg;
                        string Symbol = await GetSymbol(eP.Symbol);
                        if (string.IsNullOrEmpty(Symbol))
                            continue;  //bỏ qua msg này nếu Symbol không hợp lệ!
                        if (eP.MarketID == "STO" && eP.BoardID == "G1" && eP.Side == null) // Khớp lệnh, OHLC
                        {
                            commands_Sentinel.Add(Create_LE_TKTT_Command(eP, Symbol));
                            commands_Sentinel.Add(Create_LS_Command(eP, Symbol));
                        }
                        else if (eP.MarketID == "STO" && eP.BoardID == "G4" && eP.Side != null)
                        {
                            commands.Add(Create_PO_Command(eP, Symbol));
                        }
                        else if (eP.MarketID == "STO" && (eP.BoardID == "T1" || eP.BoardID == "T4" || eP.BoardID == "T2" || eP.BoardID == "T3" || eP.BoardID == "T6" || eP.BoardID == "R1") /*&& eP.Side != null*/)
                        {
                            CreatePT_KL(eP, Symbol, eP.BoardID, commands);
                            CreatePT_AllSide(eP, Symbol, eP.BoardID, commands);
                        }
                    }
                }

                if (commands.Any())
                    await _redisRepository.ExecuteBatchAsync(commands);
                if (commands_Sentinel.Any())
                    await _redisSentinelRepository.ExecuteBatchAsync(commands_Sentinel);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SaveBatchAsync: Lỗi khi thực hiện bulk insert!");
                // Ném lại exception để hàm gọi có thể xử lý (NACK messages)
                throw;
            }
        }
        private SortedSetAddCommand Create_LS_Command(EPrice eP, string Symbol)
        {
            try
            {
                string paddedSequence = eP.MsgSeqNum.ToString("D8");

                LS_Model ls_model = new LS_Model();
                string time = (eP.SendingTime.Split(' ')[1]).Split('.')[0];
                string strJsonC = "";
                //Xử lý CN -  Lấy Guid (random)
                Guid guid = Guid.NewGuid();
                string guidString = guid.ToString("N"); // Lấy chuỗi không dấu gạch ngang
                string first10Digits = guidString.Substring(0, 10);
                // Chuyển đổi 10 ký tự này thành số nguyên long
                long lsCN = long.Parse(first10Digits.Substring(0, 10), System.Globalization.NumberStyles.HexNumber);
                // Đảm bảo ls.CN có 10 chữ số bằng cách chia cho 10 nếu cần
                while (lsCN >= 10000000000)
                {
                    lsCN /= 10;
                }
                ls_model.SQ = paddedSequence;
                ls_model.CN = lsCN;
                ls_model.MT = time.ToString();
                ls_model.MP = (int)eP.MatchPrice;
                ls_model.MQ = eP.MatchQuantity;
                ls_model.TQ = eP.TotalVolumeTraded;
                ls_model.TV = (long)eP.GrossTradeAmt;
                ls_model.SIDE = eP.Side ?? string.Empty;

                strJsonC = JsonConvert.SerializeObject(ls_model);

                string Z_KEY = EGlobalConfig.TEMPLATE_REDIS_KEY_LS.Replace("(Symbol)", Symbol);
                long Z_SCORE = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                return new SortedSetAddCommand
                {
                    Key = Z_KEY,
                    Member = strJsonC,
                    Score = Z_SCORE
                };
            }
            catch(Exception ex)
            {
                throw ex;
            }
        }
        private UpdateSortedSetByScoreCommand Create_LE_TKTT_Command(EPrice eP, string Symbol)
        {
            try
            {
                LE_TKTT_Model le_tktt = new LE_TKTT_Model();
                string time = (eP.SendingTime.Split(' ')[1]).Split('.')[0];
                string strJsonC = "";

                le_tktt.MT = time.ToString();
                le_tktt.MP = (int)eP.MatchPrice;
                le_tktt.TQ = eP.TotalVolumeTraded;
                le_tktt.TV = eP.GrossTradeAmt / 1000000;

                strJsonC = JsonConvert.SerializeObject(le_tktt);

                string Z_KEY_VAL = EGlobalConfig.TEMPLATE_REDIS_KEY_LE_TKTT_VAL.Replace("(Symbol)", Symbol);
                string Z_KEY_VOL = EGlobalConfig.TEMPLATE_REDIS_KEY_LE_TKTT_VOL.Replace("(Symbol)", Symbol);

                DateTime dtNow = DateTime.Now;
                DateTime parsedTime = DateTime.ParseExact(time, "HH:mm:ss", null);

                // Gộp thời gian từ parsedTime với ngày hiện tại
                DateTime fullDateTime = new DateTime(dtNow.Year, dtNow.Month, dtNow.Day,
                                                      parsedTime.Hour, parsedTime.Minute, 00, DateTimeKind.Local);

                // Chuyển sang Unix timestamp (milliseconds)
                long Z_SCORE = new DateTimeOffset(fullDateTime).ToUnixTimeMilliseconds();
                return new UpdateSortedSetByScoreCommand
                {
                    KeyVal = Z_KEY_VAL,
                    KeyVol = Z_KEY_VOL,
                    Value = strJsonC,
                    Score = Z_SCORE
                };
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private SetStringCommand_PO Create_PO_Command(EPrice eP, string Symbol)
        {
            try
            {
                string sbJsonC = EGlobalConfig.TEMPLATE_JSONC_PO
                                        .Replace("(T)", eP.SendingTime.Replace(" ", "-").Substring(0, eP.SendingTime.IndexOf(".")))
                                        .Replace("(S)", Symbol)
                                        .Replace("(BP1)", ProcessPrice(eP.BuyPrice1).ToString())
                                        .Replace("(BQ1)", eP.BuyQuantity1.ToString())
                                        .Replace("(BP2)", ProcessPrice(eP.BuyPrice2).ToString())
                                        .Replace("(BQ2)", eP.BuyQuantity2.ToString())
                                        .Replace("(BP3)", ProcessPrice(eP.BuyPrice3).ToString())
                                        .Replace("(BQ3)", eP.BuyQuantity3.ToString())
                                        .Replace("(SP1)", ProcessPrice(eP.SellPrice1).ToString())
                                        .Replace("(SQ1)", eP.SellQuantity1.ToString())
                                        .Replace("(SP2)", ProcessPrice(eP.SellPrice2).ToString())
                                        .Replace("(SQ2)", eP.SellQuantity2.ToString())
                                        .Replace("(SP3)", ProcessPrice(eP.SellPrice3).ToString())
                                        .Replace("(SQ3)", eP.SellQuantity3.ToString());

                string Z_KEY = EGlobalConfig.TEMPLATE_REDIS_KEY_PO
                        .Replace("(Symbol)", Symbol);
                return new SetStringCommand_PO
                {
                    Key = Z_KEY,
                    Value = sbJsonC,
                    Period = EGlobalConfig.intPeriod
                };
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private void CreatePT_KL(EPrice eP,string Symbol, string BoardID, List<RedisCommand> commandList)
        {
            try
            {
                string paddedSequence = eP.MsgSeqNum.ToString("D8");

                Symbol = d_dic_stockno[eP.Symbol];
                string time = (eP.SendingTime.Split(' ')[1]).Split('.')[0];

                PT_Model pt_model = new PT_Model
                {
                    SQ = paddedSequence,
                    MT = time,
                    MP = ProcessPrice(eP.MatchPrice),
                    MQ = eP.MatchQuantity,
                    TQ = eP.TotalVolumeTraded,
                    TV = eP.GrossTradeAmt
                };
                PT_ForAll pt_all = new PT_ForAll
                {
                    Symbol = Symbol,
                    Data = pt_model
                };
                string strJson_Symbol = JsonConvert.SerializeObject(pt_model);
                string strJson_All = JsonConvert.SerializeObject(pt_all);

                string Z_KEY_SYMBOL = EGlobalConfig.TEMPLATE_REDIS_KEY_PT.Replace("(Symbol)", Symbol).Replace("(Board)", BoardID);
                string Z_KEY_ALL = EGlobalConfig.TEMPLATE_REDIS_KEY_PT_ALL.Replace("(Board)", BoardID);

                long Z_SCORE = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                commandList.Add(new SortedSetAddCommand
                {
                    Key = Z_KEY_SYMBOL,
                    Member = strJson_Symbol,
                    Score = Z_SCORE
                });

                commandList.Add(new SortedSetAddCommand
                {
                    Key = Z_KEY_ALL,
                    Member = strJson_All,
                    Score = Z_SCORE
                });
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private void CreatePT_AllSide(EPrice eP, string Symbol, string BoardID, List<RedisCommand> commandList)
        {
            try
            {
                PT_Side_Model sideB = new PT_Side_Model
                {
                    Symbol = Symbol,
                    Data = new Side_Data
                    {
                        MP = eP.BuyPrice1,
                        MQ = eP.BuyQuantity1
                    }
                };
                PT_Side_Model sideS = new PT_Side_Model
                {
                    Symbol = Symbol,
                    Data = new Side_Data
                    {
                        MP = eP.SellPrice1,
                        MQ = eP.SellQuantity1
                    }
                };
                string Z_KEY_BUY = EGlobalConfig.TEMPLATE_REDIS_KEY_PT_SIDE_B.Replace("(Board)", BoardID);
                string Z_KEY_SELL = EGlobalConfig.TEMPLATE_REDIS_KEY_PT_SIDE_S.Replace("(Board)", BoardID);
                //long Z_SCORE = Convert.ToInt64(DateTime.Now.ToString("yyyyMMddHHmmssfff"));
                long Z_SCORE = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                if (sideB.Data.MP > 0 && sideB.Data.MQ > 0)
                {
                    string strJsonC_Buy = JsonConvert.SerializeObject(sideB);
                    commandList.Add(new SortedSetAddCommand
                    {
                        Key = Z_KEY_BUY,
                        Member = strJsonC_Buy,
                        Score = Z_SCORE
                    });                    
                }
                if (sideS.Data.MP > 0 && sideS.Data.MQ > 0)
                {
                    string strJsonC_Sell = JsonConvert.SerializeObject(sideS);
                    commandList.Add(new SortedSetAddCommand
                    {
                        Key = Z_KEY_SELL,
                        Member = strJsonC_Sell,
                        Score = Z_SCORE
                    });
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        private async Task<string> GetSymbol(string symbol)
        {
            try
            {
                if (d_dic_stockno.Count < 1)
                {
                    //string value = await _redisRepository.GetAsync<string>(EGlobalConfig.__TEMPLATE_REDIS_KEY_STOCK_NO_HSX);
                    string value = _redisRepository.GetString(EGlobalConfig.__TEMPLATE_REDIS_KEY_STOCK_NO_HSX);
                    if (!string.IsNullOrEmpty(value))
                    {
                        var obj = JsonConvert.DeserializeObject<JObject>(value);
                        var data = obj["Data"];
                        if (data != null)
                        {
                            Dictionary<string, string> storedDictionary = data.ToObject<Dictionary<string, string>>();
                            foreach (var kew in storedDictionary)
                            {
                                d_dic_stockno[kew.Key] = kew.Value;
                            }
                        }
                    }
                }
                d_dic_stockno.TryGetValue(symbol, out var mappedSymbol);
                return mappedSymbol;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "GetSymbol: Lỗi khi thực hiện GetSymbol cho symbol: {Symbol}", symbol);
                return null;
            }
        }
        private double ProcessPrice(double priceString, int priceDividedBy = 1000, int priceRoundDigitsCount = 2)
        {
            double price = priceString; // 43100
            price = price / priceDividedBy; // 43.1
            price = Math.Round(price, priceRoundDigitsCount); // 43.1
            return price;
        }
    }
}
