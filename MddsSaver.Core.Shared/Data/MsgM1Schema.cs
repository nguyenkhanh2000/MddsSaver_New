using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgM1Schema : BaseMessageSchema
    {
        // --- Payload (Index Specific) ---
        public const string TradingSessionId        = "tradingsessionid";
        public const string MarketIndexClass        = "marketindexclass";
        public const string IndexsTypeCode          = "indexstypecode";
        public const string Currency                = "currency";
        public const string TransactTime            = "transacttime";
        public const string TransDate               = "transdate";
        public const string ValueIndexes            = "valueindexes";
        public const string TotalVolumeTraded       = "totalvolumetraded";
        public const string GrossTradeAmt           = "grosstradeamt";
        public const string ContauctAccTrdvol       = "contauctacctrdvol";
        public const string ContauctAccTrdval       = "contauctacctrdval";
        public const string BlktrdAccTrdvol         = "blktrdacctrdvol";
        public const string BlktrdAccTrdval         = "blktrdacctrdval";

        // --- Fluctuation Counts (int / NUMBER(10)) ---
        public const string FluctuationUpperLimitIc = "fluctuationupperlimitic";
        public const string FluctuationUpIc         = "fluctuationupic";
        public const string FluctuationSteadinessIc = "fluctuationsteadinessic";
        public const string FluctuationDownIc       = "fluctuationdownic";
        public const string FluctuationLowerLimitIc = "fluctuationlowerlimitic";

        // --- Fluctuation Volumes (long / NUMBER(19)) ---
        public const string FluctuationUpIv         = "fluctuationupiv";
        public const string FluctuationDownIv       = "fluctuationdowniv";
        public const string FluctuationSteadinessIv = "fluctuationsteadinessiv";
    }
}
