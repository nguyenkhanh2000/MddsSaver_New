using MddsSaver.Application.Shared.Interfaces;
using MddsSaver.Core.Shared.Entities;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace MddsSaver.Application.Shared.Common
{
    // class này chứa logic để nhận msg raw và return về một IMessageParser 
    public class MessageParserFactory : IMessageParserFactory
    {
        public TimeZoneInfo timeZone = TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time");
        private readonly ILogger<MessageParserFactory> _logger;
        public MessageParserFactory(ILogger<MessageParserFactory> logger)
        {
            _logger = logger;
        }
        public async Task<object> Parse(string rawMessage, string msgType)
        {
            try
            {
                if (string.IsNullOrEmpty(msgType)) return null;
                switch (msgType)
                {
                    case EPrice.__MSG_TYPE:
                        EPrice eP = this.Fix_Fix2EPrice(rawMessage, true, 1, 2, 1);
                        return eP;
                    case EPriceRecovery.__MSG_TYPE:
                        EPriceRecovery ePR = this.Fix_Fix2EPriceRecovery(rawMessage, true, 1, 2, 1);
                        return ePR;
                    case EIndex.__MSG_TYPE:
                        EIndex eI = await this.Raw2Entity<EIndex>(rawMessage);
                        eI.TransDate = this.FixToTransDateString(eI.SendingTime.ToString());
                        eI.TransactTime = this.FixToTimeString(eI.TransactTime.ToString());
                        return eI;
                    case EDeemTradePrice.__MSG_TYPE:
                        EDeemTradePrice eDTP = await this.Raw2Entity<EDeemTradePrice>(rawMessage);
                        return eDTP;
                    case ETradingResultOfForeignInvestors.__MSG_TYPE:
                        ETradingResultOfForeignInvestors ETRFI = await this.Raw2Entity<ETradingResultOfForeignInvestors>(rawMessage);
                        if (ETRFI.TransactTime != null)
                        {
                            ETRFI.TransactTime = this.FixToTimeString(ETRFI.TransactTime.ToString());// can phai chuyen SendingTime tu string sang DateTime	
                        }
                        return ETRFI;
                    case ESecurityDefinition.__MSG_TYPE:
                        ESecurityDefinition eSD = await this.Raw2Entity<ESecurityDefinition>(rawMessage);
                        return eSD;
                    case ESecurityStatus.__MSG_TYPE:
                        ESecurityStatus eSS = await this.Raw2Entity<ESecurityStatus>(rawMessage);
                        return eSS;
                    case ESecurityInformationNotification.__MSG_TYPE:
                        ESecurityInformationNotification eSIN = await this.Raw2Entity<ESecurityInformationNotification>(rawMessage);
                        return eSIN;
                    case ESymbolClosingInformation.__MSG_TYPE:
                        ESymbolClosingInformation eSCI = await this.Raw2Entity<ESymbolClosingInformation>(rawMessage);
                        return eSCI;
                    case EVolatilityInterruption.__MSG_TYPE:
                        EVolatilityInterruption eVI = await this.Raw2Entity<EVolatilityInterruption>(rawMessage);
                        return eVI;
                    case EMarketMakerInformation.__MSG_TYPE:
                        EMarketMakerInformation eMMI = await this.Raw2Entity<EMarketMakerInformation>(rawMessage);
                        return eMMI;
                    case ESymbolEvent.__MSG_TYPE:
                        ESymbolEvent eSE = await this.Raw2Entity<ESymbolEvent>(rawMessage);
                        eSE.EventStartDate = this.FixToDateString(eSE.EventStartDate.ToString());
                        eSE.EventEndDate = this.FixToDateString(eSE.EventEndDate.ToString());
                        return eSE;
                    case EIndexConstituentsInformation.__MSG_TYPE:
                        EIndexConstituentsInformation eICI = await this.Raw2Entity<EIndexConstituentsInformation>(rawMessage);
                        return eICI;
                    case ERandomEnd.__MSG_TYPE:
                        ERandomEnd eRE = await this.Raw2Entity<ERandomEnd>(rawMessage);
                        eRE.TransactTime = this.FixToTimeString(eRE.TransactTime.ToString());
                        return eRE;
                    case EInvestorPerIndustry.__MSG_TYPE:
                        EInvestorPerIndustry eIPI = await this.Raw2Entity<EInvestorPerIndustry>(rawMessage);
                        eIPI.TransactTime = this.FixToTimeString(eIPI.TransactTime.ToString());
                        return eIPI;
                    case EInvestorPerSymbol.__MSG_TYPE:
                        EInvestorPerSymbol eIPS = await this.Raw2Entity<EInvestorPerSymbol>(rawMessage);
                        return eIPS;
                    case ETopNMembersPerSymbol.__MSG_TYPE:
                        ETopNMembersPerSymbol eTNMPS = await this.Raw2Entity<ETopNMembersPerSymbol>(rawMessage);
                        return eTNMPS;
                    case EOpenInterest.__MSG_TYPE:
                        EOpenInterest eOI = await this.Raw2Entity<EOpenInterest>(rawMessage);
                        eOI.TradeDate = this.FixToDateString(eOI.TradeDate.ToString());
                        return eOI;
                    case EForeignerOrderLimit.__MSG_TYPE:
                        EForeignerOrderLimit eFOL = await this.Raw2Entity<EForeignerOrderLimit>(rawMessage);
                        return eFOL;
                    case EPriceLimitExpansion.__MSG_TYPE:
                        EPriceLimitExpansion ePLE = await this.Raw2Entity<EPriceLimitExpansion>(rawMessage);
                        return ePLE;
                    case EETFiNav.__MSG_TYPE:
                        EETFiNav eEiN = await this.Raw2Entity<EETFiNav>(rawMessage);
                        return eEiN;
                    case EETFiIndex.__MSG_TYPE:
                        EETFiIndex eEiI = await this.Raw2Entity<EETFiIndex>(rawMessage);
                        return eEiI;
                    case EETFTrackingError.__MSG_TYPE:
                        EETFTrackingError eETE = await this.Raw2Entity<EETFTrackingError>(rawMessage);
                        eETE.TradeDate = this.FixToDateString(eETE.TradeDate.ToString());
                        return eETE;
                    case ETopNSymbolsWithTradingQuantity.__MSG_TYPE:
                        ETopNSymbolsWithTradingQuantity ETNSWTQ = await this.Raw2Entity<ETopNSymbolsWithTradingQuantity>(rawMessage);
                        return ETNSWTQ;
                    case ETopNSymbolsWithCurrentPrice.__MSG_TYPE:
                        ETopNSymbolsWithCurrentPrice ETNSWCP = await this.Raw2Entity<ETopNSymbolsWithCurrentPrice>(rawMessage);
                        return ETNSWCP;
                    case ETopNSymbolsWithHighRatioOfPrice.__MSG_TYPE:
                        ETopNSymbolsWithHighRatioOfPrice ETNSWHROP = await this.Raw2Entity<ETopNSymbolsWithHighRatioOfPrice>(rawMessage);
                        return ETNSWHROP;
                    case ETopNSymbolsWithLowRatioOfPrice.__MSG_TYPE:
                        ETopNSymbolsWithLowRatioOfPrice ETNSWLROP = await this.Raw2Entity<ETopNSymbolsWithLowRatioOfPrice>(rawMessage);
                        return ETNSWLROP;
                    case EDisclosure.__MSG_TYPE:
                        EDisclosure eD = await this.Raw2Entity<EDisclosure>(rawMessage);
                        eD.PublicInformationDate = this.FixToDateString(eD.PublicInformationDate.ToString());
                        eD.TransmissionDate = this.FixToDateString(eD.TransmissionDate.ToString());// can phai chuyen SendingTime tu string sang DateTime	
                        return eD;
                    case EDrvProductEvent.__MSG_TYPE:
                        EDrvProductEvent eDRV = await this.Raw2Entity<EDrvProductEvent>(rawMessage);
                        return eDRV;
                    case "MV":
                        return new MvMessage
                        {
                            RawMessage = rawMessage
                        };
                    default:
                        return null;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Parse error, msgRaw: " + rawMessage);
                return null;
            }
        }
        public string GetMsgType(string rawData)
        {
            string msgType = Regex.Match(rawData, "35=(.*?)", RegexOptions.Multiline).Groups[1].Value;
            return msgType;
        }
        public Task<T> Raw2Entity<T>(string rawData) where T : EBase
        {
            try
            {
                string json = this.Fix2Json(rawData);
                T entity = JsonConvert.DeserializeObject<T>(json);
                entity.SendingTime = this.FixToDateTimeString(TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(entity.SendingTime, "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff")); // can phai chuyen SendingTime tu string sang DateTime
                return Task.FromResult(entity);
            }
            catch (Exception ex)
            {
                return null;
            }
        }
        public string Fix2Json(string fixString)
        {
            StringBuilder sb = new StringBuilder(fixString);
            sb.Length--;
            sb.Replace("", "\",\"");
            sb.Replace("=", "\":\"");
            sb.Append("\"}");
            sb.Insert(0, "{\"");
            return sb.ToString();
        }

        public EPrice Fix_Fix2EPrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            EBasePrice eBP = this.Fix_Fix2EBasePrice(rawData, readAllTags, priceDividedBy, priceRoundDigitsCount, massDividedBy);
            EPrice eP = new EPrice(rawData, eBP);

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(EGlobalConfig.__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(EGlobalConfig.__STRING_EQUAL);
                        switch (parts[0])
                        {

                            case EBase.__TAG_75: eP.TradeDate = parts[1]; break;
                            case EBase.__TAG_60: eP.TransactTime = parts[1]; break;

                        }
                    }
                }
            }

            return eP;
        }
        /// <summary>
        /// chuyen raw data thanh EBasePrice		
        /// </summary>
        /// <param name="rawData">8=FIX.4.49=69235=X49=VNMGW56=9999934=1335552=20190517 10:35:53.79630001=STO20004=G4336=4055=VN718148000675=2019022160=10355378830521=030522=030523=030524=0268=1083=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030271=083=3279=0269=1290=2270=0.0271=0346=030271=083=4279=0269=0290=2270=114500.0271=152346=030271=083=5279=0269=1290=3270=0.0271=0346=030271=083=6279=0269=0290=3270=114000.0271=54346=030271=083=7279=0269=1290=4270=0.0271=0346=030271=083=8279=0269=0290=4270=0.0271=0346=030271=083=9279=0269=1290=5270=0.0271=0346=030271=083=10279=0269=0290=5270=0.0271=0346=030271=010=186</param>
        /// <param name="readAllTags">
        /// false - ko can lay tat ca data, can xu ly data nhanh trong feeder de pub ra hub
        /// true  - lay tat ca data de insert db
        /// </param>
        /// <returns>1</returns>		
        public EBasePrice Fix_Fix2EBasePrice(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            int rptSeq = 0;		 // 83		Data sequential number within repeated record
            string mdUpdateAction = null;    // 279		Type code of update action for an entry of market data: 0 = New
            string mdEntryType = null;    // 269		Data classification for an entry of market data: 0 = Bid; 1 = Offer; 2 = Trade; 4 = Opening Price; 5 = Closing Price; 7 = Trading Session High Price; 8 = Trading Session Low Price
            int mdEntryPositionNo = 0;       // 290		Position no (or level) for an entry of market data
            double mdEntryPx = 0;       // 270		Price for an entry of market data
            int mdEntrySize = 0;       // 271		Size (or quantity) for an entry of market data
            int numberOfOrders = 0;       // 346		Number of orders
            double mdEntryYield = 0;       // 30270	Yield of the entry. Bond market only.
            int mdEntryMMSize = 0;       // 30271	Size of the entry provided from market makers
            string sep1 = EGlobalConfig.__STRING_FIX_SEPARATOR + EBasePrice.__TAG_83 + EGlobalConfig.__STRING_EQUAL;// "83="
            string sep2 = EBasePrice.__TAG_83 + EGlobalConfig.__STRING_EQUAL;         // "83="
            EBasePrice eBP = new EBasePrice(rawData, null);

            if (rawData.Contains(sep1))
            {
                string data = rawData.Substring(rawData.IndexOf(sep1));    // 83=1279=0269=1290=1270=0.0271=0346=030271=083=2279=0269=0290=1270=118500.0271=8346=030

                string[] bigArray = data.Split(sep1);    // {"1279=0269=1290=1270=0.0271=0346=030271=0", "2279=0269=0290=1270=118500.0271=8346=030271=0", ...}
                StringBuilder sb = new StringBuilder("");      //   {"data":[{"83":"1","270":"aaa"},{"83":"2","270":"bbbbb"}]}
                for (int i = 0; i < bigArray.Length; i++)
                {
                    if (!string.IsNullOrEmpty(bigArray[i]))
                    {
                        // lay value tu raw string
                        string pair = sep2 + bigArray[i]; // 83=1279=0269=1290=1270=0.0271=0346=030271=0
                        string[] smallArray = pair.Split(EGlobalConfig.__STRING_FIX_SEPARATOR); // {"83=1","279=0","269=1",...}
                        for (int j = 0; j < smallArray.Length; j++)
                        {
                            string[] arr = smallArray[j].Split(EGlobalConfig.__STRING_EQUAL); // {"83","1"} 
                            switch (arr[0])
                            {

                                case EBase.__TAG_83   : rptSeq                = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_279  : mdUpdateAction        = arr[1]; break;
                                case EBase.__TAG_269  : mdEntryType           = arr[1]; break;
                                case EBase.__TAG_290  : mdEntryPositionNo     = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_270  : mdEntryPx             = this.ProcessPrice(arr[1], priceDividedBy, priceRoundDigitsCount); break;
                                case EBase.__TAG_271  : mdEntrySize           = this.Processkl(arr[1], massDividedBy); break; //Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_346  : numberOfOrders        = Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_30270: mdEntryYield          = Convert.ToDouble(arr[1]); break;
                                case EBase.__TAG_30271: mdEntryMMSize         = this.Processkl(arr[1], massDividedBy); break;//Convert.ToInt32(arr[1]); break;
                                case EBase.__TAG_387  : eBP.TotalVolumeTraded = this.Processkl(arr[1], massDividedBy); break;
                                case EBase.__TAG_381  : eBP.GrossTradeAmt     = Convert.ToDouble(arr[1]); break;
                            }
                        }
                        // gan value vao entity
                        int entryType = Convert.ToInt32(mdEntryType);
                        switch (entryType)
                        {
                            case (int)EBasePrice.EntryTypes.Bid       : SetValues(ref eBP, EBasePrice.EntryTypes.Bid, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "B"; break;
                            case (int)EBasePrice.EntryTypes.Offer     : SetValues(ref eBP, EBasePrice.EntryTypes.Offer, rptSeq, mdUpdateAction, mdEntryPositionNo, mdEntryPx, mdEntrySize, numberOfOrders, mdEntryYield, mdEntryMMSize); eBP.Side = "S"; break;
                            case (int)EBasePrice.EntryTypes.Trade     : eBP.MatchPrice = mdEntryPx; eBP.MatchQuantity = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice.EntryTypes.OpenPrice : eBP.OpenPrice = mdEntryPx; eBP.OpenPriceQty = Convert.ToInt64(mdEntrySize); break;
                            case (int)EBasePrice.EntryTypes.ClosePrice: eBP.ClosePrice = mdEntryPx; break;
                            case (int)EBasePrice.EntryTypes.HighPrice : eBP.HighestPrice = mdEntryPx; break;
                            case (int)EBasePrice.EntryTypes.LowPrice  : eBP.LowestPrice = mdEntryPx; break;
                        }
                    }
                }
            }

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(EGlobalConfig.__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(EGlobalConfig.__STRING_EQUAL);
                        switch (parts[0])
                        {
                            case EBase.__TAG_8: eBP.BeginString = parts[1]; break;
                            case EBase.__TAG_9: eBP.BodyLength = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_35: eBP.MsgType = parts[1]; break;
                            case EBase.__TAG_49: eBP.SenderCompID = parts[1]; break;
                            case EBase.__TAG_56: eBP.TargetCompID = parts[1]; break;
                            case EBase.__TAG_34: eBP.MsgSeqNum = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_52: eBP.SendingTime = TimeZoneInfo.ConvertTimeFromUtc(DateTime.ParseExact(parts[1], "yyyyMMdd HH:mm:ss.fff", null), timeZone).ToString("yyyyMMdd HH:mm:ss.fff"); break;
                            //====================================================================================
                            case EBase.__TAG_30001: eBP.MarketID = parts[1]; break;
                            case EBase.__TAG_20004: eBP.BoardID = parts[1]; break;
                            case EBase.__TAG_336: eBP.TradingSessionID = parts[1]; break;
                            case EBase.__TAG_55: eBP.Symbol = parts[1]; break;
                            //case EBase.__TAG_75   : eBP.TradeDate         = parts[1];                   break;
                            //case EBase.__TAG_60   : eBP.TransactTime      = parts[1];                   break;
                            case EBase.__TAG_387: eBP.TotalVolumeTraded = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_381: eBP.GrossTradeAmt = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30521: eBP.SellTotOrderQty = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30522: eBP.BuyTotOrderQty = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30523: eBP.SellValidOrderCnt = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_30524: eBP.BuyValidOrderCnt = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_268: eBP.NoMDEntries = Convert.ToInt64(parts[1]); break;
                            case EBase.__TAG_346: eBP.NumberOfOrders = Convert.ToInt64(parts[1]); break;
                            // Repeating Group ............... (xem code o tren)
                            //====================================================================================
                            case EBase.__TAG_10: eBP.CheckSum = parts[1]; break;
                        }
                    }
                }
            }
            return eBP;
        }
        public EPriceRecovery Fix_Fix2EPriceRecovery(string rawData, bool readAllTags = false, int priceDividedBy = 1, int priceRoundDigitsCount = 2, int massDividedBy = 1)
        {
            EBasePrice eBP = this.Fix_Fix2EBasePrice(rawData, readAllTags, priceDividedBy, priceRoundDigitsCount, massDividedBy);
            EPriceRecovery ePR = new EPriceRecovery(rawData, eBP);

            // lay tat ca data, chi su dung khi can insert db
            if (readAllTags)
            {
                string[] arr = rawData.Split(EGlobalConfig.__STRING_FIX_SEPARATOR);
                foreach (string pair in arr)
                {
                    if (!string.IsNullOrEmpty(pair))
                    {
                        string[] parts = pair.Split(EGlobalConfig.__STRING_EQUAL);
                        switch (parts[0])
                        {
                            case EBase.__TAG_30561: ePR.OpnPx = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30562: ePR.TrdSessnHighPx = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30563: ePR.TrdSessnLowPx = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_20026: ePR.SymbolCloseInfoPx = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30565: ePR.OpnPxYld = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30566: ePR.TrdSessnHighPxYld = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30567: ePR.TrdSessnLowPxYld = Convert.ToDouble(parts[1]); break;
                            case EBase.__TAG_30568: ePR.ClsPxYld = Convert.ToDouble(parts[1]); break;

                        }
                    }
                }
            }

            return ePR;
        }
        public void SetValues(ref EBasePrice ePrice, EBasePrice.EntryTypes entryType, int rptSeq, string mdUpdateAction, int mdEntryPositionNo, double mdEntryPx, int mdEntrySize, int numberOfOrders, double mdEntryYield, int mdEntryMMSize)
        {
            switch (entryType)
            {
                case EBasePrice.EntryTypes.Bid:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.BuyPrice1 = mdEntryPx; ePrice.BuyQuantity1 = mdEntrySize; ePrice.BuyPrice1_NOO = numberOfOrders; ePrice.BuyPrice1_MDEY = mdEntryYield; ePrice.BuyPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.BuyPrice2 = mdEntryPx; ePrice.BuyQuantity2 = mdEntrySize; ePrice.BuyPrice2_NOO = numberOfOrders; ePrice.BuyPrice2_MDEY = mdEntryYield; ePrice.BuyPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.BuyPrice3 = mdEntryPx; ePrice.BuyQuantity3 = mdEntrySize; ePrice.BuyPrice3_NOO = numberOfOrders; ePrice.BuyPrice3_MDEY = mdEntryYield; ePrice.BuyPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.BuyPrice4 = mdEntryPx; ePrice.BuyQuantity4 = mdEntrySize; ePrice.BuyPrice4_NOO = numberOfOrders; ePrice.BuyPrice4_MDEY = mdEntryYield; ePrice.BuyPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.BuyPrice5 = mdEntryPx; ePrice.BuyQuantity5 = mdEntrySize; ePrice.BuyPrice5_NOO = numberOfOrders; ePrice.BuyPrice5_MDEY = mdEntryYield; ePrice.BuyPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.BuyPrice6 = mdEntryPx; ePrice.BuyQuantity6 = mdEntrySize; ePrice.BuyPrice6_NOO = numberOfOrders; ePrice.BuyPrice6_MDEY = mdEntryYield; ePrice.BuyPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.BuyPrice7 = mdEntryPx; ePrice.BuyQuantity7 = mdEntrySize; ePrice.BuyPrice7_NOO = numberOfOrders; ePrice.BuyPrice7_MDEY = mdEntryYield; ePrice.BuyPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.BuyPrice8 = mdEntryPx; ePrice.BuyQuantity8 = mdEntrySize; ePrice.BuyPrice8_NOO = numberOfOrders; ePrice.BuyPrice8_MDEY = mdEntryYield; ePrice.BuyPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.BuyPrice9 = mdEntryPx; ePrice.BuyQuantity9 = mdEntrySize; ePrice.BuyPrice9_NOO = numberOfOrders; ePrice.BuyPrice9_MDEY = mdEntryYield; ePrice.BuyPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.BuyPrice10 = mdEntryPx; ePrice.BuyQuantity10 = mdEntrySize; ePrice.BuyPrice10_NOO = numberOfOrders; ePrice.BuyPrice10_MDEY = mdEntryYield; ePrice.BuyPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
                case EBasePrice.EntryTypes.Offer:
                    switch (mdEntryPositionNo)
                    {
                        case 1: ePrice.SellPrice1 = mdEntryPx; ePrice.SellQuantity1 = mdEntrySize; ePrice.SellPrice1_NOO = numberOfOrders; ePrice.SellPrice1_MDEY = mdEntryYield; ePrice.SellPrice1_MDEMMS = mdEntryMMSize; break;
                        case 2: ePrice.SellPrice2 = mdEntryPx; ePrice.SellQuantity2 = mdEntrySize; ePrice.SellPrice2_NOO = numberOfOrders; ePrice.SellPrice2_MDEY = mdEntryYield; ePrice.SellPrice2_MDEMMS = mdEntryMMSize; break;
                        case 3: ePrice.SellPrice3 = mdEntryPx; ePrice.SellQuantity3 = mdEntrySize; ePrice.SellPrice3_NOO = numberOfOrders; ePrice.SellPrice3_MDEY = mdEntryYield; ePrice.SellPrice3_MDEMMS = mdEntryMMSize; break;
                        case 4: ePrice.SellPrice4 = mdEntryPx; ePrice.SellQuantity4 = mdEntrySize; ePrice.SellPrice4_NOO = numberOfOrders; ePrice.SellPrice4_MDEY = mdEntryYield; ePrice.SellPrice4_MDEMMS = mdEntryMMSize; break;
                        case 5: ePrice.SellPrice5 = mdEntryPx; ePrice.SellQuantity5 = mdEntrySize; ePrice.SellPrice5_NOO = numberOfOrders; ePrice.SellPrice5_MDEY = mdEntryYield; ePrice.SellPrice5_MDEMMS = mdEntryMMSize; break;
                        case 6: ePrice.SellPrice6 = mdEntryPx; ePrice.SellQuantity6 = mdEntrySize; ePrice.SellPrice6_NOO = numberOfOrders; ePrice.SellPrice6_MDEY = mdEntryYield; ePrice.SellPrice6_MDEMMS = mdEntryMMSize; break;
                        case 7: ePrice.SellPrice7 = mdEntryPx; ePrice.SellQuantity7 = mdEntrySize; ePrice.SellPrice7_NOO = numberOfOrders; ePrice.SellPrice7_MDEY = mdEntryYield; ePrice.SellPrice7_MDEMMS = mdEntryMMSize; break;
                        case 8: ePrice.SellPrice8 = mdEntryPx; ePrice.SellQuantity8 = mdEntrySize; ePrice.SellPrice8_NOO = numberOfOrders; ePrice.SellPrice8_MDEY = mdEntryYield; ePrice.SellPrice8_MDEMMS = mdEntryMMSize; break;
                        case 9: ePrice.SellPrice9 = mdEntryPx; ePrice.SellQuantity9 = mdEntrySize; ePrice.SellPrice9_NOO = numberOfOrders; ePrice.SellPrice9_MDEY = mdEntryYield; ePrice.SellPrice9_MDEMMS = mdEntryMMSize; break;
                        case 10: ePrice.SellPrice10 = mdEntryPx; ePrice.SellQuantity10 = mdEntrySize; ePrice.SellPrice10_NOO = numberOfOrders; ePrice.SellPrice10_MDEY = mdEntryYield; ePrice.SellPrice10_MDEMMS = mdEntryMMSize; break;
                    }//switch (mdEntryPositionNo)
                    break;
            } // switch (entryType)

        }
        /// <summary>
        /// chia gia cho 1000 va lam tron so
        /// ProcessPrice(43140,1000,2) = 43.14
        /// ProcessPrice(43140,1000,1) = 43.1
        /// </summary>
        /// <param name="priceString">:43100"</param>
        /// <param name="priceDividedBy">1000</param>
        /// <param name="priceRoundDigitsCount">2</param>
        /// <returns></returns>
        public double ProcessPrice(string priceString, int priceDividedBy, int priceRoundDigitsCount)
        {

            double price = Convert.ToDouble(priceString); // 43100
            price = price / priceDividedBy; // 43.1
            price = Math.Round(price, priceRoundDigitsCount); // 43.1
            return price;
        }
        public int Processkl(string priceString, int priceDividedBy)
        {
            int kl = Convert.ToInt32(priceString); // 43100
            kl = kl / priceDividedBy; // 43.1
            return kl;
        }
        public string FixToDateTimeString(string fixString)
        {
            DateTime localDateTime;
            string datetimeString = fixString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_ORACLE);
            return processed;
        }
        /// <summary>
        /// </summary>
        /// <param name="fixString">20190217</param>
        /// <returns>19-feb-2019</returns>

        public string FixToDateString(object fixString)
        {
            DateTime localDateTime;
            string rawString = fixString.ToString();
            string datetimeString = rawString.Insert(4, "-").Insert(7, "-");
            localDateTime = DateTime.Parse(datetimeString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_SQL_DATE_ONLY); // "dd-MMM-yyyy"
            return processed;
        }
        /// <summary>
        /// </summary>
        /// <param name="fixString">085500028</param>
        /// <returns>08:55:00.028</returns>
        public string FixToTimeString(string fixString)
        {
            string inputTime = fixString.Insert(2, ":").Insert(5, ":").Insert(8, ".");
            DateTime utc0Time = DateTime.ParseExact(inputTime, "HH:mm:ss.fff", null);

            // Chuyển đổi sang múi giờ UTC+7
            DateTime utc7Time = utc0Time.AddHours(7);
            string datetimeString = utc7Time.ToString("HH:mm:ss.fff");
            return datetimeString;
        }
        /// <summary>
        /// </summary>
        /// <param name="fixString">20190217</param>
        /// <returns>19-feb-2019</returns>

        public string FixToTransDateString(string fixString)
        {
            DateTime localDateTime;
            localDateTime = DateTime.Parse(fixString);
            string processed = localDateTime.ToString(EGlobalConfig.DATETIME_SQL_DATE_ONLY);
            return processed;
        }
    }
}
