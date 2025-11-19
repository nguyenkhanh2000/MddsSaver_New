using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Data
{
    public class MsgDSchema : BaseMessageSchema
    {
        // Payload (Header)
        public const string TotNumReports = "totnumreports";
        public const string SecurityExchange = "securityexchange";

        // Payload (Security Definition)
        public const string Symbol = "symbol";
        public const string TickerCode = "tickercode";
        public const string SymbolShortCode = "symbolshortcode";
        public const string SymbolName = "symbolname";
        public const string SymbolEnName = "symbolenname";
        public const string ProductId = "productid";
        public const string ProductGrpId = "productgrpid";
        public const string SecurityGroupId = "securitygroupid";
        public const string PutOrCall = "putorcall";
        public const string ExerciseStyle = "exercisestyle";
        public const string MaturityMonthYear = "maturitymonthyear";
        public const string MaturityDate = "maturitydate";
        public const string Issuer = "issuer";
        public const string IssueDate = "issuedate";
        public const string ContractMultiplier = "contractmultiplier";
        public const string CouponRate = "couponrate";
        public const string Currency = "currency";
        public const string ListedShares = "listedshares";
        public const string HighLimitPrice = "highlimitprice";
        public const string LowLimitPrice = "lowlimitprice";
        public const string StrikePrice = "strikeprice";
        public const string SecurityStatus = "securitystatus";
        public const string ContractSize = "contractsize";
        public const string SettlMethod = "settlmethod";
        public const string Yield = "yield";
        public const string ReferencePrice = "referenceprice";
        public const string EvaluationPrice = "evaluationprice";
        public const string HgstOrderPrice = "hgstorderprice";
        public const string LwstOrderPrice = "lwstorderprice";
        public const string PrevClosePx = "prevclosepx";
        public const string SymbolCloseInfoPxType = "symbolcloseinfopxtype";
        public const string FirstTradingDate = "firsttradingdate";
        public const string FinalTradeDate = "finaltradedate";
        public const string FinalSettleDate = "finalsettledate";
        public const string ListingDate = "listingdate";
        public const string ReTriggeringConditionCode = "retriggeringconditioncode";
        public const string ExClassType = "exclasstype";
        public const string VWap = "vwap";
        public const string SymbolAdminStatusCode = "symboladminstatuscode";
        public const string SymbolTradingMethodSc = "symboltradingmethodsc";
        public const string SymbolTradingSantionSc = "symboltradingsantionsc";
        public const string SectorTypeCode = "sectortypecode";
        public const string RedumptionDate = "redumptiondate";
    }
}
