using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Entities
{
    public class EPrice : EBasePrice
    {
        /// <summary>
        /// X = Price
        /// </summary>
        new public const string __MSG_TYPE = __MSG_TYPE_PRICE; // override __MSG_TYPE cua EBasePrice

        /// <summary>
        /// 2020-04-27 15:11:08 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=75 ; required=Y; format=LocalMkt Date; length=8</i></para>
        /// <para><b>Ngày giao dịch. Định dạng (YYYYMMDD)</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_75, Order = 12)]
        public string TradeDate { get; set; }

        /// <summary>
        /// 2020-04-10 11:58:50 hungtq
        /// <para><i>SPEC version=1.3; date=2019.08.23</i></para>
        /// <para><i>tag=60 ; required=Y; format=UTCTime; length=9</i></para>
        /// <para><b>Thời gian thực thi HHmmSSsss</b></para>
        /// </summary>
        [JsonProperty(PropertyName = __TAG_60, Order = 13)]
        public string TransactTime { get; set; }



        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="basePriceInstance"></param>
        public EPrice(string rawData, EBasePrice basePriceInstance) : base(rawData, basePriceInstance)
        {
        }
    }
}
