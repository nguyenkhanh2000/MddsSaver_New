using MddsSaver.Core.Shared.Entities;
using MddsSaver.Core.Shared.Enums;
using MddsSaver.Core.Shared.Interfaces;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Infrastructure.Shared.Services
{
    public class MonitorMdds : IMonitor
    {
        private readonly ILogger<MonitorMdds> _logger;
        private readonly IRedisRepository _redisRepo;
        public MonitorMdds(ILogger<MonitorMdds> logger, IRedisRepository redisRepo) 
        {
            _logger = logger;
            _redisRepo = redisRepo;
        }
        public async Task SendStatusToMonitor(string ActiveTime, string strIP, string AppName, int intRowCount, long lngDuration)
        {
            try
            {
                string strJsonB = EGlobalConfig.TEMPLATE_JSON_TYPE_STATUS;
                string strAppName = AppName;
                string strRowID = strAppName;
                string strKey = strRowID;
                string strOut = "";

                //tao json
                strJsonB = strJsonB.Replace("(DateTimeMonitor)", DateTime.Now.ToString(EGlobalConfig.FORMAT_DATETIME));
                strJsonB = strJsonB.Replace("(RowID)", strRowID);
                strJsonB = strJsonB.Replace("(StartedTime)", ActiveTime);
                strJsonB = strJsonB.Replace("(ActiveTime)", ActiveTime);
                strJsonB = strJsonB.Replace("(RowCount)", intRowCount.ToString());
                strJsonB = strJsonB.Replace("(DurationFeeder)", lngDuration.ToString());
                try
                {
                    long subscriberCount = await _redisRepo.PublishAsync(EGlobalConfig.m_strChannelMonitor, strJsonB);

                    if (subscriberCount == -1)
                    {
                        _logger.LogWarning("Gửi status tới Monitor thất bại (lỗi publish).");
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
            catch (Exception ex) 
            {
                throw ex;
            }
        }
        public bool SendStatusToMonitor(AppList appList, string statusData)
        {
            try
            {
                this.SendStatusToMonitor(this.GetLocalDateTime(), this.GetLocalIP(), appList, statusData);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monitor - public SendStatusToMonitor");
                return false;
            }
        }
        private void SendStatusToMonitor(string localDateTime, string localIp, AppList appList, string statusData)
        {
            try
            {
                string strJsonB = EGlobalConfig.TEMPLATE_JSON_TYPE_STATUS;
                string strAppName = "";
                string strRowID = strAppName;
                string strKey = strRowID;
                string strOut = "";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monitor - private SendStatusToMonitor");
            }
        }
        public string GetLocalDateTime()
        {
            try
            {
                string strDateTime = DateTime.Now.ToString(EGlobalConfig.DATETIME_MONITOR);

                return strDateTime;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Monitor - GetLocalDateTime");
                return null;
            }
        }
        public string GetLocalIP() 
        {
            try
            {
                string sHostName = Dns.GetHostName();
                System.Net.IPHostEntry ipE = Dns.GetHostByName(sHostName);
                IPAddress[] IpA = ipE.AddressList;
                foreach (IPAddress ip in IpA)
                {
                    string strLocalIP = ip.ToString();
                    if (strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_FOX) != -1       //= "172.16.0.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_HSX) != -1       //= "10.26.248.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_VM) != -1       //= "10.26.249.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_HNX) != -1       //= "10.26.100.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_FPTS) != -1       //= "10.26.2.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_FPTS_BLAZE) != -1   //= "10.26.5.";
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_FPTS_4) != -1   //= "10.26.4."; 2018-08-15 09:01:56 hungpv
                    || strLocalIP.IndexOf(EGlobalConfig._PREFIX_IP_LAN_FPTS_7) != -1
                    )
                        return strLocalIP;
                }
                return "";
            }
            catch (Exception ex) 
            {
                _logger.LogError(ex, "Monitor - GetLocalIP");
                return null;
            }
        }
    }
}
