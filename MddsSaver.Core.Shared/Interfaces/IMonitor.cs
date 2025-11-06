using MddsSaver.Core.Shared.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Interfaces
{
    /// <summary>
	/// interface monitor cho cac monitor client (tat ca project se dung)
	/// </summary>
    public interface IMonitor
    {
        //string GetAppName(AppList appList);
        bool SendStatusToMonitor(AppList appList, string statusData);
        string GetLocalDateTime();
        string GetLocalIP();
    }
}
