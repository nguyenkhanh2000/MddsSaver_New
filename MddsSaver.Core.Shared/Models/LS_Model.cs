using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MddsSaver.Core.Shared.Models
{
    public class LS_Model
    {
        public string SQ { get; set; }
        public long CN { get; set; } // lấy timestamp gán vào CN - mục đích tránh mất data khi value trùng nhau
        public string MT { get; set; }
        public int MP { get; set; }
        public long MQ { get; set; }
        public long TV { get; set; }
        public long TQ { get; set; }
        public string SIDE { get; set; }
        //constructor
        public LS_Model()
        {
            SIDE = string.Empty;
        }
    }
}
