using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;

namespace EDPoS_Reward.Common
{
    public static class Debuger
    {
        public static string strLogFolder = Path.Combine(Directory.GetCurrentDirectory(), "BBCRewardLog");

        /// <summary>
        /// 输出调试信息
        /// </summary>
        /// <param name="message"></param>
        public static void Trace(string message)
        {
            string strMsg = string.Format("[{0} tid={1}] Trace:{2}", DateTime.Now.ToString("HH:mm:ss"), Thread.CurrentThread.ManagedThreadId, message);
            AddLog(strMsg);
        }

        /// <summary>
        /// 输出错误信息
        /// </summary>
        /// <param name="message"></param>
        public static void Error(string message)
        {
            string strMsg = string.Format("[{0} tid={1}] Error:{2}", DateTime.Now.ToString("HH:mm:ss"), Thread.CurrentThread.ManagedThreadId, message);
            AddLog(strMsg);
        }

        /// <summary>
        /// 日志记录
        /// </summary>
        /// <param name="obj"></param>
        public static void AddLog(string message)
        {
            if (!Directory.Exists(strLogFolder))
                Directory.CreateDirectory(strLogFolder);

            using (StreamWriter sw = new StreamWriter(strLogFolder + DateTime.Now.ToString("yyyyMMdd") + ".log", true))
            {
                sw.WriteLine(message);
            }
        }

        /// <summary>
        /// 独立日志记录跟踪信息
        /// </summary>
        /// <param name="message"></param>
        public static void TraceAlone(string message, string type)
        {
            string strMsg = string.Format("[{0} tid={1}] Trace:{2}", DateTime.Now.ToString("HH:mm:ss"), Thread.CurrentThread.ManagedThreadId, message);
            if (!Directory.Exists(strLogFolder))
                Directory.CreateDirectory(strLogFolder);

            using (StreamWriter sw = new StreamWriter(strLogFolder + DateTime.Now.ToString("yyyyMMdd") + type + ".log", true))
            {
                sw.WriteLine(strMsg);
            }
        }
    }
}
