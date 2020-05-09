using EDPoS_Reward.BlockRewards;
using EDPoS_Reward.DailyRewards;
using System;
using System.Configuration;
using System.Threading;

namespace EDPoS_RewardTask
{
    class Program
    {
        private static int Interval = int.Parse(ConfigurationManager.AppSettings["Interval"].ToString());
        private static int StatInterval = int.Parse(ConfigurationManager.AppSettings["StatInterval"].ToString());

        private static Thread thrBlockReward = null;
        private static Thread thrDailyReward = null;

        static void Main(string[] args)
        {
            thrBlockReward = new Thread(new ThreadStart(BlockReward));
            thrDailyReward = new Thread(new ThreadStart(DailyReward));

            if (thrBlockReward.ThreadState == ThreadState.Stopped || thrBlockReward.ThreadState == ThreadState.Unstarted)
            {
                thrBlockReward.Start();
            }

            if (thrDailyReward.ThreadState == ThreadState.Stopped || thrDailyReward.ThreadState == ThreadState.Unstarted)
            {
                thrDailyReward.Start();
            }
        }

        private static void BlockReward()
        {
            BlockRewardCompute bll = new BlockRewardCompute();
            while (true)
            {
                var re = bll.BlockRewardStat();
                if (!re)
                {
                    Thread.Sleep(Interval * 1000);
                }
            }
        }

        private static void DailyReward()
        {
            DailyRewardCompute bll = new DailyRewardCompute();
            while (true)
            {
                var re = bll.DailyRewardStat();
                if (!re)
                {
                    Thread.Sleep(Interval * 1000);
                }
            }
        }
    }
}
