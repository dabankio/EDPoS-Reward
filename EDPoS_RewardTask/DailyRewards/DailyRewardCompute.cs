using EDPoS_Reward.Common;
using EDPoS_Reward.SqlHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace EDPoS_Reward.DailyRewards
{
    /// <summary>
    /// Daily voting income summary
    /// </summary>
    class DailyRewardCompute
    {
        SqlDataProvider dataProvider = new SqlDataProvider();
        // Here is the ID in the income table
        // Daily rewards are calculated from the specified ID value. 
        // Each reading of 1000 pieces of data for compute, the results will be inserted into the daily income table
        private int startIndex = 0;

        ///<summary> 
        ///汇总数据dpos奖励数据，return 是否有数据更新
        ///</summary>
        public bool DailyRewardStat()
        {
            try
            {
                List<string> listSql = new List<string>();
                // Start quotation marks for the next round of calculations. 
                // if the program had been interrupted, 
                // the next compute will be after the next digit of the serial number of the previous round of calculations
                int lastIndex = dataProvider.ReturnIntValue("select max(id) from DposRewardDetails where reward_state=1"); //最新的出块奖励详情id（已计算汇总
                if (lastIndex > 0)
                {
                    startIndex = lastIndex + 1;
                }

                //最老1000条未计算汇总记录的 max(id)
                int endIndex = dataProvider.ReturnIntValue(@"select max(id) from (
                    select id from DposRewardDetails where reward_state=0 order by id limit 1000
                ) t");
                if (endIndex <= 0)
                {
                    Console.WriteLine("[Daily Reward] waiting ...");
                    Debuger.TraceAlone("[Daily Reward] waiting ...", "daily");
                    return false;
                }

                Console.WriteLine("[Daily Reward] C Scope of index : [" + startIndex + " ——>> " + endIndex + "]");
                Debuger.TraceAlone("[Daily Reward] Scope of index : [" + startIndex + " ——>> " + endIndex + "]", "daily");
                dataProvider.AddParam("?startIndex", startIndex);
                dataProvider.AddParam("?endIndex", endIndex);
                DataTable dt = dataProvider.ExecDataSet(@"
select dpos_addr,client_addr,sum(reward_money) as reward,reward_date 
from DposRewardDetails 
where reward_state=0 and id between ?startIndex and ?endIndex 
group by dpos_addr,reward_date,client_addr").Tables[0];

                foreach (DataRow r in dt.Rows)
                {
                    //  Summary of historical income data and dynamic summary of day income
                    var reward_date = DateTime.Parse(r["reward_date"].ToString()).ToString("yyyy-MM-dd");
                    bool dailyRewardOfAddressExist = dataProvider.Exist("select id from DposDailyReward where dpos_addr='" + r["dpos_addr"].ToString() + "' and client_addr='" + r["client_addr"].ToString() + "' and payment_date='" + reward_date + "'");
                    if (dailyRewardOfAddressExist)
                    {
                        listSql.Add("update DposDailyReward set payment_money=payment_money+" + r["reward"].ToString() + " where dpos_addr='" + r["dpos_addr"].ToString() + "' and client_addr='" + r["client_addr"].ToString() + "' and payment_date='" + reward_date + "'");
                    }
                    else
                    {
                        listSql.Add("insert into DposDailyReward(dpos_addr,client_addr,payment_date,payment_money) values('" + r["dpos_addr"].ToString() + "','" + r["client_addr"].ToString() + "','" + reward_date + "'," + r["reward"].ToString() + ")");
                    }
                }

                if (listSql.Count <= 0)
                {
                    return false;
                }

                listSql.Add("update DposRewardDetails set reward_state=1 where id between " + startIndex + " and " + endIndex);
                return dataProvider.ExecSqlTran(listSql);
            }
            catch (Exception ex)
            {
                Console.WriteLine("[Daily Reward Exception] " + ex.Message);
                Debuger.TraceAlone("[Daily Reward Exception] " + ex.Message, "daily");
                return false;
            }
        }
    }
}
