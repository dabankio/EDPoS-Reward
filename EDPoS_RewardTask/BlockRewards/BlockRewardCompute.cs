using EDPoS_Reward.Common;
using EDPoS_Reward.SqlHelper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;

namespace EDPoS_Reward.BlockRewards
{
    class BlockRewardCompute
    {
        SqlDataProvider dataProvider = null;

        // Here is the ID in the income table
        // Daily rewards are calculated from the specified ID value
        // It can be setted as 0 or the height of the first edpos block
        private int startHeight = 0;
        private int ConfirmHeight = 30;

        public BlockRewardCompute()
        {
            dataProvider = new SqlDataProvider();
            ConfirmHeight = Convert.ToInt32(ConfigurationManager.AppSettings["ConfirmHeight"].ToString());
        }

        /// <summary>
        ///  Calculate the reward according to the edpos block of each node
        /// </summary>
        public bool BlockRewardStat()
        {
            try
            {
                // Start quotation marks for the next round of calculations. 
                // if the program had been interrupted, 
                // the next compute will be after the next digit of the serial number of the previous round of calculations
                int lastBlockHeight = dataProvider.ReturnIntValue("select max(block_height) from DposRewardDetails");
                if (lastBlockHeight > 0)
                {
                    startHeight = lastBlockHeight + 1;
                }

                //Get effective height， 有效高度
                int newHeight = dataProvider.ReturnIntValue(@"select height from Block where is_useful = 1 order by height desc limit 1 ");
                dataProvider.AddParam("?startHeight", startHeight);
                dataProvider.AddParam("?endHeight", newHeight - ConfirmHeight);
                DataTable dt = dataProvider.ExecDataSet(
                    @"select id,hash,FROM_UNIXTIME(time,'%Y-%m-%d') as time,height,reward_address,reward_money from Block 
                      where type = 'primary-dpos' 
                      and is_useful = 1 
                      and reward_state = 0 
                      and height between ?startHeight and ?endHeight 
                      order by height asc 
                      limit 1000").Tables[0]; //dpos出块奖励blocks

                if (dt.Rows.Count > 0)
                {
                    lastBlockHeight = dt.AsEnumerable().Max(x => x.Field<int>("height")); //max height in result
                    string msg = "[Block Reward] Scope of index : [" + startHeight + " ——>> " + lastBlockHeight + "]";
                    Console.WriteLine(msg);
                    Debuger.Trace(msg);
                }
                else
                {
                    string msg = "[Block Reward] waiting for the next DPoS block. The next round of the compute will starts at the height of " + startHeight;
                    Console.WriteLine(msg);
                    Debuger.Trace(msg);
                    return false;
                }

                int minid = GetTxMinID(); //某个固定的边界(tx id)
                foreach (DataRow r in dt.Rows) //range blocks
                {
                    // Voters is used to record the number of voters and casts
                    Dictionary<string, decimal> voters = new Dictionary<string, decimal>(); //key: 投票人, value: 投票金额
                    // Redem_voters is used to record the number of the voters and tokens who have redeemed
                    Dictionary<string, decimal> redeem_voters = new Dictionary<string, decimal>(); //key: 投票人, value: 赎回金额
                    // The voting result set and the redemption result set are merged and combined
                    // Adding and subtracting can be done which have the same key
                    Dictionary<string, decimal> merge_voters = new Dictionary<string, decimal>(); //投票-赎回
                    List<string> listSql = new List<string>();
                    int maxid = GetTxMaxID(r["hash"].ToString()); //max(id) from tx where hash = ?

                    decimal selfVote = 0, selfIn = 0, selfOut = 0;

                    if (int.Parse(r["height"].ToString()) <= 248997) //2020-05-14 23:38:13, blockHash: 0003cca552ae27b26db2339c8180b007c2ded18f92027fe381a30f3cca16262f
                    {
                        dataProvider.AddParam("?block_hash", r["hash"].ToString());
                        dataProvider.AddParam("?address", r["reward_address"].ToString());
                        selfVote = dataProvider.ReturnDecimalValue("select amount from Tx where block_hash=?block_hash and `to`=?address and type='certification' and n=0");
                        if (selfVote == 0)
                        {
                            dataProvider.AddParam("?address", r["reward_address"].ToString());
                            selfVote = dataProvider.ReturnDecimalValue("select vote_amount from DposRewardDetails where dpos_addr=?address and client_addr=?address order by block_height desc limit 1");

                            string msg = "[" + r["reward_address"].ToString() + "] height: " + r["height"].ToString() + ", selfVote: " + selfVote.ToString();
                            Console.WriteLine(msg);
                            Debuger.Trace(msg);
                        }
                    }
                    else
                    {
                        dataProvider.AddParam("?minid", minid);
                        dataProvider.AddParam("?maxid", maxid - 1);
                        dataProvider.AddParam("?address", r["reward_address"].ToString());

                        selfIn = dataProvider.ReturnDecimalValue(@"
                        select sum(amount) 
                        from Tx 
                        where `to`=?address and (form<>?address or (type='certification' and n=1)) and id between ?minid and ?maxid"); //节点自投

                        dataProvider.AddParam("?minid", minid);
                        dataProvider.AddParam("?maxid", maxid);
                        dataProvider.AddParam("?address", r["reward_address"].ToString());

                        selfOut = dataProvider.ReturnDecimalValue(@"
                        select sum(amount) 
                        from Tx 
                        where form=?address and `to`<>?address and id between ?minid and ?maxid"); //节点自投赎回
                        selfVote = selfIn - selfOut;
                        string msg = "[" + r["reward_address"].ToString() + "] height: " + r["height"].ToString() + ",selfVote: " + selfVote.ToString() + " selfIn:" + selfIn.ToString() + " selfOut: " + selfOut.ToString();

                        Console.WriteLine(msg);
                        Debuger.Trace(msg);
                    }

                    if (voters.ContainsKey(r["reward_address"].ToString()))
                    {
                        voters[r["reward_address"].ToString()] = selfVote;
                    }
                    else
                    {
                        voters.Add(r["reward_address"].ToString(), selfVote);//delegate vote to self
                    }

                    dataProvider.AddParam("?address", r["reward_address"].ToString());
                    dataProvider.AddParam("?id", maxid);
                    DataSet ds = dataProvider.ExecDataSet(
                        @"select client_in,ifnull(sum(amount),0) as amount 
                          from Tx 
                          where dpos_in=?address and id <=?id 
                          group by client_in;

                          select client_out,ifnull(sum(amount+free),0) as amount 
                          from Tx 
                          where dpos_out=?address and id <=?id group by client_out;");

                    DataTable dtVotes = ds.Tables[0]; //一般地址投票
                    DataTable dtRedeem = ds.Tables[1]; //一般地址赎回
                    foreach (DataRow voter in dtVotes.Rows) //range votes
                    {
                        if (voters.ContainsKey(voter["client_in"].ToString()))
                        {
                            voters[voter["client_in"].ToString()] += decimal.Parse(voter["amount"].ToString());
                        }
                        else
                        {
                            voters.Add(voter["client_in"].ToString(), decimal.Parse(voter["amount"].ToString()));
                        }
                    }

                    foreach (DataRow voter in dtRedeem.Rows) //range redeem
                    {
                        if (redeem_voters.ContainsKey(voter["client_out"].ToString()))
                        {
                            redeem_voters[voter["client_out"].ToString()] -= decimal.Parse(voter["amount"].ToString());
                        }
                        else
                        {
                            redeem_voters.Add(voter["client_out"].ToString(), -decimal.Parse(voter["amount"].ToString()));
                        }
                    }

                    // The voting result set and the redemption result set are merged and combined
                    // Adding and subtracting can be done which have the same key
                    var m = voters.Keys.Union(redeem_voters.Keys);
                    foreach (var v in m)
                    {
                        merge_voters.Add(v, (voters.ContainsKey(v) ? voters[v] : 0) + (redeem_voters.ContainsKey(v) ? redeem_voters[v] : 0));
                    }
                    decimal totalVoteAmount = merge_voters.Select(x => x.Value).Sum();

                    var dpos_miner_address = r["reward_address"].ToString();
                    var block_coinbase_amount = Decimal.Parse(r["reward_money"].ToString());
                    var reward_date = DateTime.Parse(r["time"].ToString()).ToString("yyyy-MM-dd");
                    var block_height = r["height"].ToString();

                    foreach (KeyValuePair<string, decimal> kv in merge_voters)
                    {
                        string voterAddress = kv.Key;
                        decimal voteAmount = kv.Value;
                        if (voteAmount != 0)
                        {
                            decimal voteRewardAmount = voteAmount / totalVoteAmount * block_coinbase_amount;
                            listSql.Add(@"insert into DposRewardDetails(dpos_addr,client_addr,vote_amount,reward_money,reward_date,block_height) 
                            values('" + dpos_miner_address + "','" + voterAddress + "'," + voteAmount + "," + voteRewardAmount + ",'" + reward_date + "'," + block_height + ")");
                        }
                    }
                    // Set the state of the block as 1,it shows that this block has been used
                    listSql.Add("update Block set reward_state=1 where id='" + r["id"].ToString() + "'");
                    dataProvider.ExecSqlTran(listSql);
                }

                return true;
            }
            catch (Exception ex)
            {
                string msg = "[Block Reward Exception] " + ex.Message;
                Console.WriteLine(msg);
                Debuger.Error(msg);
                return false;
            }
        }

        /// <summary>
        /// Get the max id of Tx by block hash
        /// </summary>
        /// <param name="block_hash">block hash</param>
        /// <returns>Max ID</returns>
        private int GetTxMaxID(string block_hash)
        {
            dataProvider.AddParam("?block_hash", block_hash);
            return dataProvider.ReturnIntValue("select max(id) from Tx where block_hash=?block_hash");
        }

        private int GetTxMinID()
        {
            return dataProvider.ReturnIntValue(@"
            select min(t.id) 
            from Tx t join Block b on t.block_hash=b.hash 
            where b.height=248969");//0003cc89bd7cbedf905a52595e0ae6a923fe1a245d0ea1406642dabf79eefde7, 2020-05-14 23:14:33
        }
    }
}
