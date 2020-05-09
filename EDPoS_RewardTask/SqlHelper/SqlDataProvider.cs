using EDPoS_Reward.Common;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Reflection;
using System.Text;

namespace EDPoS_Reward.SqlHelper
{
    class SqlDataProvider
    {
        private MySqlConnection conn;
        private MySqlCommand cmd;
        private MySqlDataAdapter Adapter;
        private MySqlParameter Para;
        public SqlDataProvider()
        {
            conn = new MySqlConnection();
            conn.ConnectionString = ConfigurationManager.AppSettings["ConnStr"].ToString();
            cmd = new MySqlCommand();
            cmd.Connection = conn;
        }

        /// <summary>
        /// 添加参数到参数集合
        /// </summary>
        /// <param name="ParaName">参数名</param>
        /// <param name="ParaValue">参数值</param>
        /// <param name="ParaCollect">参数集合</param>
        public void AddParam(string ParaName, object ParaValue)
        {
            cmd.Parameters.AddWithValue(ParaName, ParaValue);
        }

        /// <summary>
        /// 添加存储过程输出参数(输出参数的值为字符型时需指定字节大小)
        /// </summary>
        /// <param name="ParaName">参数名称</param>
        /// <param name="type">字节大小</param>
        public void AddOutputParam(string ParaName, int size)
        {
            Para = new MySqlParameter();
            Para.ParameterName = ParaName;
            Para.Size = size;
            Para.Direction = ParameterDirection.Output;
            cmd.Parameters.Add(Para);
        }

        /// <summary>
        /// 添加存储过程输出参数
        /// </summary>
        /// <param name="ParaName">参数名称</param>
        /// <param name="type">参数类型</param>
        public void AddOutputParam(string ParaName, DbType type)
        {
            Para = new MySqlParameter();
            Para.ParameterName = ParaName;
            Para.DbType = type;
            Para.Direction = ParameterDirection.Output;
            cmd.Parameters.Add(Para);
        }

        /// <summary>
        /// 添加存储过程返回值参数
        /// </summary>
        /// <param name="ParaName">参数名称</param>
        /// <param name="type">字节大小</param>
        public void AddReturnParam(string ParaName, int size)
        {
            Para = new MySqlParameter();
            Para.ParameterName = ParaName;
            Para.Size = size;
            Para.Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(Para);
        }

        /// <summary>
        /// 添加存储过程返回值参数
        /// </summary>
        /// <param name="ParaName">参数名称</param>
        /// <param name="type">参数类型</param>
        public void AddReturnParam(string ParaName, DbType type)
        {
            Para = new MySqlParameter();
            Para.ParameterName = ParaName;
            Para.DbType = type;
            Para.Direction = ParameterDirection.ReturnValue;
            cmd.Parameters.Add(Para);
        }


        /// <summary>
        /// 获取存储过程输出参数或返回值参数的值
        /// </summary>
        /// <returns></returns>
        public string GetParmValue(string param)
        {
            return cmd.Parameters[param].Value.ToString();
        }

        /// <summary>
        /// 清空参数集合(存储过程调用完毕后可执行此方法)
        /// </summary>
        public void ClearParameters()
        {
            cmd.Parameters.Clear();
        }

        /// <summary>
        /// 执行SQL并返回数据集
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public DataSet ExecDataSet(string Sql)
        {
            Adapter = new MySqlDataAdapter();
            DataSet ds = new DataSet();
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                Adapter.SelectCommand = cmd;
                Adapter.SelectCommand.CommandTimeout = 1200;//可以设置适当的超时时间(秒)，避免选择时间段过大导致填充数据集超时
                Adapter.Fill(ds);
                cmd.Parameters.Clear();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
            return ds;

        }

        /// <summary>
        /// 执行SQL语句并返回DataReader对象
        /// </summary>
        /// <param name="dbcon"></param>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public MySqlDataReader ExecuteDataReader(string cmdText)
        {

            try
            {
                this.Open();
                cmd.CommandText = cmdText;
                cmd.CommandType = CommandType.Text;
                MySqlDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                cmd.Parameters.Clear();
                return dr;
            }
            catch
            {
                this.Close();//发生异常在此处关闭，否则在调用显式处关闭
                return null;
            }

        }


        /// <summary>
        /// 判断记录是否存在
        /// </summary>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public bool Exist(string Sql)
        {
            bool exist;
            this.Open();
            cmd.CommandText = Sql;
            cmd.CommandType = CommandType.Text;
            MySqlDataReader dr = cmd.ExecuteReader();
            cmd.Parameters.Clear();
            if (dr.HasRows)
            {
                exist = true;   //记录存在
            }
            else
            {
                exist = false;  //记录不存在
            }
            dr.Close();
            this.Close();
            return exist;
        }

        /// <summary>
        /// 执行SQL语句
        /// </summary>
        /// <param name="sql"></param>
        public void ExecSql(string Sql)
        {
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                cmd.Parameters.Clear();
                this.Close();
            }
        }

        /// <summary>
        /// 执行SQL语句
        /// </summary>
        /// <param name="sql"></param>
        public void ExecSql(string Sql, out int row)
        {
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                row = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
        }

        /// <summary>
        /// 执行SQL语句,返回一个单值
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public string ReturnValue(string Sql)
        {
            object returnValue = string.Empty;
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                returnValue = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                if (returnValue == null)
                {
                    returnValue = string.Empty;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
            return returnValue.ToString();
        }

        /// <summary>
        /// 执行SQL语句,返回一个单值
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public int ReturnIntValue(string Sql)
        {
            object returnValue = 0;
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                returnValue = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                if (returnValue == null)
                {
                    returnValue = 0;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
            int retValue;
            int.TryParse(returnValue.ToString(), out retValue);
            return retValue;
        }

        /// <summary>
        /// 返回一个decimal类型值
        /// </summary>
        /// <param name="Sql"></param>
        /// <returns></returns>
        public decimal ReturnDecimalValue(string Sql)
        {
            object returnValue = 0;
            try
            {
                this.Open();
                cmd.CommandText = Sql;
                cmd.CommandType = CommandType.Text;
                returnValue = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                if (returnValue == null)
                {
                    returnValue = 0;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
            return Decimal.Parse(returnValue.ToString());
        }

        /// <summary>
        /// 执行多条SQL语句并启用数据库事务
        /// </summary>
        /// <param name="SQLStringList"></param>		
        public bool ExecSqlTran(List<String> SQLStringList)
        {
            this.Open();
            MySqlTransaction trans = conn.BeginTransaction();
            cmd.Transaction = trans;
            try
            {
                for (int n = 0; n < SQLStringList.Count; n++)
                {
                    cmd.CommandText = SQLStringList[n];
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                }
                cmd.Parameters.Clear();
                trans.Commit();
                return true;
            }
            catch (Exception ex)
            {
                Debuger.Error(ex.Message);
                trans.Rollback();
                return false;
            }
            finally
            {
                this.Close();
            }

        }

        /// <summary>
        /// 执行存储过程并返回结果集
        /// </summary>
        /// <param name="storedProcName">存储过程名</param>
        /// <returns>DataSet</returns>
        public DataSet RunProcedure(string storedProcName)
        {
            Adapter = new MySqlDataAdapter();
            DataSet ds = new DataSet();
            try
            {
                this.Open();
                cmd.CommandText = storedProcName;
                cmd.CommandType = CommandType.StoredProcedure;
                Adapter.SelectCommand = cmd;
                Adapter.SelectCommand.CommandTimeout = 1200;//可以设置适当的超时时间(秒)，避免选择时间段过大导致填充数据集超时
                Adapter.Fill(ds);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();

            }
            return ds;

        }

        /// <summary>
        /// 执行存储过程，方法不返回结果集
        /// </summary>
        /// <param name="storedProcName"></param>
        public void RunVoidProcedure(string storedProcName)
        {
            cmd.CommandText = storedProcName;
            cmd.CommandType = CommandType.StoredProcedure;

            try
            {
                this.Open();
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                this.Close();
            }
        }


        /// <summary>
        /// 将实体类的值插入对应的数据库表
        /// </summary>
        /// <param name="model">实体对象</param>
        public void EntityInsert(object model)
        {
            Type T = model.GetType();
            PropertyInfo[] propert = T.GetProperties();
            string fields = string.Empty;
            string fieldValue = string.Empty;
            string Sql = string.Empty;
            for (int i = 0; i < propert.Length; i++)
            {
                if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(model, null) == null) //可空类型判断,主要针对DateTime类型
                {
                    AddParam("?" + propert[i].Name, DBNull.Value);
                }
                else
                {
                    AddParam("?" + propert[i].Name, propert[i].GetValue(model, null) == null ? DBNull.Value : propert[i].GetValue(model, null)); //将实体类的属性进行参数转换
                }
                fields += propert[i].Name + ",";
                fieldValue += "?" + propert[i].Name + ",";

            }
            Sql = "INSERT INTO " + T.Name + "(" + fields.TrimEnd(',') + ") VALUES(" + fieldValue.TrimEnd(',') + ")";
            ExecSql(Sql);
        }

        /// <summary>
        /// 将实体类的值插入对应的数据库表，过滤无需插入的字段，如自增字段
        /// </summary>
        /// <param name="model">实体对象</param>
        /// <param name="FilterFields">过滤字段</param>
        public void EntityInsert(object model, List<string> FilterFields)
        {
            Type T = model.GetType();
            PropertyInfo[] propert = T.GetProperties();
            string fields = string.Empty;
            string fieldValue = string.Empty;
            string Sql = string.Empty;
            for (int i = 0; i < propert.Length; i++)
            {
                if (FilterFields.Contains(propert[i].Name))
                {
                    continue;
                }
                else
                {
                    if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(model, null) == null) //可空类型判断,主要针对DateTime类型
                    {
                        AddParam("?" + propert[i].Name, DBNull.Value);
                    }
                    else
                    {
                        AddParam("?" + propert[i].Name, propert[i].GetValue(model, null) == null ? DBNull.Value : propert[i].GetValue(model, null)); //将实体类的属性进行参数转换
                    }
                    fields += propert[i].Name + ",";
                    fieldValue += "?" + propert[i].Name + ",";
                }

            }
            Sql = "INSERT INTO " + T.Name + "(" + fields.TrimEnd(',') + ") VALUES(" + fieldValue.TrimEnd(',') + ")";
            ExecSql(Sql);
        }

        /// <summary>
        /// 执行实体类集合插入并启用数据库事务
        /// </summary>
        /// <param name="listModel">实体类集合</param>
        /// <returns></returns>
        public bool ExecModelTran(List<Object> listModel)
        {
            this.Open();
            MySqlTransaction trans = conn.BeginTransaction();
            cmd.Transaction = trans;
            try
            {
                for (int n = 0; n < listModel.Count; n++)
                {
                    Type T = listModel[n].GetType();
                    PropertyInfo[] propert = T.GetProperties();
                    string fields = string.Empty;
                    string fieldValue = string.Empty;
                    string Sql = string.Empty;
                    for (int i = 0; i < propert.Length; i++)
                    {
                        if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(listModel[n], null) == null) //可空类型判断,主要针对DateTime类型
                        {
                            AddParam("?" + propert[i].Name, DBNull.Value);
                        }
                        else
                        {
                            AddParam("?" + propert[i].Name, propert[i].GetValue(listModel[n], null) == null ? DBNull.Value : propert[i].GetValue(listModel[n], null)); //将实体类的属性进行参数转换
                        }
                        fields += propert[i].Name + ",";
                        fieldValue += "?" + propert[i].Name + ",";

                    }
                    Sql = "INSERT INTO " + T.Name + "(" + fields.TrimEnd(',') + ") VALUES(" + fieldValue.TrimEnd(',') + ")";
                    cmd.CommandText = Sql;
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }
                trans.Commit();
                return true;
            }
            catch (Exception ex)
            {
                Debuger.Error(ex.Message);
                trans.Rollback();
                return false;
            }
            finally
            {
                this.Close();
            }

        }

        /// <summary>
        /// 执行实体类集合插入并启用数据库事务
        /// </summary>
        /// <param name="listModel">实体类集合</param>
        /// <param name="FilterFields">过滤字段(应用于所有实体对象)</param>
        /// <returns></returns>
        public bool ExecModelTran(List<Object> listModel, List<string> FilterFields)
        {
            this.Open();
            MySqlTransaction trans = conn.BeginTransaction();
            cmd.Transaction = trans;
            try
            {
                for (int n = 0; n < listModel.Count; n++)
                {
                    Type T = listModel[n].GetType();
                    PropertyInfo[] propert = T.GetProperties();
                    string fields = string.Empty;
                    string fieldValue = string.Empty;
                    string Sql = string.Empty;
                    for (int i = 0; i < propert.Length; i++)
                    {
                        if (FilterFields.Contains(propert[i].Name))
                        {
                            continue;
                        }
                        else
                        {
                            if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(listModel[n], null) == null) //可空类型判断,主要针对DateTime类型
                            {
                                AddParam("?" + propert[i].Name, DBNull.Value);
                            }
                            else
                            {
                                AddParam("?" + propert[i].Name, propert[i].GetValue(listModel[n], null) == null ? DBNull.Value : propert[i].GetValue(listModel[n], null)); //将实体类的属性进行参数转换
                            }
                            fields += propert[i].Name + ",";
                            fieldValue += "?" + propert[i].Name + ",";
                        }

                    }
                    Sql = "INSERT INTO " + T.Name + "(" + fields.TrimEnd(',') + ") VALUES(" + fieldValue.TrimEnd(',') + ")";
                    cmd.CommandText = Sql;
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                    cmd.Parameters.Clear();
                }
                trans.Commit();
                return true;
            }
            catch
            {
                trans.Rollback();
                return false;
            }
            finally
            {
                this.Close();
            }

        }



        /// <summary>
        /// 基于主键将实体类的值更新到对应的数据库表
        /// </summary>
        /// <param name="model">实体对象</param>
        /// <param name="PrimaryKey">主键字段名称</param>
        public void EntityUpdate(object model, string PrimaryKey)
        {
            Type T = model.GetType();
            PropertyInfo[] propert = T.GetProperties();
            string fields = string.Empty;
            string Sql = string.Empty;
            for (int i = 0; i < propert.Length; i++)
            {
                if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(model, null) == null) //可空类型判断,主要针对DateTime类型
                {
                    AddParam("?" + propert[i].Name, DBNull.Value);
                }
                else
                {
                    AddParam("?" + propert[i].Name, propert[i].GetValue(model, null) == null ? DBNull.Value : propert[i].GetValue(model, null)); //将实体类的属性进行参数转换  
                }
                if (propert[i].Name != PrimaryKey) //自增主键列不更新
                {
                    fields += propert[i].Name + " = " + "?" + propert[i].Name + ",";
                }
            }
            Sql = "UPDATE " + T.Name + " SET " + fields.TrimEnd(',') + " WHERE " + PrimaryKey + " = ?" + PrimaryKey;
            ExecSql(Sql);

        }

        /// <summary>
        /// 基于主键将实体类的值更新到对应的数据库表，通过过滤无需更新的字段
        /// </summary>
        /// <param name="model">实体对象</param>
        /// <param name="PrimaryKey">主键字段名称</param>
        /// <param name="FilterFields">过滤字段</param>
        public void EntityFilterUpdate(object model, string PrimaryKey, List<string> FilterFields)
        {
            Type T = model.GetType();
            PropertyInfo[] propert = T.GetProperties();
            string fields = string.Empty;
            string Sql = string.Empty;
            for (int i = 0; i < propert.Length; i++)
            {
                if (FilterFields.Contains(propert[i].Name))
                {
                    continue;
                }
                else
                {
                    if (propert[i].PropertyType.Name == "Nullable`1" && propert[i].GetValue(model, null) == null) //可空类型判断,主要针对DateTime类型
                    {
                        AddParam("?" + propert[i].Name, DBNull.Value);
                    }
                    else
                    {
                        AddParam("?" + propert[i].Name, propert[i].GetValue(model, null) == null ? DBNull.Value : propert[i].GetValue(model, null)); //将实体类的属性进行参数转换  
                    }
                    if (propert[i].Name != PrimaryKey) //自增主键列不更新
                    {
                        fields += propert[i].Name + " = " + "?" + propert[i].Name + ",";
                    }
                }
            }
            Sql = "UPDATE " + T.Name + " SET " + fields.TrimEnd(',') + " WHERE " + PrimaryKey + " = ?" + PrimaryKey;
            ExecSql(Sql);

        }


        /// <summary>
        /// 通过反射将取出的数据写入实体类(查询的字段名必须要求与实体类属性全字符匹配)
        /// </summary>
        /// <param name="model"></param>
        /// <param name="cmdText"></param>
        public void GetModel(object model, string cmdText)
        {
            PropertyInfo propertyInfo;
            MySqlDataReader dr = ExecuteDataReader(cmdText);
            while (dr.Read())
            {
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    propertyInfo = model.GetType().GetProperty(dr.GetName(i));
                    if (propertyInfo != null)
                    {
                        if (dr.GetValue(i) != DBNull.Value)
                        {
                            if (propertyInfo.PropertyType.ToString() == "System.Single")
                            {

                                propertyInfo.SetValue(model, Convert.ToSingle(dr.GetValue(i)), null);
                            }
                            else
                            {
                                propertyInfo.SetValue(model, dr.GetValue(i), null);
                            }
                        }
                    }
                }
            }
            dr.Close();
            this.Close();
        }

        /// <summary>
        /// 用于实体类有外键关联的模型
        /// </summary>
        /// <param name="model">实体类</param>
        /// <param name="selectField">要查询的自段</param>
        /// <param name="keyField">主键字段名称</param>
        /// <param name="keyValue">主键值</param>
        public void GetSubModel(object model, string selectField, string keyField, string keyValue)
        {
            PropertyInfo propertyInfo;
            AddParam("?" + keyField, keyValue);
            MySqlDataReader dr = ExecuteDataReader("SELECT " + selectField + " FROM " + model.GetType().Name + " WHERE " + keyField + "=?" + keyField);
            while (dr.Read())
            {
                for (int i = 0; i < dr.FieldCount; i++)
                {
                    propertyInfo = model.GetType().GetProperty(dr.GetName(i));
                    if (propertyInfo != null)
                    {
                        if (dr.GetValue(i) != DBNull.Value)
                        {
                            propertyInfo.SetValue(model, dr.GetValue(i), null);
                        }
                    }
                }
            }
            dr.Close();
            this.Close();
        }


        private void Open()
        {
            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }
        }

        private void Close()
        {
            if (conn.State == ConnectionState.Open)
            {
                conn.Close();
            }
        }
    }
}
