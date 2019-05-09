//string conString = "Data Source=r04phidwh50; Initial Catalog=PCS_LABMed; Integrated Security=True;";
using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Text;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace GraphAnalysisTimeWindow
{
    public class SqlTable
    {
        public static int ExecuteNonQuery(string sqlConString, string sqlCode)
        {
            int rowsAffected;
            using (SqlConnection connection = new SqlConnection(sqlConString))
            {
                connection.Open();
                SqlCommand sqlCreateTable = new SqlCommand(sqlCode, connection);
                sqlCreateTable.CommandTimeout = 0; //30 min
                rowsAffected = sqlCreateTable.ExecuteNonQuery();
            }
            return rowsAffected;
        }
        public static void AddIdCol(DataTable dt)
        {
            DataColumn dc = new DataColumn("Id", typeof(int));
            dt.Columns.Add(dc);
            dc.SetOrdinal(0);
        }
        public static DataTable GetTable(string query, string sqlConString)
        {
            DataTable dt = new DataTable();
            using (SqlConnection connection = new SqlConnection(sqlConString))
            {
                SqlCommand cmd = new SqlCommand(query, connection);
                cmd.CommandTimeout = 0; // 60 * 30;  //timeout after 10 minutes (previously 2 minutes)
                cmd.CommandType = CommandType.Text; //interpret SqlCommand as text

                SqlDataAdapter da = new SqlDataAdapter();
                da.SelectCommand = cmd;
                //try
                //{
                da.Fill(dt);
                //}
                //catch (Exception) 
                //{ 
                //    return null; 
                //}

            }

            return dt;
        }
        public static void BulkInsertDataTable(string sqlConString, string sqlTableName, DataTable dataTable, int secondsToTimeout = 0)
        {
            using (SqlConnection connection = new SqlConnection(sqlConString))
            {
                connection.Open();

                SqlBulkCopy bulkCopy = new SqlBulkCopy(connection
                    , SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction, null);
                bulkCopy.BatchSize = 10000;
                bulkCopy.DestinationTableName = sqlTableName;
                bulkCopy.BulkCopyTimeout = secondsToTimeout;
                bulkCopy.WriteToServer(dataTable);
            }
        }
    }
}