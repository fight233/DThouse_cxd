using System;
using DThouseDriver;
using System.Runtime.InteropServices;

namespace Test.UtilsTools
{
    public class StmtUtilTools
    {
        public static IntPtr StmtInit(IntPtr conn)
        {
            IntPtr stmt = DThouse.StmtInit(conn);
            if (stmt == IntPtr.Zero)
            {
                Console.WriteLine("Init stmt failed");
                UtilsTools.CloseConnection(conn);
                UtilsTools.ExitProgram();
            }
            else
            {
                Console.WriteLine("Init stmt success");
            }
            return stmt;
        }

        public static void StmtPrepare(IntPtr stmt, string sql)
        {
            int res = DThouse.StmtPrepare(stmt, sql);
            if (res == 0)
            {
                Console.WriteLine("stmt prepare success");
            }
            else
            {
                Console.WriteLine("stmt prepare failed " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetTableName(IntPtr stmt, String tableName)
        {
            int res = DThouse.StmtSetTbname(stmt, tableName);
            if (res == 0)
            {
                Console.WriteLine("set_tbname success");
            }
            else
            {
                Console.Write("set_tbname failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetTableNameTags(IntPtr stmt, String tableName, TAOS_BIND[] tags)
        {
            int res = DThouse.StmtSetTbnameTags(stmt, tableName, tags);
            if (res == 0)
            {
                Console.WriteLine("set tbname && tags success");

            }
            else
            {
                Console.Write("set tbname && tags failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void SetSubTableName(IntPtr stmt, string name)
        {
            int res = DThouse.StmtSetSubTbname(stmt, name);
            if (res == 0)
            {
                Console.WriteLine("set subtable name success");
            }
            else
            {
                Console.Write("set subtable name failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }

        }

        public static void BindParam(IntPtr stmt, TAOS_BIND[] binds)
        {
            int res = DThouse.StmtBindParam(stmt, binds);
            if (res == 0)
            {
                Console.WriteLine("bind  para success");
            }
            else
            {
                Console.Write("bind  para failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void BindSingleParamBatch(IntPtr stmt, TAOS_MULTI_BIND bind, int index)
        {
            int res = DThouse.StmtBindSingleParamBatch(stmt, ref bind, index);
            if (res == 0)
            {
                Console.WriteLine("single bind  batch success");
            }
            else
            {
                Console.Write("single bind  batch failed: " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void BindParamBatch(IntPtr stmt, TAOS_MULTI_BIND[] bind)
        {
            int res = DThouse.StmtBindParamBatch(stmt, bind);
            if (res == 0)
            {
                Console.WriteLine("bind  parameter batch success");
            }
            else
            {
                Console.WriteLine("bind  parameter batch failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static void AddBatch(IntPtr stmt)
        {
            int res = DThouse.StmtAddBatch(stmt);
            if (res == 0)
            {
                Console.WriteLine("stmt add batch success");
            }
            else
            {
                Console.Write("stmt add batch failed,reason: " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }
        public static void StmtExecute(IntPtr stmt)
        {
            int res = DThouse.StmtExecute(stmt);
            if (res == 0)
            {
                Console.WriteLine("Execute stmt success");
            }
            else
            {
                Console.Write("Execute stmt failed,reason: " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }
        public static void StmtClose(IntPtr stmt)
        {
            int res = DThouse.StmtClose(stmt);
            if (res == 0)
            {
                Console.WriteLine("close stmt success");
            }
            else
            {
                Console.WriteLine("close stmt failed, " + DThouse.StmtErrorStr(stmt));
                StmtClose(stmt);
            }
        }

        public static IntPtr StmtUseResult(IntPtr stmt)
        {
            IntPtr res = DThouse.StmtUseResult(stmt);
            if ((res == IntPtr.Zero) || (DThouse.ErrorNo(res) != 0))
            {
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + DThouse.Error(res));
                }
                Console.WriteLine("");
                StmtClose(stmt);
            }
            else
            {
                Console.WriteLine("StmtUseResult success");

            }
            return res;
        }

        public static void loadTableInfo(IntPtr conn, string[] arr)
        {
            if (DThouse.LoadTableInfo(conn, arr) == 0)
            {
                Console.WriteLine("load table info success");
            }
            else
            {
                Console.WriteLine("load table info failed");
            }
        }

    }
}