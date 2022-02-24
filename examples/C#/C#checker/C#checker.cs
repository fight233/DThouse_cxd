/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

using System;
using System.Text;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Collections;

namespace DThouseDriver
{
    class DThouseTest
    {
        //connect parameters
        private string host;
        private string configDir;
        private string user;
        private string password;
        private short port = 0;

        //sql parameters
        private string dbName;
        private string tbName;
        private string precision;

        private bool isInsertData;
        private bool isQueryData;

        private long tableCount;
        private long totalRows;
        private long batchRows;
        private long beginTimestamp = 1551369600000L;

        private IntPtr conn = IntPtr.Zero;
        private long rowsInserted = 0;

        static void Main(string[] args)
        {
            DThouseTest tester = new DThouseTest();
            tester.ReadArgument(args);


            tester.InitDThouse();
            tester.ConnectDThouse();
            tester.createDatabase();
            tester.useDatabase();
            tester.checkDropTable();
            tester.createTable();
            tester.checkInsert();
            tester.checkSelect();
            tester.checkDropTable();
            tester.dropDatabase();
            tester.CloseConnection();
            tester.cleanup();


        }

        public long GetArgumentAsLong(String[] argv, String argName, int minVal, int maxVal, int defaultValue)
        {
            int argc = argv.Length;
            for (int i = 0; i < argc; ++i)
            {
                if (argName != argv[i])
                {
                    continue;
                }
                if (i < argc - 1)
                {
                    String tmp = argv[i + 1];
                    if (tmp[0] == '-')
                    {
                        Console.WriteLine("option {0:G} requires an argument", tmp);
                        ExitProgram();
                    }

                    long tmpVal = Convert.ToInt64(tmp);
                    if (tmpVal < minVal || tmpVal > maxVal)
                    {
                        Console.WriteLine("option {0:G} should in range [{1:G}, {2:G}]", argName, minVal, maxVal);
                        ExitProgram();
                    }

                    return tmpVal;
                }
            }

            return defaultValue;
        }

        public String GetArgumentAsString(String[] argv, String argName, String defaultValue)
        {
            int argc = argv.Length;
            for (int i = 0; i < argc; ++i)
            {
                if (argName != argv[i])
                {
                    continue;
                }
                if (i < argc - 1)
                {
                    String tmp = argv[i + 1];
                    if (tmp[0] == '-')
                    {
                        Console.WriteLine("option {0:G} requires an argument", tmp);
                        ExitProgram();
                    }
                    return tmp;
                }
            }

            return defaultValue;
        }

        public void PrintHelp(String[] argv)
        {
            for (int i = 0; i < argv.Length; ++i)
            {
                if ("--help" == argv[i])
                {
                    String indent = "    ";
                    Console.WriteLine("taosTest is simple example to operate DThouse use C# Language.\n");
                    Console.WriteLine("{0:G}{1:G}", indent, "-h");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "TDEngine server IP address to connect");
                    Console.WriteLine("{0:G}{1:G}", indent, "-u");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "The TDEngine user name to use when connecting to the server, default is root");
                    Console.WriteLine("{0:G}{1:G}", indent, "-p");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "The TDEngine user name to use when connecting to the server, default is taosdata");
                    Console.WriteLine("{0:G}{1:G}", indent, "-d");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Database used to create table or import data, default is db");
                    Console.WriteLine("{0:G}{1:G}", indent, "-s");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Super Tables used to create table, default is mt");
                    Console.WriteLine("{0:G}{1:G}", indent, "-t");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Table prefixs, default is t");
                    Console.WriteLine("{0:G}{1:G}", indent, "-w");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Whether to insert data");
                    Console.WriteLine("{0:G}{1:G}", indent, "-r");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Whether to query data");
                    Console.WriteLine("{0:G}{1:G}", indent, "-n");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many Tables to create, default is 10");
                    Console.WriteLine("{0:G}{1:G}", indent, "-b");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many rows per insert batch, default is 10");
                    Console.WriteLine("{0:G}{1:G}", indent, "-i");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "How many rows to insert, default is 100");
                    Console.WriteLine("{0:G}{1:G}", indent, "-c");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Configuration directory");
                    //
                    Console.WriteLine("{0:G}{1:G}", indent, "-ps");
                    Console.WriteLine("{0:G}{1:G}{2:G}", indent, indent, "Configurate db precision,default millisecond");
                    ExitProgram();
                }
            }
        }

        public void ReadArgument(String[] argv)
        {
            PrintHelp(argv);
            host = this.GetArgumentAsString(argv, "-h", "127.0.0.1");
            user = this.GetArgumentAsString(argv, "-u", "root");
            password = this.GetArgumentAsString(argv, "-p", "taosdata");
            dbName = this.GetArgumentAsString(argv, "-d", "test");
            tbName = this.GetArgumentAsString(argv, "-s", "weather");
            precision = this.GetArgumentAsString(argv, "-ps", "ms");
            isInsertData = this.GetArgumentAsLong(argv, "-w", 0, 1, 1) != 0;
            isQueryData = this.GetArgumentAsLong(argv, "-r", 0, 1, 1) != 0;
            tableCount = this.GetArgumentAsLong(argv, "-n", 1, 10000, 10);
            batchRows = this.GetArgumentAsLong(argv, "-b", 1, 1000, 500);
            totalRows = this.GetArgumentAsLong(argv, "-i", 1, 10000000, 10000);
            configDir = this.GetArgumentAsString(argv, "-c", "C:/DThouse/cfg");
        }

        public void InitDThouse()
        {
            DThouse.Options((int)DThouseInitOption.TDDB_OPTION_CONFIGDIR, this.configDir);
            DThouse.Options((int)DThouseInitOption.TDDB_OPTION_SHELL_ACTIVITY_TIMER, "60");
            Console.WriteLine("init...");
            DThouse.Init();
            Console.WriteLine("get connection starting...");
        }

        public void ConnectDThouse()
        {
            string db = "";
            this.conn = DThouse.Connect(this.host, this.user, this.password, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("connection failed: " + this.host);
                ExitProgram();
            }
            else
            {
                Console.WriteLine("[ OK ] Connection established.");
            }
        }
        public void createDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("create database if not exists ").Append(this.dbName).Append(" precision '").Append(this.precision).Append("'");
            execute(sql.ToString());
        }
        public void useDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("use ").Append(this.dbName);
            execute(sql.ToString());
        }
        public void checkSelect()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("select * from ").Append(this.dbName).Append(".").Append(this.tbName);
            ExecuteQuery(sql.ToString());
        }
        public void createTable()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("create table if not exists ").Append(this.dbName).Append(".").Append(this.tbName).Append("(ts timestamp, temperature float, humidity int)");
            execute(sql.ToString());
        }
        public void checkInsert()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("insert into ").Append(this.dbName).Append(".").Append(this.tbName).Append("(ts, temperature, humidity) values(now, 20.5, 34)");
            execute(sql.ToString());
        }
        public void checkDropTable()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("drop table if exists ").Append(this.dbName).Append(".").Append(this.tbName).Append("");
            execute(sql.ToString());
        }
        public void dropDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("drop database if exists ").Append(this.dbName);
            execute(sql.ToString());
        }
        public void execute(string sql)
        {
            DateTime dt1 = DateTime.Now;
            IntPtr res = DThouse.Query(this.conn, sql.ToString());
            DateTime dt2 = DateTime.Now;
            TimeSpan span = dt2 - dt1;

            if ((res == IntPtr.Zero) || (DThouse.ErrorNo(res) != 0))
            {
              Console.Write(sql.ToString() + " failure, ");
              if (res != IntPtr.Zero) {
                Console.Write("reason: " + DThouse.Error(res));
              }
              Console.WriteLine("");
              ExitProgram();
            }
            else
            {
                Console.WriteLine(sql.ToString() + " success");
            }
            DThouse.FreeResult(res);
        }

        public void ExecuteQuery(string sql)
        {

            DateTime dt1 = DateTime.Now;
            long queryRows = 0;
            IntPtr res = DThouse.Query(conn, sql);
            getPrecision(res);
            if ((res == IntPtr.Zero) || (DThouse.ErrorNo(res) != 0))
            {
              Console.Write(sql.ToString() + " failure, ");
              if (res != IntPtr.Zero) {
                Console.Write("reason: " + DThouse.Error(res));
              }
              Console.WriteLine("");
              ExitProgram();
            }
            DateTime dt2 = DateTime.Now;
            TimeSpan span = dt2 - dt1;
            Console.WriteLine("[OK] time cost: " + span.ToString() + "ms, execute statement ====> " + sql.ToString());
            int fieldCount = DThouse.FieldCount(res);

            List<DThouseMeta> metas = DThouse.FetchFields(res);
            for (int j = 0; j < metas.Count; j++)
            {
                DThouseMeta meta = (DThouseMeta)metas[j];
            }

            IntPtr rowdata;
            StringBuilder builder = new StringBuilder();
            while ((rowdata = DThouse.FetchRows(res)) != IntPtr.Zero)
            {
                queryRows++;
                for (int fields = 0; fields < fieldCount; ++fields)
                {
                    DThouseMeta meta = metas[fields];
                    int offset = IntPtr.Size * fields;
                    IntPtr data = Marshal.ReadIntPtr(rowdata, offset);

                    builder.Append("---");

                    if (data == IntPtr.Zero)
                    {
                        builder.Append("NULL");
                        continue;
                    }

                    switch ((DThouseDataType)meta.type)
                    {
                        case DThouseDataType.TSDB_DATA_TYPE_BOOL:
                            bool v1 = Marshal.ReadByte(data) == 0 ? false : true;
                            builder.Append(v1);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_TINYINT:
                            byte v2 = Marshal.ReadByte(data);
                            builder.Append(v2);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_SMALLINT:
                            short v3 = Marshal.ReadInt16(data);
                            builder.Append(v3);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_INT:
                            int v4 = Marshal.ReadInt32(data);
                            builder.Append(v4);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_BIGINT:
                            long v5 = Marshal.ReadInt64(data);
                            builder.Append(v5);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_FLOAT:
                            float v6 = (float)Marshal.PtrToStructure(data, typeof(float));
                            builder.Append(v6);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_DOUBLE:
                            double v7 = (double)Marshal.PtrToStructure(data, typeof(double));
                            builder.Append(v7);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_BINARY:
                            string v8 = Marshal.PtrToStringAnsi(data);
                            builder.Append(v8);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_TIMESTAMP:
                            long v9 = Marshal.ReadInt64(data);
                            builder.Append(v9);
                            break;
                        case DThouseDataType.TSDB_DATA_TYPE_NCHAR:
                            string v10 = Marshal.PtrToStringAnsi(data);
                            builder.Append(v10);
                            break;
                    }
                }
                builder.Append("---");

                if (queryRows <= 10)
                {
                    Console.WriteLine(builder.ToString());
                }
                builder.Clear();
            }

            if (DThouse.ErrorNo(res) != 0)
            {
                Console.Write("Query is not complete, Error {0:G}", DThouse.ErrorNo(res), DThouse.Error(res));
            }
            Console.WriteLine("");

            DThouse.FreeResult(res);

        }

        public void CloseConnection()
        {
            if (this.conn != IntPtr.Zero)
            {
                DThouse.Close(this.conn);
                Console.WriteLine("connection closed.");
            }
        }

        static void ExitProgram()
        {
            System.Environment.Exit(0);
        }

        public void cleanup()
        {
            Console.WriteLine("clean up...");
            System.Environment.Exit(0);
        }
        // method to get db precision
        public void getPrecision(IntPtr res)
        {
            int psc=DThouse.ResultPrecision(res);
            switch(psc)
            {
                case 0:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G}",this.dbName,"millisecond");
                    break;
                case 1:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G}",this.dbName,"microsecond");
                    break;
                case 2:
                    Console.WriteLine("db：[{0:G}]'s precision is {1:G}",this.dbName,"nanosecond");
                    break;
            }

        }
    }
}
