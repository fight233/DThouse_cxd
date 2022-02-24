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
    class SchemalessSample
    {
        // connect parameters 
        private string host = "127.0.0.1";
        private string configDir = "C:/DThouse/cfg";
        private string user = "root";
        private string passwd = "taosdata";
        private short port = 0;

        private IntPtr conn = IntPtr.Zero;
        private string dbName = "csharp";
        private string dbPrecision = "ms";

        static void Main(string[] args)
        {
            string[] lines = {
                "stg,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
                "stg,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833641000000"
            };
            string[] jsonStr = {
                "{"
                   +"\"metric\": \"stb0_0\","
                   +"\"timestamp\": 1626006833,"
                   +"\"value\": 10,"
                   +"\"tags\": {"
                       +" \"t1\": true,"
                       +"\"t2\": false,"
                       +"\"t3\": 10,"
                       +"\"t4\": \"123_abc_.!@#$%^&*:;,./?|+-=()[]{}<>\""
                    +"}"
                +"}"
            };

            SchemalessSample sample = new SchemalessSample();
            sample.InitDThouse();
            sample.ConnectDThouse();
            sample.dropDatabase();
            sample.createDatabase();
            sample.useDatabase();
            sample.schemalessInsert(lines, 2, (int)DThouseSchemalessProtocol.TSDB_SML_LINE_PROTOCOL, (int)DThouseSchemalessPrecision.TSDB_SML_TIMESTAMP_NANO_SECONDS);
            sample.checkSelect("stg");
            sample.schemalessInsert(jsonStr,1,(int)DThouseSchemalessProtocol.TSDB_SML_JSON_PROTOCOL,(int)DThouseSchemalessPrecision.TSDB_SML_TIMESTAMP_SECONDS);
            sample.checkSelect("stb0_0");
            sample.CloseConnection();
            sample.cleanup();
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
            this.conn = DThouse.Connect(host, this.user, this.passwd, db, this.port);
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
            sql.Append("create database if not exists ").Append(this.dbName).Append(" precision '").Append(this.dbPrecision).Append("'");
            execute(sql.ToString());
        }
        public void useDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("use ").Append(this.dbName);
            execute(sql.ToString());
        }
        public void checkSelect(String tableName)
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("select * from ").Append(this.dbName).Append(".").Append(tableName);
            ExecuteQuery(sql.ToString());
        }

        public void schemalessInsert(string[] sqlstr, int lineCnt, int protocol, int precision)
        {

            IntPtr res = DThouse.SchemalessInsert(this.conn, sqlstr, lineCnt, protocol, precision);

            if (DThouse.ErrorNo(res) != 0)
            {
                Console.WriteLine("schemaless_insert failed:{0}", DThouse.Error(res));
                Console.WriteLine("line string:{0}", sqlstr);
                Console.WriteLine("");
                ExitProgram();
            }
            else
            {
                Console.WriteLine("else");
                Console.WriteLine("schemaless insert success:{0}", DThouse.ErrorNo(res));
            }
            DisplayRes(res);
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
                if (res != IntPtr.Zero)
                {
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
        public void DisplayRes(IntPtr res)
        {
            long queryRows = 0;
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
        public void ExecuteQuery(string sql)
        {

            DateTime dt1 = DateTime.Now;

            IntPtr res = DThouse.Query(conn, sql);

            DateTime dt2 = DateTime.Now;
            TimeSpan span = dt2 - dt1;

            if ((res == IntPtr.Zero) || (DThouse.ErrorNo(res) != 0))
            {
                Console.Write(sql.ToString() + " failure, ");
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + DThouse.Error(res));
                }
                Console.WriteLine("");
                ExitProgram();
            }

            Console.WriteLine("[OK] time cost: " + span.ToString() + "ms, execute statement ====> " + sql.ToString());
            DisplayRes(res);

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
    }
}
