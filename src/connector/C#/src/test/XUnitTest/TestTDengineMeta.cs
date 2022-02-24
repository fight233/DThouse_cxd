using System;
using Xunit;
using DThouseDriver;

namespace DThouseDriver.Test
{
    public class TestDThouseMeta
    {
        [Fact]
        public void TestTypeNameBool()
        {
            string typeName = "BOOL";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 1;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }

        [Fact]
        public void TestTypeNameTINYINT()
        {
            string typeName = "TINYINT";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 2;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameSMALLINT()
        {
            string typeName = "SMALLINT";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 3;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameINT()
        {
            string typeName = "INT";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 4;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameBIGINT()
        {
            string typeName = "BIGINT";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 5;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUTINYINT()
        {
            string typeName = "TINYINT UNSIGNED";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 11;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUSMALLINT()
        {
            string typeName = "SMALLINT UNSIGNED";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 12;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUINT()
        {
            string typeName = "INT UNSIGNED";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 13;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUBIGINT()
        {
            string typeName = "BIGINT UNSIGNED";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 14;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }

        [Fact]
        public void TestTypeNameFLOAT()
        {
            string typeName = "FLOAT";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 6;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameDOUBLE()
        {
            string typeName = "DOUBLE";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 7;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameSTRING()
        {
            string typeName = "STRING";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 8;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameTIMESTAMP()
        {
            string typeName = "TIMESTAMP";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 9;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameNCHAR()
        {
            string typeName = "NCHAR";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();
            meta.type = 10;
            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
        [Fact]
        public void TestTypeNameUndefined()
        {
            string typeName = "undefine";
            DThouseDriver.DThouseMeta meta = new DThouseDriver.DThouseMeta();

            string metaTypeName = meta.TypeName();

            Assert.Equal(metaTypeName, typeName);

        }
    }
}
