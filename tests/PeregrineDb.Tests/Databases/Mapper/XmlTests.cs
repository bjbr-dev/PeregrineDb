namespace PeregrineDb.Tests.Databases.Mapper
{
    using System.Xml;
    using System.Xml.Linq;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class XmlTests
    {
        [Fact]
        public void CommonXmlTypesSupported()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var xml = new XmlDocument();
                xml.LoadXml("<abc/>");

                var foo = new Foo
                    {
                        A = xml,
                        B = XDocument.Parse("<def/>"),
                        C = XElement.Parse("<ghi/>")
                    };
                var bar = database.QuerySingle<Foo>($"select {foo.A} as [A], {foo.B} as [B], {foo.C} as [C]");
                Assert.Equal("abc", bar.A.DocumentElement.Name);
                Assert.Equal("def", bar.B.Root.Name.LocalName);
                Assert.Equal("ghi", bar.C.Name.LocalName);
            }
        }

        public class Foo
        {
            public XmlDocument A { get; set; }
            public XDocument B { get; set; }
            public XElement C { get; set; }
        }
    }
}
