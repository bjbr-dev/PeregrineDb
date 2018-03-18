namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DefaultDatabaseConnectionPerformanceTests
    {
        private long PerformInsert(IDialect dialect)
        {
            using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
            {
                var database = instance.Item;

                // Arrange
                var entities = Enumerable.Range(0, 30000).Select(i => new SimpleBenchmarkEntity
                    {
                        FirstName = $"First Name {i}",
                        LastName = $"Last Name {i}",
                        DateOfBirth = DateTime.Now
                    }).ToList();

                var stopWatch = Stopwatch.StartNew();

                // Act
                using (var transaction = database.StartUnitOfWork())
                {
                    foreach (var entity in entities)
                    {
                        transaction.Insert(entity);
                    }

                    transaction.SaveChanges();
                }

                // Assert
                stopWatch.Stop();
                Console.WriteLine($"Performed insert in {stopWatch.ElapsedMilliseconds}ms");

                return stopWatch.ElapsedMilliseconds;
            }
        }

        private long PerformInsertRange(IDialect dialect)
        {
            using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
            {
                // Arrange
                var database = instance.Item;
                var entities = Enumerable.Range(0, 30000).Select(i => new SimpleBenchmarkEntity
                    {
                        FirstName = $"First Name {i}",
                        LastName = $"Last Name {i}",
                        DateOfBirth = DateTime.Now
                    }).ToList();

                var stopWatch = Stopwatch.StartNew();

                // Act
                using (var transaction = database.StartUnitOfWork())
                {
                    transaction.InsertRange(entities);
                    transaction.SaveChanges();
                }

                // Assert
                stopWatch.Stop();
                Console.WriteLine($"Performed insertrange in {stopWatch.ElapsedMilliseconds}ms");

                return stopWatch.ElapsedMilliseconds;
            }
        }

        public class SqlServer2012
            : DefaultDatabaseConnectionPerformanceTests
        {
            [Fact]
            public void Takes_less_than_6_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert(Dialect.SqlServer2012);
                timeTaken.Should().BeLessThan(6000);
            }

            [Fact]
            public void Takes_less_than_5_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange(Dialect.SqlServer2012);
                timeTaken.Should().BeLessThan(5000);
            }
        }

        public class PostgreSQL
            : DefaultDatabaseConnectionPerformanceTests
        {
            [Fact]
            public void Takes_less_than_6_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert(Dialect.PostgreSql);
                timeTaken.Should().BeLessThan(6000);
            }

            [Fact]
            public void Takes_less_than_5_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange(Dialect.PostgreSql);
                timeTaken.Should().BeLessThan(5000);
            }
        }
    }
}