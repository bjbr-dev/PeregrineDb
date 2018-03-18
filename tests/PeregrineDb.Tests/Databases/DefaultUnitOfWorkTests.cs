namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Data;
    using FluentAssertions;
    using Moq;
    using PeregrineDb;
    using PeregrineDb.Databases;
    using Xunit;

    public class DefaultUnitOfWorkTests
    {
        public class Dispose
        {
            [Fact]
            public void Disposes_of_database_and_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));

                // Act
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Disposes_transaction_but_not_database()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql), false);

                // Act
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose(), Times.Never);
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Rollsback_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Rollback());
            }

            [Fact]
            public void Disposes_database_and_transaction_even_if_exception_occurs_during_rollback()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                transaction.Setup(t => t.Rollback())
                           .Throws<CustomException>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));

                // Act
                Action act = () => sut.Dispose();

                // Assert
                act.ShouldThrow<CustomException>();
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Does_not_rollback_a_saved_unit_of_work()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));
                sut.SaveChanges();

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Rollback(), Times.Never);
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Does_not_rollback_twice()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));
                sut.Rollback();

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Rollback(), Times.Once);
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Disposes_only_once()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, DefaultPeregrineConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql));
                sut.Dispose();

                // Act
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose(), Times.Once);
                transaction.Verify(t => t.Dispose(), Times.Once);
            }
        }

        private class CustomException
            : Exception
        {
        }
    }
}