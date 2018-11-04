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

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);

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

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres, true);

                // Act
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose(), Times.Never);
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Disposes_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Disposes_database_even_if_exception_occurs_during_disposing_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                transaction.Setup(t => t.Dispose()).Throws<CustomException>();

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);

                // Act
                Action act = () => sut.Dispose();

                // Assert
                act.Should().Throw<CustomException>();
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Does_not_rollback_a_saved_unit_of_work()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);
                sut.SaveChanges();

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Rollback(), Times.Never);
                database.Verify(d => d.Dispose());
                transaction.Verify(t => t.Dispose());
            }

            [Fact]
            public void Does_not_dispose_twice()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);

                // Act
                sut.Dispose();
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose(), Times.Once);
                transaction.Verify(t => t.Dispose(), Times.Once);
            }

            [Fact]
            public void Disposes_only_once()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();

                var sut = DefaultUnitOfWork.From(database.Object, transaction.Object, PeregrineConfig.Postgres);
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