namespace Dapper.MicroCRUD.Tests.Databases
{
    using System;
    using System.Data;
    using Dapper.MicroCRUD.Databases;
    using FluentAssertions;
    using Moq;
    using Xunit;

    public class DefaultUnitOfWorkTests
    {
        public class Dispose
        {
            [Fact]
            public void Disposes_of_database_and_transaction()
            {
                // Arrange
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);

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
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object, false);

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
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);

                // Act
                sut.Dispose();

                // Assert
                transaction.Verify(t => t.Rollback());
            }

            [Fact]
            public void Disposes_database_and_transaction_even_if_exception_occurs_during_rollback()
            {
                // Arrange
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();
                transaction.Setup(t => t.Rollback())
                           .Throws<CustomException>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);

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
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);
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
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);
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
                var database = new Mock<IDatabase>();
                var transaction = new Mock<IDbTransaction>();

                var sut = new DefaultUnitOfWork(database.Object, transaction.Object);
                sut.Dispose();

                // Act
                sut.Dispose();

                // Assert
                database.Verify(d => d.Dispose(), Times.Once);
                transaction.Verify(t => t.Dispose(), Times.Once);
            }
        }

        public class StartUnitOfWork
        {
            [Fact]
            public void Does_not_dispose_of_itself_when_an_error_occurs()
            {
                // Arrange
                var database = new Mock<IDbConnection>();

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                Action act = () => sut.StartUnitOfWork();

                // Assert
                act.ShouldThrowExactly<ArgumentNullException>();
                database.Verify(d => d.Dispose(), Times.Never);
            }

            [Fact]
            public void Returns_a_unit_of_work_with_right_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                database.Setup(d => d.BeginTransaction()).Returns(transaction.Object);

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                var result = sut.StartUnitOfWork();

                // Assert
                result.Transaction.Should().BeSameAs(transaction.Object);
            }

            [Fact]
            public void Does_not_dispose_of_database_when_unit_of_work_is_disposed()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                database.Setup(d => d.BeginTransaction()).Returns(transaction.Object);

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                using (sut.StartUnitOfWork())
                {
                }

                // Assert
                database.Verify(d => d.Dispose(), Times.Never);
            }
        }

        public class StartUnitOfWorkWithIsolationLevel
        {
            [Fact]
            public void Does_not_dispose_of_itself_when_an_error_occurs()
            {
                // Arrange
                var database = new Mock<IDbConnection>();

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                Action act = () => sut.StartUnitOfWork(IsolationLevel.ReadCommitted);

                // Assert
                act.ShouldThrowExactly<ArgumentNullException>();
                database.Verify(d => d.Dispose(), Times.Never);
            }

            [Fact]
            public void Returns_a_unit_of_work_with_right_transaction()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                database.Setup(d => d.BeginTransaction(IsolationLevel.ReadCommitted)).Returns(transaction.Object);

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                var result = sut.StartUnitOfWork(IsolationLevel.ReadCommitted);

                // Assert
                result.Transaction.Should().BeSameAs(transaction.Object);
            }

            [Fact]
            public void Does_not_dispose_of_database_when_unit_of_work_is_disposed()
            {
                // Arrange
                var database = new Mock<IDbConnection>();
                var transaction = new Mock<IDbTransaction>();
                database.Setup(d => d.BeginTransaction(IsolationLevel.ReadCommitted)).Returns(transaction.Object);

                var sut = new DefaultDatabase(database.Object, Dialect.PostgreSql);

                // Act
                using (sut.StartUnitOfWork(IsolationLevel.ReadCommitted))
                {
                }

                // Assert
                database.Verify(d => d.Dispose(), Times.Never);
            }
        }

        private class CustomException
            : Exception
        {
        }
    }
}