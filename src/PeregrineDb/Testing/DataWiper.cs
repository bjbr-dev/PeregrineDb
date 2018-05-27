namespace PeregrineDb.Testing
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using PeregrineDb.Schema.Relations;

    public class DataWiper
    {
        public static List<SqlCommand> ClearAllData(ISqlConnection connection, IEnumerable<string> ignoredTables = null, int? commandTimeout = null)
        {
            var commands = GenerateWipeDatabaseSql(connection, ignoredTables);

            foreach (var statement in commands)
            {
                connection.Execute(statement.CommandText, statement.Parameters, CommandType.Text, commandTimeout);
            }

            return commands;
        }

        public static List<SqlCommand> GenerateWipeDatabaseSql(ISqlConnection connection, IEnumerable<string> ignoredTables = null)
        {
            if (!(connection.Config.Dialect is ISchemaQueryDialect dialect))
            {
                throw new ArgumentException($"The dialect '{connection.Config.Dialect.GetType().Name}' does not support querying the schema");
            }

            var allTablesStatement = dialect.MakeGetAllTablesStatement();
            var tables = connection.Query<AllTablesQueryResult>(allTablesStatement.CommandText, allTablesStatement.Parameters)
                                   .Select(t => t.Name)
                                   .Except(ignoredTables ?? Enumerable.Empty<string>())
                                   .OrderBy(t => t)
                                   .ToList();

            var allRelationsStatement = dialect.MakeGetAllRelationsStatement();
            var relations = connection.Query<TableRelationsQueryResult>(allRelationsStatement.CommandText, allRelationsStatement.Parameters);

            var schemaRelations = new SchemaRelations(tables);

            foreach (var relation in relations)
            {
                schemaRelations.AddRelationship(relation.TargetTable, relation.SourceTable, relation.SourceColumn, relation.SourceColumnIsOptional);
            }

            var commands = new List<SqlCommand>();
            foreach (var command in schemaRelations.GetClearDataCommands())
            {
                switch (command)
                {
                    case ClearTableCommand c:
                    {
                        commands.Add(dialect.MakeDeleteAllCommand(c.TableName));
                        break;
                    }

                    case NullColumnCommand c:
                    {
                        commands.Add(dialect.MakeSetColumnNullStatement(c.TableName, c.ColumnName));
                        break;
                    }

                    default:
                        throw new InvalidOperationException("Unknown sql command: " + command?.GetType());
                }
            }

            return commands;
        }

        private class AllTablesQueryResult
        {
            public string Name { get; set; }
        }

        private class TableRelationsQueryResult
        {
            public string TargetTable { get; set; }

            public string SourceTable { get; set; }

            public string SourceColumn { get; set; }

            public bool SourceColumnIsOptional { get; set; }
        }
    }
}