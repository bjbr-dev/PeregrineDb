namespace PeregrineDb.Testing
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using PeregrineDb.Schema;
    using PeregrineDb.Schema.Relations;

    public class DataWiper
    {
        public static List<FormattableString> ClearAllData(IDatabaseConnection connection, IEnumerable<string> ignoredTables = null, int? commandTimeout = null)
        {
            var commands = GenerateWipeDatabaseSql(connection, ignoredTables);

            foreach (var statement in commands)
            {
                connection.Execute(statement, commandTimeout);
            }

            return commands;
        }

        public static List<FormattableString> GenerateWipeDatabaseSql(IDatabaseConnection connection, IEnumerable<string> ignoredTables = null)
        {
            if (!(connection.Config.Dialect is ISchemaQueryDialect dialect))
            {
                throw new ArgumentException($"The dialect '{connection.Config.Dialect.Name}' does not support querying the schema");
            }

            var tables = connection.Query<AllTablesQueryResult>(dialect.MakeGetAllTablesStatement()).Select(t => t.Name)
                                   .Except(ignoredTables ?? Enumerable.Empty<string>())
                                   .OrderBy(t => t)
                                   .ToList();

            var relations = connection.Query<TableRelationsQueryResult>(dialect.MakeGetAllRelationsStatement());

            var schemaRelations = new SchemaRelations(tables);

            foreach (var relation in relations)
            {
                schemaRelations.AddRelationship(relation.TargetTable, relation.SourceTable, relation.SourceColumn, relation.SourceColumnIsOptional);
            }

            var commands = new List<FormattableString>();
            foreach (var command in schemaRelations.GetClearDataCommands())
            {
                switch (command)
                {
                    case ClearTableCommand c:
                    {
                        var tableSchema = new TableSchema(c.TableName, ImmutableArray<ColumnSchema>.Empty);
                        commands.Add(dialect.MakeDeleteRangeStatement(tableSchema, null));
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