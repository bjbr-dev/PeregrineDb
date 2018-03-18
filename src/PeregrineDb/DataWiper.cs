namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using PeregrineDb.Schema;
    using PeregrineDb.Schema.Relations;

    public class DataWiper
    {
        public static void ClearAllData(IDatabaseConnection connection, HashSet<string> ignoredTables = null, int? commandTimeout = null)
        {
            if (!(connection.Config.Dialect is ISchemaQueryDialect dialect))
            {
                throw new ArgumentException($"The dialect '{connection.Config.Dialect.Name}' does not support querying the schema");
            }

            ignoredTables = ignoredTables ?? new HashSet<string>();

            var tables = connection.Query<AllTablesQueryResult>(dialect.MakeGetAllTablesStatement());
            var relations = connection.Query<TableRelationsQueryResult>(dialect.MakeGetAllRelationsStatement());

            var schemaRelations = new SchemaRelations();
            foreach (var table in tables)
            {
                if (!ignoredTables.Contains(table.Name))
                {
                    schemaRelations.AddTable(table.Name);
                }
            }

            foreach (var relation in relations)
            {
                schemaRelations.AddRelationship(relation.ReferencedTable, relation.ReferencingTable, relation.ReferencingColumn, relation.RelationIsOptional);
            }

            var commands = schemaRelations.GetClearDataCommands();

            foreach (var command in commands)
            {
                switch (command)
                {
                    case ClearTableCommand c:
                    {
                        var tableSchema = new TableSchema(c.TableName, ImmutableArray<ColumnSchema>.Empty);

                        var sql = dialect.MakeDeleteRangeStatement(tableSchema, null);
                        connection.Execute(sql, commandTimeout: commandTimeout);
                        break;
                    }
                    case NullColumnCommand c:
                    {
                        var sql = dialect.MakeSetColumnNullStatement(c.TableName, c.ColumnName);
                        connection.Execute(sql, commandTimeout: commandTimeout);
                        break;
                    }
                    default:
                        throw new InvalidOperationException("Unknown sql command: " + command?.GetType());
                }
            }
        }

        private class AllTablesQueryResult
        {
            public string Name { get; set; }
        }

        private class TableRelationsQueryResult
        {
            public string ReferencedTable { get; set; }

            public string ReferencingTable { get; set; }

            public string ReferencingColumn { get; set; }

            public bool RelationIsOptional { get; set; }
        }
    }
}