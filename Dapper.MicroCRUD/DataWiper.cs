namespace Dapper.MicroCRUD
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Schema.Relations;

    public class DataWiper
    {
        public IEnumerable<string> IgnoredTables { get; set; }

        public void ClearAllData(IDapperConnection connection, int? commandTimeout = null)
        {
            if (!(connection.Dialect is ISchemaQueryDialect dialect))
            {
                throw new ArgumentException($"The dialect '{connection.Dialect.Name}' does not support querying the schema and can therefore not be used");
            }

            var ignoredTables = new HashSet<string>(this.IgnoredTables ?? Enumerable.Empty<string>());

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