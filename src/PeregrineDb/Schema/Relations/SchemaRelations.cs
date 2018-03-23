namespace PeregrineDb.Schema.Relations
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    internal class SchemaRelations
    {
        private readonly IEnumerable<string> tables;
        private readonly List<TableRelation> relations;

        public SchemaRelations(IEnumerable<string> tables)
        {
            this.tables = tables;
            this.relations = new List<TableRelation>();
        }

        public void AddRelationship(string referencedTable, string referencingTable, string columnName, bool isNullable)
        {
            if (referencedTable == referencingTable)
            {
                return;
            }

            if (!this.tables.Contains(referencedTable) || !this.tables.Contains(referencingTable))
            {
                return;
            }

            this.relations.Add(new TableRelation(referencedTable, referencingTable, columnName, isNullable));
        }

        public IEnumerable<object> GetClearDataCommands()
        {
            var commands = new List<object>();
            BuildTableList(this.tables.ToList(), new RelationCollection(this.relations));
            return commands;

            void BuildTableList(ICollection<string> remainingTables, RelationCollection remainingRelationships)
            {
                var referencedTables = remainingRelationships.GetReferencedTables();

                var leafTables = remainingTables.Except(referencedTables).ToList();

                if (referencedTables.Count > 0 && leafTables.Count == 0)
                {
                    var nullableRelation = remainingRelationships.FindFirstNullableCyclicRelation();
                    if (nullableRelation == null)
                    {
                        throw new InvalidOperationException("Cylclic foreign key detected with no nullable foreign keys");
                    }

                    commands.Add(new NullColumnCommand(nullableRelation.ForeignKeyTable, nullableRelation.ColumnName));

                    BuildTableList(remainingTables.Except(leafTables).ToList(), remainingRelationships.WithoutRelation(nullableRelation));
                    return;
                }

                commands.AddRange(leafTables.Select(t => new ClearTableCommand(t)));

                if (referencedTables.Any())
                {
                    BuildTableList(remainingTables.Except(leafTables).ToList(), remainingRelationships.WithoutLeaves(leafTables));
                }
            }
        }

        private class RelationCollection
        {
            private readonly IList<TableRelation> relations;

            public RelationCollection(IList<TableRelation> relations)
            {
                this.relations = relations;
            }

            public List<string> GetReferencedTables()
            {
                return this.relations.Select(rel => rel.PrimaryKeyTable).Distinct().ToList();
            }

            public RelationCollection WithoutRelation(TableRelation relation)
            {
                var relationships = this.relations.ToList();
                relationships.Remove(relation);
                return new RelationCollection(relationships);
            }

            public RelationCollection WithoutLeaves(IReadOnlyCollection<string> leaves)
            {
                return new RelationCollection(this.relations.Where(x => !leaves.Contains(x.ForeignKeyTable)).ToList());
            }

            public TableRelation FindFirstNullableCyclicRelation()
            {
                return this.relations.FirstOrDefault(x => x.IsNullable && HasCycle(x));

                bool HasCycle(TableRelation t)
                {
                    return HasPath(t.PrimaryKeyTable, t.ForeignKeyTable);
                }

                bool HasPath(string source, string dest)
                {
                    if (source == dest)
                    {
                        return true;
                    }

                    return this.relations.Any(r => r.ForeignKeyTable == source && HasPath(r.PrimaryKeyTable, dest));
                }
            }
        }

        private class TableRelation
        {
            public TableRelation(string primaryKeyTable, string foreignKeyTable, string columnName, bool isNullable)
            {
                this.PrimaryKeyTable = primaryKeyTable;
                this.ForeignKeyTable = foreignKeyTable;
                this.ColumnName = columnName;
                this.IsNullable = isNullable;
            }

            public string PrimaryKeyTable { get; }

            public string ForeignKeyTable { get; }

            public string ColumnName { get; }

            public bool IsNullable { get; }
        }
    }
}