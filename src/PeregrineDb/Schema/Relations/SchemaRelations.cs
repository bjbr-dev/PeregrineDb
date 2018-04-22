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
            ICollection<string> remainingTables = this.tables.ToList();
            var remainingRelationships = new RelationCollection(this.relations);

            var safetyNet = 0;
            var max = this.tables.Count() * 3;
            while (safetyNet++ <= max)
            {
                var referencedTables = remainingRelationships.GetReferencedTables();
                var leafTables = remainingTables.Except(referencedTables).ToList();

                if (leafTables.Any() || !referencedTables.Any())
                {
                    commands.AddRange(leafTables.Select(t => new ClearTableCommand(t)));

                    if (!referencedTables.Any())
                    {
                        return commands;
                    }

                    remainingTables = remainingTables.Except(leafTables).ToList();
                    remainingRelationships = remainingRelationships.WithoutLeaves(leafTables);
                }
                else
                {
                    var nullableRelation = remainingRelationships.FindFirstNullableCyclicRelation();
                    if (nullableRelation == null)
                    {
                        throw new InvalidOperationException("Cylclic foreign key detected with no nullable foreign keys");
                    }

                    commands.Add(new NullColumnCommand(nullableRelation.SourceTable, nullableRelation.SourceColumn));

                    remainingRelationships = remainingRelationships.WithoutRelation(nullableRelation);
                }
            }

            throw new InvalidOperationException("Infinite loop detected whilst processing tables: " + Environment.NewLine + Environment.NewLine +
                                                string.Join(Environment.NewLine, this.tables) + Environment.NewLine + Environment.NewLine + "And relations:" +
                                                Environment.NewLine + Environment.NewLine + string.Join(Environment.NewLine, this.relations));
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
                return this.relations.Select(rel => rel.TargetTable).Distinct().ToList();
            }

            public RelationCollection WithoutRelation(TableRelation relation)
            {
                var relationships = this.relations.ToList();
                if (!relationships.Remove(relation))
                {
                    throw new InvalidOperationException("Could not remove relation: " + relation);
                }
                return new RelationCollection(relationships);
            }

            public RelationCollection WithoutLeaves(IReadOnlyCollection<string> leaves)
            {
                return new RelationCollection(this.relations.Where(x => !leaves.Contains(x.SourceTable)).ToList());
            }

            public TableRelation FindFirstNullableCyclicRelation()
            {
                return this.relations.FirstOrDefault(x => x.IsNullable && HasCycle(x));

                bool HasCycle(TableRelation t)
                {
                    return HasPath(t.TargetTable, t.SourceTable);
                }

                bool HasPath(string source, string dest)
                {
                    if (source == dest)
                    {
                        return true;
                    }

                    return this.relations.Any(r => r.SourceTable == source && HasPath(r.TargetTable, dest));
                }
            }
        }

        private class TableRelation
        {
            public TableRelation(string targetTable, string sourceTable, string sourceColumn, bool isNullable)
            {
                this.TargetTable = targetTable;
                this.SourceTable = sourceTable;
                this.SourceColumn = sourceColumn;
                this.IsNullable = isNullable;
            }

            public string TargetTable { get; }

            public string SourceTable { get; }

            public string SourceColumn { get; }

            public bool IsNullable { get; }

            public override string ToString()
            {
                var relationType = this.IsNullable ? "~" : "-";
                return $"{this.SourceTable}.{this.SourceColumn} {relationType}> {this.TargetTable}";
            }
        }
    }
}