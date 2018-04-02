namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using Pagination;
    using PeregrineDb.Schema;

    /// <summary>
    /// Simple implementation of a SQL dialect which performs most SQL generation.
    /// </summary>
    public abstract class StandardDialect
        : IDialect, IEquatable<StandardDialect>
    {
        /// <inheritdoc />
        public SqlCommand MakeCountStatement(TableSchema schema, FormattableString conditions)
        {
            var sql = new SqlCommandBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public SqlCommand MakeFindStatement(TableSchema schema, object id)
        {
            var primaryKeys = schema.GetPrimaryKeys();

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(schema.Columns);
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            sql.AddPrimaryKeyParameter(schema, id);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetTopNStatement(TableSchema schema, int take, FormattableString conditions, string orderBy);

        /// <inheritdoc />
        public SqlCommand MakeGetRangeStatement(TableSchema tableSchema, FormattableString conditions)
        {
            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetPageStatement(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy);

        /// <inheritdoc />
        public SqlCommand MakeInsertStatement(TableSchema tableSchema, object entity)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, Include).Append(");");
            sql.AddParameters(entity);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity);

        /// <inheritdoc />
        public SqlCommand MakeUpdateStatement(TableSchema tableSchema, object entity)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new SqlCommandBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameters(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            sql.AddParameters(entity);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteEntityStatement(TableSchema tableSchema, object entity)
        {
            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            sql.AddParameters(entity);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteByPrimaryKeyStatement(TableSchema schema, object id)
        {
            var sql = new SqlCommandBuilder("DELETE FROM ").Append(schema.Name);
            var primaryKeys = schema.GetPrimaryKeys();
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            sql.AddPrimaryKeyParameter(schema, id);
            return sql.ToCommand();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeDeleteRangeStatement(TableSchema tableSchema, FormattableString conditions)
        {
            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public FormattableString MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions)
        {
            if (conditionsSchema.IsEmpty)
            {
                return new SqlString(string.Empty);
            }

            var sql = new StringBuilder("WHERE ");
            var isFirst = true;

            foreach (var condition in conditionsSchema)
            {
                if (!isFirst)
                {
                    sql.Append(" AND ");
                }

                if (condition.IsNull(conditions))
                {
                    sql.Append(condition.Column.ColumnName).Append(" IS NULL");
                }
                else
                {
                    sql.Append(condition.Column.ColumnName).Append(" = {").Append(condition.Column.Index).Append("}");
                }

                isFirst = false;
            }

            return new SqlString(sql.ToString(), GetArguments(conditionsSchema.Select(s => s.Column).ToList(), conditions));
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeCreateTempTableStatement(TableSchema tableSchema);

        /// <inheritdoc />
        public abstract SqlCommand MakeDropTempTableStatement(TableSchema tableSchema);

        public override bool Equals(object obj)
        {
            return this.Equals(obj as StandardDialect);
        }

        public bool Equals(StandardDialect other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return this.GetType() == other.GetType();
        }

        public override int GetHashCode()
        {
            return this.GetType().Name.GetHashCode();
        }

        protected static object[] GetArguments(IReadOnlyCollection<ColumnSchema> columns, object entity)
        {
            var properties = entity.GetType().GetTypeInfo().DeclaredProperties;
            var arguments = new object[columns.Max(c => c.Index) + 1];

            foreach (var prop in properties)
            {
                var propertyName = prop.Name;
                var possibleColumns = columns.Where(c => string.Equals(c.PropertyName, propertyName, StringComparison.OrdinalIgnoreCase)).ToList();
                if (possibleColumns.Count > 1)
                {
                    possibleColumns = columns.Where(c => string.Equals(c.PropertyName, propertyName, StringComparison.Ordinal)).ToList();
                    if (possibleColumns.Count > 1)
                    {
                        throw new InvalidOperationException("Ambiguous column: " + propertyName);
                    }
                }

                if (possibleColumns.Count == 1)
                {
                    arguments[possibleColumns.Single().Index] = prop.GetValue(entity);
                }
            }

            return arguments;
        }
    }
}