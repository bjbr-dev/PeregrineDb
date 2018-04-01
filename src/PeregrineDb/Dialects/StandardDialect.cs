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
    using PeregrineDb.SqlCommands;

    /// <summary>
    /// Simple implementation of a SQL dialect which performs most SQL generation.
    /// </summary>
    public abstract class StandardDialect
        : IDialect, IEquatable<StandardDialect>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StandardDialect"/> class.
        /// </summary>
        /// <param name="name"></param>
        protected StandardDialect(string name)
        {
            this.Name = name;
        }

        /// <summary>
        /// Gets the name of the dialect
        /// </summary>
        public string Name { get; }

        /// <inheritdoc />
        public FormattableString MakeCountStatement(TableSchema schema, FormattableString conditions)
        {
            var sql = new StringBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendClause(conditions);
            return new SqlString(sql.ToString(), conditions?.GetArguments());
        }

        /// <inheritdoc />
        public FormattableString MakeFindStatement(TableSchema schema, object id)
        {
            var primaryKeys = schema.GetPrimaryKeys();

            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(schema.Columns);
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendWherePrimaryKeysClause(primaryKeys);

            object[] arguments;
            if (primaryKeys.Length == 1)
            {
                arguments = new object[schema.Columns.Length];
                arguments[primaryKeys.Single().Index] = id;
            }
            else
            {
                arguments = GetArguments(schema.Columns, id);
            }

            return new SqlString(sql.ToString(), arguments);
        }

        /// <inheritdoc />
        public abstract FormattableString MakeGetTopNStatement(TableSchema schema, int take, FormattableString conditions, string orderBy);

        /// <inheritdoc />
        public FormattableString MakeGetRangeStatement(TableSchema tableSchema, FormattableString conditions)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return new SqlString(sql.ToString(), conditions?.GetArguments());
        }

        /// <inheritdoc />
        public abstract FormattableString MakeGetPageStatement(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy);

        /// <inheritdoc />
        public FormattableString MakeInsertStatement(TableSchema tableSchema, object entity)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new StringBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterPlaceholders(columns, Include).Append(");");

            var arguments = GetArguments(tableSchema.Columns, entity);

            return new SqlString(sql.ToString(), arguments);
        }

        /// <inheritdoc />
        public abstract FormattableString MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity);

        /// <inheritdoc />
        public FormattableString MakeUpdateStatement(TableSchema tableSchema, object entity)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new StringBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualPlaceholders(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return new SqlString(sql.ToString(), GetArguments(tableSchema.Columns, entity));
        }

        /// <inheritdoc />
        public FormattableString MakeDeleteEntityStatement(TableSchema tableSchema, object entity)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return new SqlString(sql.ToString(), GetArguments(tableSchema.Columns, entity));
        }

        /// <inheritdoc />
        public FormattableString MakeDeleteByPrimaryKeyStatement(TableSchema schema, object id)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(schema.Name);
            var primaryKeys = schema.GetPrimaryKeys();
            sql.AppendWherePrimaryKeysClause(primaryKeys);

            object[] arguments;
            if (primaryKeys.Length == 1)
            {
                arguments = new object[schema.Columns.Length];
                arguments[primaryKeys.Single().Index] = id;
            }
            else
            {
                arguments = GetArguments(schema.Columns, id);
            }

            return new SqlString(sql.ToString(), arguments);
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public FormattableString MakeDeleteRangeStatement(TableSchema tableSchema, FormattableString conditions)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return new SqlString(sql.ToString(), conditions?.GetArguments());
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
        public abstract FormattableString MakeCreateTempTableStatement(TableSchema tableSchema);

        /// <inheritdoc />
        public abstract FormattableString MakeDropTempTableStatement(TableSchema tableSchema);

        /// <inheritdoc />
        public abstract string MakeColumnName(string name);

        /// <inheritdoc />
        public abstract string MakeTableName(string tableName);

        /// <inheritdoc />
        public abstract string MakeTableName(string schema, string tableName);

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

            return string.Equals(this.Name, other.Name);
        }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }
        
        /// <inheritdoc />
        public override string ToString()
        {
            return "Dialect " + this.Name;
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