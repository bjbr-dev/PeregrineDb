# Deleting entities

```csharp
public static void Delete<TEntity>(this IDbConnection connection, TEntity entity, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
```

Deletes the given entity by using it's primary key.

Throws `AffectedRowCountException` if the delete command didn't delete anything, or deleted multiple records.

:memo: Async version is available

```csharp
public static void Delete<TEntity>(this IDbConnection connection, object id, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
```

Deletes the entity who's primary key matches the given id.

Throws `AffectedRowCountException` if the delete command didn't delete anything, or deleted multiple records.

:memo: Async version is available

### Examples
Given the POCO class:
```csharp
[Table("Users")]
public class UserEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
}
```

#### Delete an entity
```csharp
var entity = this.connection.Find<User>(5);

this.connection.Delete(entity);
// or
await this.connection.DeleteAsync(entity);
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
DELETE FROM [Users]
WHERE [Id] = @Id
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
DELETE FROM Users
WHERE Id = @Id
```
</details>

#### Delete an entity by id
```csharp
this.connection.Delete<User>(5);
// or
await this.connection.DeleteAsync<User>(5);
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
DELETE FROM [Users]
WHERE [Id] = @Id
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
DELETE FROM Users
WHERE Id = @Id
```
</details>

<a id="DeleteRange"></a>
# Deleting many entities

```csharp
public static SqlCommandResult DeleteRange<TEntity>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Deletes the entities in the table which match the given conditions.

:warning: For safety, the conditions parameter must begin with a `WHERE` clause. If you don't want a `WHERE` clause, then you must use `DeleteAll<TEntity>`

:memo: Async version is available

```csharp
public static SqlCommandResult DeleteRange<TEntity>(this IDbConnection connection, object conditions, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Deletes the entities in the table which match the given conditions. The conditions should be an anonymous object whose properties match those of TEntity. 
All properties defined on the conditions will be combined with an AND clause. If the value of a property is *null* then the SQL generated will check for `IS NULL`.

:memo: Async version is available

```csharp
public static void DeleteAll<TEntity>(this IDbConnection connection, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Deletes all entities in the table.

:memo: This performs a SQL Delete statement not a TRUNCATE statement.

:memo: Async version is available

### Examples
Given the POCO class:
```csharp
[Table("Users")]
public class UserEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
}
```

#### Delete all users whos name contain "Foo"

```csharp
this.connection.DeleteRange<UserEntity>("WHERE Name LIKE '%Foo%'");
// or
await this.connection.DeleteRangeAsync<UserEntity>("WHERE Name LIKE '%Foo%'");
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
DELETE FROM [Users]
WHERE Name LIKE '%Foo%'
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
DELETE FROM Users
WHERE Name LIKE '%Foo%'
```
</details>


#### Delete all users who are 18

```csharp
this.connection.DeleteRange<UserEntity>(new { Age = 18 });
// or
await this.connection.DeleteRangeAsync<UserEntity>(new { Age = 18 });
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
DELETE FROM [Users]
WHERE [Age] = @Age
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
DELETE FROM Users
WHERE Age = @Age
```
</details>


#### Delete all users

```csharp
this.connection.DeleteAll<UserEntity>();
// or
await this.connection.DeleteAllAsync<UserEntity>();
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
DELETE FROM [Users]
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
DELETE FROM Users
```
</details>