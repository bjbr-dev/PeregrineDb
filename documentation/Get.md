# Getting an individual entity

```csharp
public static TEntity Find<TEntity>(this IDbConnection connection, object id, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Find an entity by it's id, or null.

:memo: Async version is available

```csharp
public static TEntity Get<TEntity>(this IDbConnection connection, object id, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Get an entity by it's id, or throw an exception.

:memo: `Find` and `Get` are identical, except that `Get` throws an exception when the entity does not exist, similar to `FirstOrDefault()` vs `First()`

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

##### Find an entity with a primary key of 12:
```csharp
var entity = this.connection.Find<User>(12);
// or
var entity = await this.connection.FindAsync<User>(12);
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name]
FROM [Users]
WHERE [Id] = 12
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name
FROM Users
WHERE Id = 12
```
</details>

##### Get an entity with a primary key of 12:
```csharp
var entity = this.connection.Get<User>(12);
// or
var entity = await this.connection.GetAsync<User>(12);
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name]
FROM [Users]
WHERE [Id] = 12
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name
FROM Users
WHERE Id = 12
```
</details>

<a id="GetRange"></a>
<a id="GetAll"></a>
# Getting multiple entities

```csharp
public static IEnumerable<TEntity> GetRange<TEntity>(this IDbConnection connection, string conditions, object parameters = null, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Gets all the entities in the table which match the conditions.

:memo: Async version is available

```csharp
public static IEnumerable<TEntity> GetRange<TEntity>(this IDbConnection connection, object conditions, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Gets all the entities in the table which match all the conditions. The conditions should be an anonymous object whose properties match those of TEntity. 
All properties defined on the conditions will be combined with an AND clause. If the value of a property is *null* then the SQL generated will check for `IS NULL`.

:memo: Async version is available

```csharp
public static IEnumerable<TEntity> GetAll<TEntity>(this IDbConnection connection, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Gets all the entities in the table.

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

##### Get all the users over the age of 18
```csharp
var users = this.connection.GetRange<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE Age > @MinAge
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name, Age
FROM Users
WHERE Age > @MinAge
```
</details>

##### Get all the users without a known name who are 18 years old
```csharp
var users = this.connection.GetRange<UserEntity>(new { Name = (string)null, Age = 18 });
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE [Name] IS NULL AND [Age] = @Age
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name, Age
FROM Users
WHERE NAME IS NULL AND Age = @Age
```
</details>

##### Get all users:
```csharp
var users = this.connection.GetAll<UserEntity>();
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name], [Age]
FROM [Users]
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name, Age
FROM Users
```
</details>

<a id="GetPage"></a>
# Getting a specific page of entities

```csharp
public static IEnumerable<TEntity> GetPage<TEntity>(this IDbConnection connection, int pageNumber, int itemsPerPage, string conditions, string orderBy, object parameters = null, IDbTransaction transaction = null, IDialect dialect = null, int? commandTimeout = null)
```

Gets a page of entities which match the conditions.

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

##### Get the third page of all the users over the age of 18
```csharp
var users = this.connection.GetPage<User>(3, 10, "WHERE Age > @Age", "Age ASC", new { Age = 10 });
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE Age > @MinAge
ORDER BY Age ASC
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT Id, Name, Age
FROM Users
WHERE Age > @MinAge
ORDER BY Age ASC
LIMIT 20 OFFSET 10
```
</details>