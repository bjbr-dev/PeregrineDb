# Counting entities

```csharp
public static int Count<TEntity>(this IDbConnection connection, string conditions = null, object parameters = null, IDbTransaction transaction = null, Dialect dialect = null, int? commandTimeout = null)
```

Counts how many entities in the table match the conditions.

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

#### Get the total number of users

```csharp
var numUsers = this.connection.Count<User>();
// or
numUsers = await this.connection.CountAsync<User>();
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT COUNT(*)
FROM [Users]
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT COUNT(*)
FROM Users
```
</details>


#### Get the number of users over 18 years old

```csharp
var numUsers = this.connection.Count<User>("WHERE Age > @MinAge", new { MinAge = 18 });
// or
numUsers = await this.connection.CountAsync<User>("WHERE Age > @MinAge", new { MinAge = 18 });
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
SELECT COUNT(*)
FROM [Users]
WHERE Age > @MinAge
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
SELECT COUNT(*)
FROM Users
WHERE Age > @MinAge
```
</details>