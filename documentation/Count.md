# Count
Counts how many entities in the table match the conditions.

```csharp
public static int Count<TEntity>(this IDbConnection connection, string conditions = null, object parameters = null, IDbTransaction transaction = null, Dialect dialect = null, int? commandTimeout = null)
```
```csharp
public static Task<int> CountAsync<TEntity>(this IDbConnection connection, string conditions = null, object parameters = null, IDbTransaction transaction = null, Dialect dialect = null, int? commandTimeout = null)
```

| Name       | Description            |
|------------|------------------------|
| connection | *IDbConnection*<br>The database connection. |
| conditions | *string*<br>The SQL conditions, appended after the SQL `FROM` clause. |
| parameters | *object*<br>The parameters of any dynamic variables in the conditions. |
| transaction | *IDbTransaction*<br>The transaction to run the query in.|
| dialect | *Dialect*<br>The dialect of the DBMS. If null, will use `MicroCRUDConfig.DefaultDialect` |
| commandTimeout | *int?*<br>How many seconds to allow the query before timing out |

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

You can get the number of users over 18 years old by:

```csharp
var numUsers = this.connection.Count<User>("WHERE Age > @MinAge", new { MinAge = 18 });
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