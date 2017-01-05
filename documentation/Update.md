# Updating entities

```csharp
public static void Update<TEntity>(this IDbConnection connection, TEntity entity, IDbTransaction transaction = null, Dialect dialect = null, int? commandTimeout = null)
```

Updates the entity by using it's primary key.

```csharp
public static SqlCommandResult UpdateRange<TEntity>(this IDbConnection connection, IEnumerable<TEntity> entities, IDbTransaction transaction = null, Dialect dialect = null, int? commandTimeout = null)
```

Efficiently updates multiple entities in the database.

:memo: for performance, it is recommended to wrap all bulk actions in a transaction.

### Examples
```csharp
[Table("Users")]
public class UserEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
}
```

##### Update a single entity
```csharp
var entity = this.connection.Find<UserEntity>(5);
entity.Name = "Little bobby tables";
this.connection.Update(entity);
```


<details>
<summary>MS-SQL 2012 +</summary>
```SQL
UPDATE [Users]
SET [Name] = @Name, [Age] = @Age
WHERE [Id] = @Id
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
UPDATE Users
SET Name = @Name, Age = @Age
WHERE Id = @Id
```
</details>

##### Update multiple entities

```csharp
using (var transaction = this.connection.BeginTransaction())
{
    var entities = this.connection.GetRange<UserEntity>("WHERE @Age = 10");

    foreach (var entity in entities)
    {
        entity.Name = "Little bobby tables";
    }

    this.connection.UpdateRange(entities);

    transaction.Commit();
}
```

<details>
<summary>MS-SQL 2012 +</summary>
```SQL
UPDATE [Users]
SET [Name] = @Name, [Age] = @Age
WHERE [Id] = @Id
```
</details>
<details>
<summary>PostgreSQL</summary>
```SQL
UPDATE Users
SET Name = @Name, Age = @Age
WHERE Id = @Id
```
</details>