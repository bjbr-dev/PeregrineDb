## Update&lt;TEntity&gt;(entity)
Updates the entity by using it's primary key.

## UpdateRange&lt;TEntity&gt;(entities)
Efficiently updates multiple entities in the database.

### Examples
```csharp
[Table("Users")]
public class UserEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
}
```

You can update a single entity:
```csharp
var entity = this.connection.Find<UserEntity>(5);
entity.Name = "Little bobby tables";
this.connection.Update(entity);
```

Or a range of entities:
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

:memo: for performance, it is recommended to wrap all bulk actions in a transaction.