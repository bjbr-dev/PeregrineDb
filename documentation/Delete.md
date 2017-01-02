## Delete<TEntity>(entity)
Deletes the entity by using it's primary key.

## Delete<TEntity>(id)
Deletes the entity who's primary key matches the given id.

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

You can delete an entity by:
```csharp
var entity = this.connection.Find<User>(5);
this.connection.Delete(entity);
```

or delete it directly by its id:
```csharp
this.connection.Delete<User>(5);
```

:memo: If you need to delete a lot of entities in one go, it's faster to call [DeleteRange<TEntity>](DeleteRange.md)