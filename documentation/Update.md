## Update<TEntity>(entity)
Updates the entity by using it's primary key.

### Examples
```csharp
[Table("Users")]
public class UserEntity
{
    public int Id { get; set; }
    public string Name { get; set; }
}
```

You can insert a range of entities, and record their generated id back in the model:
```csharp
var entity = this.connection.Find<UserEntity>(5);
entity.Name = "Little bobby tables";
this.connection.Update(entity);
```