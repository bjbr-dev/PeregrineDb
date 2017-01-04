## Find(id)
Find an entity by it's id, or null.

## Get(id)
Get an entity by it's id, or throw an exception.

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

You can find an entity with a primary key of 12:
```csharp
var entity = this.connection.Find<User>(12);
```

Likewise you can get a entity with a primary key of 12:
```csharp
var entity = this.connection.Get<User>(12);
```