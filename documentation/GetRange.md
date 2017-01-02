## GetRange<TEntity>(conditions)
Gets all the entities in the table which match the conditions.

## GetAll<TEntity>(conditions)
Gets all the entities in the table.

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

You can get all the users over the age of 18:
```csharp
var users = this.connection.GetRange<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
```

or get all users:
```csharp
var users = this.connection.GetAll<UserEntity>();
```