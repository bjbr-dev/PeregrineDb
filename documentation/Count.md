## Count<TEntity>([conditions])
Counts how many entities in the table match the conditions.

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