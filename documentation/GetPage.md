## GetRange&lt;TEntity&gt;(conditions)
Gets a page of entities which match the conditions.

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

You can get the second page of all the users over the age of 18:
```csharp
var users = this.connection.GetPage<User>(2, 10, "WHERE Age = @Age", "Age", new { Age = 10 });
```