## DeleteRange<TEntity>(conditions)
Deletes the entities in the table which match the given conditions.

:warning: For safety, the conditions parameter must contain a "WHERE" clause. If you don't want a "WHERE" clause, then you must use `DeleteAll<TEntity>`

## DeleteAll<TEntity>(conditions)
Deletes all entities in the table.

:memo: This performs a SQL Delete statement not a TRUNCATE statement

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

You can delete all entities with a name containing 'Foo':
```csharp
this.connection.DeleteRange<UserEntity>("WHERE Name LIKE '%Foo%'");
```

or delete everything:
```csharp
this.connection.DeleteAll<UserEntity>();
```