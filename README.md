Dapper.MicroCRUD - Lightweight CRUD Extensions for Dapper.Net
========================================
Overview
--------

Dapper.MicroCRUD is a small and fast Dapper.Net extension library to perform CRUD statement with POCO models.

[![Build status](https://ci.appveyor.com/api/projects/status/1jwpeo49kmmlv9jr/branch/master?svg=true)](https://ci.appveyor.com/project/berkeleybross/dapper-microcrud/branch/master)

Project aims
------------

1. Provide fast and lightweight CRUD extensions for Dapper
2. Should be able to run against multiple different databases simultaneously
3. Thread safety should be guarenteed

I was using the fantastic Dapper.SimpleCRUD library and found it incredibly easy to use. However, two things drove me to write this library - I have a project which uses SqlServer and Postgres databases side-by-side, and unfortunately Dapper.SimpleCRUD does not support this. Second, I found it was missing a few crucial optimizations which was doubling the time taken to easily insert many rows.

Features
--------

Dapper.MicroCRUD provides the following extensions to the IDbConnection:

- Count<TEntity>([conditions]): Counts how many entities in the TEntity table which match the conditions.
- Find(id): Find an entity by it's id, or null.
- GetRange<TEntity>(conditions): Gets all the entities in the TEntity table which match the conditions.
- GetAll<TEntity>(): Gets all the entities in the TEntity table.
- Insert(entity): Inserts an entity into the TEntity table without retrieving it's identity.
- Insert<TPrimaryKey>(entity): Inserts an entity into the TEntity table and returns it's generated identity.
- Update<TEntity>(entity): Updates the entity by using it's primary key.
- Delete<TEntity>(entity): Deletes the entity by using it's primary key.
- Delete<TEntity>(id): Deletes the entity with the given id.
- DeleteRange<TEntity>(conditions): Deletes the entities which match the given conditions.
- DeleteAll<TEntity>(conditions): Deletes all entities in the TEntity table.