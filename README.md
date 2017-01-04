# Dapper.MicroCRUD - Lightweight CRUD Extensions for Dapper.Net
## Overview

Dapper.MicroCRUD is a small and fast [Dapper.Net](https://github.com/StackExchange/dapper-dot-net) extension library to perform CRUD statement with POCO models.

[![Build status](https://ci.appveyor.com/api/projects/status/1jwpeo49kmmlv9jr/branch/master?svg=true)](https://ci.appveyor.com/project/berkeleybross/dapper-microcrud/branch/master)

## Project aims

1. Provide fast and lightweight CRUD extensions for Dapper
2. Should be able to run against multiple different databases in the same project
3. Thread safety should be guarenteed

I was using the fantastic [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) library and found it incredibly easy to use. However, two things drove me to write this library - I have a project which uses SqlServer and Postgres databases side-by-side, and unfortunately Dapper.SimpleCRUD does not support this. Second, I found it was missing a few crucial optimizations which was doubling the time taken to easily insert many rows.

## Features
Currently, the following DBMS are supported. More will be added (e.g. SQLite) when there is demand for them:

- Microsoft SqlServer 2012 and above
- PostgreSQL

#### CRUD Helpers
Dapper.MicroCRUD provides the following extensions to the IDbConnection:

- [Count&lt;TEntity&gt;([conditions])](documentation/Count.md): Counts how many entities in the TEntity table match the conditions.
- [Find(id)](documentation/Find.md): Find an entity by it's id, or null.
- [Get(id)](documentation/Find.md): Get an entity by it's id, or throw an exception.
- [GetRange&lt;TEntity&gt;(conditions)](documentation/GetRange.md): Gets all the entities in the TEntity table which match the conditions.
- [GetPage&lt;TEntity&gt;(...)](documentation/GetPage.md): Gets a page of entities which match the conditions.
- [GetAll&lt;TEntity&gt;()](documentation/GetRange.md): Gets all the entities in the TEntity table.
- [Insert(entity)](documentation/Insert.md): Inserts an entity into the TEntity table.
- [Insert&lt;TPrimaryKey&gt;(entity)](documentation/Insert.md): Inserts an entity into the TEntity table and returns it's generated identity.
- [InsertRange&lt;TEntity&gt;(entities)](documentation/InsertRange.md): Efficiently inserts multiple entities.
- [InsertRange&lt;TEntity, TPrimaryKey&gt;(entities, Action)](documentation/InsertRange.md): Efficiently inserts multiple entities, and for each one calls an action allowing its identity to be recorded.
- [Update&lt;TEntity&gt;(entity)](documentation/Update.md): Updates the entity by using it's primary key.
- [UpdateRange&lt;TEntity&gt;(entities)](documentation/Update.md): Efficiently updates multiple entities in the database.
- [Delete&lt;TEntity&gt;(entity)](documentation/Delete.md): Deletes the entity by using it's primary key.
- [Delete&lt;TEntity&gt;(id)](documentation/Delete.md): Deletes the entity with the given id.
- [DeleteRange&lt;TEntity&gt;(conditions)](documentation/DeleteRange.md): Deletes the entities which match the given conditions.
- [DeleteAll&lt;TEntity&gt;(conditions)](documentation/DeleteRange.md): Deletes all entities in the TEntity table.

#### Configuration
*Documentation in progress!*

Don't forget, for practical examples of usage, you can also browse our extensive [unit tests suite](Dapper.MicroCRUD.Tests).

## Installation
*Documentation in progress!*

## Licensing and Attribution
See [License](License).

This project was written after using [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) by [Eric Coffman](https://github.com/ericdc1), and borrows it's excellent method conventions and API heavily. The code base is however completely rewritten.