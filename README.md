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
Dapper.MicroCRUD provides the following extensions to the IDbConnection (with Async equivalents):

- [Count](documentation/Count.md): Counts how many entities in the table match the conditions.
- [Find](documentation/Find.md): Find an entity by it's id, or null.
- [Get](documentation/Find.md): Get an entity by it's id, or throw an exception.
- [GetRange](documentation/GetRange.md): Gets all the entities in the table which match the conditions.
- [GetPage](documentation/GetPage.md): Gets a page of entities which match the conditions.
- [GetAll](documentation/GetRange.md): Gets all the entities in the table.
- [Insert](documentation/Insert.md): Inserts an entity into the table, with the ability to return the generated identity.
- [InsertRang](documentation/InsertRange.md): Efficiently inserts multiple entities.
- [Update](documentation/Update.md): Updates the entity by using it's primary key.
- [UpdateRange](documentation/Update.md): Efficiently updates multiple entities in the database.
- [Delete](documentation/Delete.md): Deletes an entity
- [DeleteRange](documentation/Delete.md#DeleteRange): Deletes the entities which match the given conditions.
- [DeleteAll](documentation/Delete.md#DeleteRange): Deletes all entities in the table.

#### Configuration
*Documentation in progress!*

Don't forget, for practical examples of usage, you can also browse our extensive [unit tests suite](Dapper.MicroCRUD.Tests).

## Installation
*Documentation in progress!*

## Licensing and Attribution
See [License](LICENSE).

This project was written after using [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) by [Eric Coffman](https://github.com/ericdc1), and borrows it's excellent method conventions and API heavily. The code base is however completely rewritten.