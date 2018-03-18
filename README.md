# PeregrineDb - Lightweight CRUD Extensions for Dapper.Net
## Overview

PeregrineDb is a small and fast [Dapper.Net](https://github.com/StackExchange/dapper-dot-net) extension library to perform CRUD statement with POCO models.

[![Build status](https://ci.appveyor.com/api/projects/status/kcepamp69b45xkjj/branch/master?svg=true)](https://ci.appveyor.com/project/berkeleybross/peregrinedb/branch/master)

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
PeregrineDb provides the following extensions to the IDbConnection (with Async equivalents):

- [Count](documentation/Count.md): Counts how many entities in the table match the conditions.
- [Find](documentation/Get.md): Find an entity by it's id, or null.
- [Get](documentation/Get.md): Get an entity by it's id, or throw an exception.
- [GetRange](documentation/Get.md#GetRange): Gets all the entities in the table which match the conditions.
- [GetPage](documentation/Get.md#GetPage): Gets a page of entities which match the conditions.
- [GetAll](documentation/Get.md#GetAll): Gets all the entities in the table.
- [Insert](documentation/Insert.md): Inserts an entity into the table, with the ability to return the generated identity.
- [InsertRange](documentation/Insert.md#InsertRange): Efficiently inserts multiple entities.
- [Update](documentation/Update.md): Updates the entity by using it's primary key.
- [UpdateRange](documentation/Update.md): Efficiently updates multiple entities in the database.
- [Delete](documentation/Delete.md): Deletes an entity
- [DeleteRange](documentation/Delete.md#DeleteRange): Deletes the entities which match the given conditions.
- [DeleteAll](documentation/Delete.md#DeleteRange): Deletes all entities in the table.

#### Configuration
*Documentation in progress!*

Don't forget, for practical examples of usage, you can also browse our extensive [unit tests suite](tests/PeregrineDb.Tests).

## Comparison
*NB: These may not be correct nor up-to-date. I made this comparison very quickly*

| Library | Operations | Composite Keys | Async | .Net Core | Notes |
|---|---|---|---|---|---|
| [PeregrineDb](https://github.com/berkeleybross/PeregrineDb) | Count<br>Find/Get<br>Get(Range/All)<br>GetPage<br>Insert(Range)<br>Update(Range)<br>Delete(Range/All) | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | <ul><li>Can work across multiple DBMS in same project</li><li>Throws exceptions for inconsistencies (E.g. Update not affected anything)</li></ul> |
| [Dapper.Contrib](https://github.com/StackExchange/dapper-dot-net/tree/master/Dapper.Contrib) | Get<br>GetAll<br>Insert<br>Update<br>Delete(All)<br>| :heavy_check_mark: | :heavy_check_mark: | | <ul><li>Can use interfaces to track changes</li></ul> |
| [Dapper.Extensions](https://github.com/tmsmith/Dapper-Extensions) | Get<br>Insert<br>Update<br>Delete<br>GetList<br>GetPage/GetSet<br>Count | :heavy_check_mark: | | | <ul><li>Can use simple lambdas and predicates</li><li>Generates GUID keys</li><li>Can be configured without attributes</li></ul> |
| [Dapper.FastCRUD](https://github.com/MoonStorm/Dapper.FastCRUD/tree/master/Dapper.FastCrud.Tests) | Insert<br>Get<br>Find(Equivalent to GetRange)<br>(Bulk)Update<br>(Bulk)Delete<br>Count | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | <ul><li>Has a nice fluent syntax for complex WHERE operations</li><li>Can be configured without attributes</li></ul> |
| [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) | Get<br>GetList<br>GetListPaged<br>Insert<br>Update<br>Delete(List)<br>RecordCount | | :heavy_check_mark: | | <ul><li>Can create WHERE clauses from objects</li><li>Generates GUID keys</li></ul> |

## Installation
Simply add the nuget package [PeregrineDb](https://www.nuget.org/packages/PeregrineDb/) to your project.

## Licensing and Attribution
See [License](LICENSE).

This project was written after using [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) by [Eric Coffman](https://github.com/ericdc1), and borrows it's excellent method conventions and API heavily. The code base is however completely rewritten.