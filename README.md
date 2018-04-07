# PeregrineDb - Lightweight CRUD Extensions for Dapper.Net

<a href="https://www.rspb.org.uk/birds-and-wildlife/wildlife-guides/bird-a-z/peregrine" target="_blank">
<img src="https://github.com//berkeleybross/PeregrineDb/raw/master/Peregrine.png" alt="Image of a Peregrine, from the RSPB"/>
</a>

PeregrineDb is a small and fast [Dapper.Net](https://github.com/StackExchange/dapper-dot-net) extension library to perform CRUD statement with POCO models. 

[![Build status](https://ci.appveyor.com/api/projects/status/kcepamp69b45xkjj/branch/master?svg=true)](https://ci.appveyor.com/project/berkeleybross/peregrinedb/branch/master)

## Project aims

1. Provide fast and lightweight CRUD extensions for Dapper
2. Should be able to run against multiple different databases in the same project
3. Thread safety should be guarenteed

I was using the fantastic [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) library and found it incredibly easy to use. However, two things drove me to write this library - I have a project which uses SqlServer and Postgres databases side-by-side, and unfortunately Dapper.SimpleCRUD does not support this. Second, I found it was missing a few crucial optimizations which was doubling the time taken to easily insert many rows.

## DBMS Support
Currently, the following DBMS are supported. More will be added (e.g. SQLite) when there is demand for them:

- Microsoft SqlServer 2012 and above
- PostgreSQL

## Features
* Getting Started
    * [Installing and using](https://github.com/berkeleybross/PeregrineDb/wiki)
    * [Manual SQL methods](https://github.com/berkeleybross/PeregrineDb/wiki/Manual-SQL-Methods)
* CRUD Methods
    * [Creating](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Creating): Insert one or many entities, with or without generating a primary key.
    * [Counting](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Counting): Count how many entities match some conditions
    * [Reading an Entity](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Reading-One-Entity): Many overloads for searching for a single entity.
    * [Reading many Entities](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Reading-Many-Entities): Search for any number of entities matching arbitrary conditions. Includes pagination.
    * [Updating](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Updating): Update one or many entities, matched on primary key
    * [Deleting](https://github.com/berkeleybross/PeregrineDb/wiki/CRUD-Deleting): Delete one, many or all entities
* [Testing your code](Test-helpers)
    * DataWiper
* Contributing

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
This project can be used and distributed under the [MIT License](LICENSE).

Please read [Notice](NOTICE.md) for licenses of other projects used by or inspirations of this projects.