CREATE SCHEMA Other;

GO

CREATE TABLE Other.SchemaOther 
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL,
)

GO

CREATE TABLE CompositeKeys
(
	Key1 INT NOT NULL,
	Key2 INT NOT NULL,
	PRIMARY KEY (Key1, Key2)
);

CREATE TABLE KeyAlias
(
	[Key] INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE KeyGuid
(
	Id uniqueidentifier NOT NULL PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE KeyInt32
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE KeyInt64
(
	Id BIGINT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE KeyString
(
	Name NVARCHAR(200) PRIMARY KEY,
	Age INT NOT NULL
);

CREATE TABLE NoAutoIdentity
(
	Id INT NOT NULL PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE NoKey
(
	Name NVARCHAR(MAX) NOT NULL,
	Age INT NOT NULL
);

CREATE TABLE PropertyEnum
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	FavoriteColor INT NULL
);

CREATE TABLE PropertyAllPossibleTypes
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Int16Property smallint NOT NULL,
	NullableInt16Property smallint NULL,
	Int32Property int NOT NULL,
	NullableInt32Property int NULL,
	Int64Property bigint NOT NULL,
	NullableInt64Property bigint NULL,
	SingleProperty real NOT NULL,
	NullableSingleProperty real NULL,
	DoubleProperty float NOT NULL,
	NullableDoubleProperty float NULL,
	DecimalProperty numeric NOT NULL,
	NullableDecimalProperty numeric NULL,
	BoolProperty bit NOT NULL,
	NullableBoolProperty bit NULL,
	StringProperty NVARCHAR(MAX) NOT NULL,
	CharProperty NVARCHAR(MAX) NOT NULL,
	NullableCharProperty NVARCHAR(MAX) NULL,
	GuidProperty uniqueidentifier NOT NULL,
	NullableGuidProperty uniqueidentifier NULL,
	DateTimeProperty datetime2 NOT NULL,
	NullableDateTimeProperty datetime2 NULL,
	DateTimeOffsetProperty DATETIMEOFFSET NOT NULL,
	NullableDateTimeOffsetProperty DATETIMEOFFSET NULL,
	ByteArrayProperty varbinary(MAX) NOT NULL
);

CREATE TABLE SimpleBenchmarkEntities
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	FirstName NVARCHAR(50) NOT NULL,
	LastName NVARCHAR(50) NOT NULL,
	DateOfBirth DATETIME2(7) NOT NULL
);

CREATE TABLE Users
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL,
	Age INT NOT NULL
);