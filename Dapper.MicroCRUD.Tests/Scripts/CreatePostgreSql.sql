CREATE SCHEMA Other;

CREATE TABLE Other.SchemaOther 
(
	Id SERIAL NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE CompositeKeys
(
	Key1 INT NOT NULL,
	Key2 INT NOT NULL,
	PRIMARY KEY (Key1, Key2)
);

CREATE TABLE KeyAlias
(
	Key SERIAL NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE KeyInt32
(
	Id SERIAL NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE KeyInt64
(
	Id BIGSERIAL NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE KeyString
(
	Name TEXT PRIMARY KEY,
	Age INT NOT NULL
);

CREATE TABLE NoAutoIdentity
(
	Id INT NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL
);

CREATE TABLE NoKey
(
	Name TEXT NOT NULL,
	Age INT NOT NULL
);

CREATE TABLE PropertyEnum
(
	Id SERIAL NOT NULL PRIMARY KEY,
	FavoriteColor INT NULL
);

CREATE TABLE PropertyAllPossibleTypes
(
	Id SERIAL NOT NULL PRIMARY KEY,
	Int16Property smallint NOT NULL,
	NullableInt16Property smallint NULL,
	Int32Property int NOT NULL,
	NullableInt32Property int NULL,
	Int64Property bigint NOT NULL,
	NullableInt64Property bigint NULL,
	SingleProperty real NOT NULL,
	NullableSingleProperty real NULL,
	DoubleProperty double precision NOT NULL,
	NullableDoubleProperty double precision NULL,
	DecimalProperty numeric NOT NULL,
	NullableDecimalProperty numeric NULL,
	BoolProperty bool NOT NULL,
	NullableBoolProperty bool NULL,
	StringProperty TEXT NOT NULL,
	CharProperty TEXT NOT NULL,
	NullableCharProperty TEXT NULL,
	GuidProperty uuid NOT NULL,
	NullableGuidProperty uuid NULL,
	DateTimeProperty timestamp NOT NULL,
	NullableDateTimeProperty timestamp NULL,
	DateTimeOffsetProperty timestamp with time zone NOT NULL,
	NullableDateTimeOffsetProperty timestamp with time zone NULL,
	ByteArrayProperty bytea NOT NULL
);

CREATE TABLE Users
(
	Id SERIAL NOT NULL PRIMARY KEY,
	Name TEXT NOT NULL,
	Age INT NOT NULL
);