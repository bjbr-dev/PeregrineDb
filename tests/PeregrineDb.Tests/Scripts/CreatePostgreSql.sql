CREATE SCHEMA Other;

CREATE TABLE Other.SchemaOther 
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE CompositeKeys
(
	Key1 int,
	Key2 int,
	Name text NOT NULL,
	PRIMARY KEY (Key1, Key2)
);

CREATE TABLE KeyAlias
(
	Key serial NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE KeyExplicit
(
	Key serial NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE KeyGuid
(
	Id uuid NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE KeyInt32
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE KeyInt64
(
	Id BIGSERIAL NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE KeyString
(
	Name text PRIMARY KEY,
	Age int NOT NULL
);

CREATE TABLE NoAutoIdentity
(
	Id int NOT NULL PRIMARY KEY,
	Name text NOT NULL
);

CREATE TABLE NoKey
(
	Name text NOT NULL,
	Age int NOT NULL
);

CREATE TABLE PropertyAllPossibleTypes
(
	Id serial NOT NULL PRIMARY KEY,
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
	StringProperty text NOT NULL,
	CharProperty text NOT NULL,
	NullableCharProperty text NULL,
	GuidProperty uuid NOT NULL,
	NullableGuidProperty uuid NULL,
	DateTimeProperty timestamp NOT NULL,
	NullableDateTimeProperty timestamp NULL,
	DateTimeOffsetProperty timestamp with time zone NOT NULL,
	NullableDateTimeOffsetProperty timestamp with time zone NULL,
	ByteArrayProperty bytea NOT NULL
);

CREATE TABLE PropertyEnum
(
	Id serial NOT NULL PRIMARY KEY,
	FavoriteColor int NULL
);

CREATE TABLE PropertyNotMapped
(
	Id serial NOT NULL PRIMARY KEY,
	FirstName text NOT NULL,
	LastName text NOT NULL
);

CREATE TABLE PropertyNullables
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NULL
);

CREATE TABLE SimpleBenchmarkEntities
(
	Id serial NOT NULL PRIMARY KEY,
	FirstName text NOT NULL,
	LastName text NOT NULL,
	DateOfBirth timestamp NOT NULL
);

CREATE TABLE Users
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL,
	Age int NOT NULL
);

CREATE TABLE SimpleForeignKeys
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL,
	UserId int NOT NULL REFERENCES Users
);

CREATE TABLE SelfReferenceForeignKeys
(
	Id serial NOT NULL PRIMARY KEY,
	ForeignId int NULL REFERENCES SelfReferenceForeignKeys(Id)
);

CREATE TABLE CyclicForeignKeyA
(
	Id serial NOT NULL PRIMARY KEY,
	ForeignId int NULL
);

CREATE TABLE CyclicForeignKeyB
(
	Id serial NOT NULL PRIMARY KEY,
	ForeignId int NOT NULL REFERENCES CyclicForeignKeyA
);

CREATE TABLE CyclicForeignKeyC
(
	Id serial NOT NULL PRIMARY KEY,
	ForeignId int NOT NULL REFERENCES CyclicForeignKeyB
);

ALTER TABLE CyclicForeignKeyA
ADD CONSTRAINT CyclicForeignKeyA_ForeignId_fkey
	FOREIGN KEY (ForeignId)
	REFERENCES CyclicForeignKeyC(Id);

CREATE TABLE Other.SchemaSimpleForeignKeys 
(
	Id serial NOT NULL PRIMARY KEY,
	SchemaOtherId int NOT NULL REFERENCES Other.SchemaOther(Id)
);

CREATE TABLE wipemultipleforeignkeytargets
(
	id serial NOT NULL PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE wipemultipleforeignkeysources
(
	id serial NOT NULL PRIMARY KEY,
	nameid int NOT NULL REFERENCES wipemultipleforeignkeytargets,
	optionalnameid int NULL REFERENCES wipemultipleforeignkeytargets
);