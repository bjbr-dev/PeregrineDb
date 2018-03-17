CREATE TABLE Other.SchemaOther 
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL,
);

CREATE TABLE CompositeKeys
(
	Key1 INT,
	Key2 INT,
	Name NVARCHAR(MAX) NOT NULL,
	PRIMARY KEY (Key1, Key2)
);

CREATE TABLE KeyAlias
(
	[Key] INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL
);

CREATE TABLE KeyExplicit
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
	CharProperty NCHAR(1) NOT NULL,
	NullableCharProperty NCHAR(1) NULL,
	GuidProperty uniqueidentifier NOT NULL,
	NullableGuidProperty uniqueidentifier NULL,
	DateTimeProperty datetime2(7) NOT NULL,
	NullableDateTimeProperty datetime2(7) NULL,
	DateTimeOffsetProperty DATETIMEOFFSET NOT NULL,
	NullableDateTimeOffsetProperty DATETIMEOFFSET NULL,
	ByteArrayProperty varbinary(MAX) NOT NULL
);

CREATE TABLE PropertyEnum
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	FavoriteColor INT NULL
);

CREATE TABLE PropertyNotMapped
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	FirstName NVARCHAR(MAX) NOT NULL,
	LastName NVARCHAR(MAX) NOT NULL
);

CREATE TABLE PropertyNullables
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NULL
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

CREATE TABLE SimpleForeignKeys
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	Name NVARCHAR(MAX) NOT NULL,
	UserId INT NOT NULL,
	CONSTRAINT FK_SimpleForeignKeys_Users
		FOREIGN KEY (UserId)
		REFERENCES Users(Id)
);

CREATE TABLE SelfReferenceForeignKeys
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	ForeignId INT NULL,
	CONSTRAINT FK_SelfReferenceForeignKeys_SelfReferenceForeignKeys
		FOREIGN KEY (ForeignId)
		REFERENCES SelfReferenceForeignKeys(Id)
);

CREATE TABLE CyclicForeignKeyA
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	ForeignId INT NULL
);

CREATE TABLE CyclicForeignKeyB
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	ForeignId INT NOT NULL,
	CONSTRAINT FK_CyclicForeignKeyB_CyclicForeignKeyA
		FOREIGN KEY (ForeignId)
		REFERENCES CyclicForeignKeyA(Id)
);

CREATE TABLE CyclicForeignKeyC
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	ForeignId INT NOT NULL,
	CONSTRAINT FK_CyclicForeignKeyC_CyclicForeignKeyB
		FOREIGN KEY (ForeignId)
		REFERENCES CyclicForeignKeyB(Id)
);

ALTER TABLE CyclicForeignKeyA
ADD CONSTRAINT FK_CyclicForeignKeyA_CyclicForeignKeyC
	FOREIGN KEY (ForeignId)
	REFERENCES CyclicForeignKeyC(Id);

CREATE TABLE Other.SchemaSimpleForeignKeys 
(
	Id INT NOT NULL IDENTITY PRIMARY KEY,
	SchemaOtherId INT NOT NULL,
	CONSTRAINT FK_SimpleForeignKeys_SchemaOther
		FOREIGN KEY (SchemaOtherId)
		REFERENCES Other.SchemaOther(Id)
);