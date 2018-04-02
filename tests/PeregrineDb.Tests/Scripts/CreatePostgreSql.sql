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

CREATE TABLE Property_All_Possible_Types
(
	Id serial NOT NULL PRIMARY KEY,
	Int16_Property smallint NOT NULL,
	Nullable_Int16_Property smallint NULL,
	Int32_Property int NOT NULL,
	Nullable_Int32_Property int NULL,
	Int64_Property bigint NOT NULL,
	Nullable_Int64_Property bigint NULL,
	Single_Property real NOT NULL,
	Nullable_Single_Property real NULL,
	Double_Property double precision NOT NULL,
	Nullable_Double_Property double precision NULL,
	Decimal_Property numeric NOT NULL,
	Nullable_Decimal_Property numeric NULL,
	Bool_Property bool NOT NULL,
	Nullable_Bool_Property bool NULL,
	String_Property text NOT NULL,
	Char_Property text NOT NULL,
	Nullable_Char_Property text NULL,
	Guid_Property uuid NOT NULL,
	Nullable_Guid_Property uuid NULL,
	Date_Time_Property timestamp NOT NULL,
	Nullable_Date_Time_Property timestamp NULL,
	Date_Time_Offset_Property timestamp with time zone NOT NULL,
	Nullable_Date_Time_Offset_Property timestamp with time zone NULL,
	Byte_Array_Property bytea NOT NULL
);

CREATE TABLE PropertyEnum
(
	Id serial NOT NULL PRIMARY KEY,
	Favorite_Color int NULL
);

CREATE TABLE PropertyNotMapped
(
	Id serial NOT NULL PRIMARY KEY,
	First_Name text NOT NULL,
	Last_Name text NOT NULL
);

CREATE TABLE Property_Nullable
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NULL
);

CREATE TABLE SimpleBenchmarkEntities
(
	Id serial NOT NULL PRIMARY KEY,
	First_Name text NOT NULL,
	Last_Name text NOT NULL,
	Date_Of_Birth timestamp NOT NULL
);

CREATE TABLE dog
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL,
	Age int NOT NULL
);

CREATE TABLE Simple_Foreign_Key
(
	Id serial NOT NULL PRIMARY KEY,
	Name text NOT NULL,
	dog_id int NOT NULL REFERENCES dog
);

CREATE TABLE Self_Reference_Foreign_Key
(
	Id serial NOT NULL PRIMARY KEY,
	Foreign_Id int NULL REFERENCES Self_Reference_Foreign_Key(Id)
);

CREATE TABLE CyclicForeignKeyA
(
	Id serial NOT NULL PRIMARY KEY,
	Foreign_Id int NULL
);

CREATE TABLE CyclicForeignKeyB
(
	Id serial NOT NULL PRIMARY KEY,
	Foreign_Id int NOT NULL REFERENCES CyclicForeignKeyA
);

CREATE TABLE CyclicForeignKeyC
(
	Id serial NOT NULL PRIMARY KEY,
	Foreign_Id int NOT NULL REFERENCES CyclicForeignKeyB
);

ALTER TABLE CyclicForeignKeyA
ADD CONSTRAINT CyclicForeignKeyA_ForeignId_fkey
	FOREIGN KEY (Foreign_Id)
	REFERENCES CyclicForeignKeyC(Id);

CREATE TABLE Other.SchemaSimpleForeignKeys 
(
	Id serial NOT NULL PRIMARY KEY,
	schema_other_id int NOT NULL REFERENCES Other.SchemaOther(Id)
);

CREATE TABLE wipe_multiple_foreign_key_target
(
	id serial NOT NULL PRIMARY KEY,
	name text NOT NULL
);

CREATE TABLE wipe_multiple_foreign_key_source
(
	id serial NOT NULL PRIMARY KEY,
	name_id int NOT NULL REFERENCES wipe_multiple_foreign_key_target,
	optional_name_id int NULL REFERENCES wipe_multiple_foreign_key_target
);