package pgrest

import (
  //"log"
  "github.com/jackc/pgx/v5/pgtype"
)

// server -> client

type Table struct {
  Schemaname, Tablename, Tableowner,  Tablespace  pgtype.Text
  Hasindexes, Hasrules,  Hastriggers, Rowsecurity bool
}

type Schema struct {
  Catalog_name, Schema_name, Schema_owner, Default_character_set_catalog,
  Default_character_set_schema, Default_character_set_name, Sql_path pgtype.Text
}

type Function struct {
  Specific_schema, Specific_name, Type_udt_name pgtype.Text
}

type Column struct {
  Column_name, Data_type, Collation_name, Is_nullable, Column_default pgtype.Text
}

type Index struct {
  Schemaname, Tablename, Indexname, Tablespace, Indexdef pgtype.Text
}

// client -> server

type ReqTable struct {
  TableName string
}
