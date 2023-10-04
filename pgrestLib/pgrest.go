package pgrest

import (
  "fmt"
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

type DataType struct {
  Data_type pgtype.Text
}

type Result struct {
  Success *string
  Error   *string
}

func (res *Result) String() string {
  var success string
  if res.Success != nil {
    success = *res.Success
  } else {
    success = "nil"
  }
  var err string
  if res.Error != nil {
    err = *res.Error
  } else {
    err = "nil"
  }
  return fmt.Sprintf("{Success:%s Error:%s}", success, err)
}


// client -> server

type ReqTable struct {
  TableName string
}

type ReqColumn struct {
  TableName  string
  ColumnName string
}

type CreateIndex struct {
  TableName  string
  IndexName  string
  ColumnName string
}

type Insert struct {
  TableName string
  Values    []ColVal
}

type ColVal struct {
  ColumnName string
  Value      string
}
