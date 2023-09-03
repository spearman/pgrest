package pgserverLib

import (
  //"log"
  "github.com/jackc/pgx/v5/pgtype"
)

type Table struct {
  Schemaname, Tablename, Tableowner,  Tablespace  pgtype.Text
  Hasindexes, Hasrules,  Hastriggers, Rowsecurity bool
}
