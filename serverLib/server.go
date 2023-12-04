package server

import (
  "context"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
  "strings"
)

import (
  "github.com/georgysavva/scany/v2/pgxscan"
  "github.com/jackc/pgx/v5"
  "github.com/jackc/pgx/v5/pgtype"
  json "github.com/goccy/go-json"
)

import (
  pgrest "pgrest/pgrestLib"
)

type PgServer struct {
  conn  *pgx.Conn
  ctx   context.Context
}

type constraint_name struct {
  Conname pgtype.Text
}

type column_name struct {
  Column_name pgtype.Text
}

func MakeServer(connString string) (PgServer, error) {
  cfg, err := pgx.ParseConfig(connString)
  if err != nil {
    log.Println("error parsing pg connection string:", err)
    return PgServer{}, err
  }
  ctx := context.Background()
  conn, err := pgx.ConnectConfig(ctx, cfg)
  if err != nil {
    log.Println("error creating pg connection:", err)
    return PgServer{}, err
  }
  return PgServer { conn, ctx }, nil
}

func (server *PgServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  log.Printf("received: %+v\n", r)
  log.Printf("URL: %v -----------------------------------------------\n", r.URL)
  switch r.URL.Path {
    case "/dt": server.dt(w, r)
    case "/dn": server.dn(w, r)
    case "/df": server.df(w, r)
    case "/d": server.d(w, r)
    case "/dc": server.dc(w, r)
    case "/idx": server.idx(w, r)
    case "/create": server.create(w, r)
    case "/createIndex": server.createIndex(w, r)
    case "/read": server.read(w, r)
    case "/insert": server.insert(w, r)
    case "/upsert": server.upsert(w, r)
    case "/delete": server.delete(w, r)
    case "/priv": server.priv(w, r)
    case "/execSql": server.execSql(w, r)
    case "/exec": server.exec(w, r)
    case "/own": server.own(w, r)
    case "/du": server.du(w, r)
    case "/add": server.add(w, r)
    default:
      http.Error(w, "Invalid request URL", http.StatusBadRequest)
  }
}

func (server *PgServer) dt(w http.ResponseWriter, r *http.Request) {
  tables := make([]*pgrest.Table, 0)
  err := pgxscan.Select(server.ctx, server.conn, &tables,
    "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
  if check_err(w, err, "getting tables") {
    return
  }
  send_json(w, tables, "tables")
}

func (server *PgServer) dn(w http.ResponseWriter, r *http.Request) {
  schemas := make([]*pgrest.Schema, 0)
  err := pgxscan.Select(server.ctx, server.conn, &schemas,
    "SELECT * FROM information_schema.schemata")
  if check_err(w, err, "getting schemas") {
    return
  }
  send_json(w, schemas, "schemas")
}

func (server *PgServer) df(w http.ResponseWriter, r *http.Request) {
  functions := make([]*pgrest.Function, 0)
  err := pgxscan.Select(server.ctx, server.conn, &functions,
    "SELECT specific_schema, specific_name, type_udt_name " +
    "FROM information_schema.routines WHERE specific_schema = 'public'")
  if check_err(w, err, "getting functions") {
    return
  }
  send_json(w, functions, "functions")
}

func (server *PgServer) d(w http.ResponseWriter, r *http.Request) {
  var req_table pgrest.ReqTable
  if !unmarshal_body(w, r, &req_table) {
    return
  }
  columns := make([]*pgrest.Column, 0)
  var query   string
  if req_table.TableName == "all" {
    query = "SELECT column_name, data_type, collation_name, is_nullable, column_default " +
      "FROM information_schema.columns"
  } else {
    query = fmt.Sprintf(
      "SELECT column_name, data_type, collation_name, is_nullable, column_default " +
      "FROM information_schema.columns WHERE table_name = '%s'",
      req_table.TableName)
  }
  err := pgxscan.Select(server.ctx, server.conn, &columns, query)
  if check_err(w, err, "getting columns") {
    return
  }
  send_json(w, columns, "columns")
}

func (server *PgServer) dc(w http.ResponseWriter, r *http.Request) {
  var req_col pgrest.ReqColumn
  if !unmarshal_body(w, r, &req_col) {
    return
  }
  data_type := make([]*pgrest.DataType, 0)
  query := fmt.Sprintf("SELECT data_type FROM information_schema.columns " +
    "WHERE table_name = '%s' AND column_name = '%s'",
    req_col.TableName, req_col.ColumnName)
  err := pgxscan.Select(server.ctx, server.conn, &data_type, query)
  if check_err(w, err, "getting column data type") {
    return
  }
  if len(data_type) == 0 {
    log.Println("error column not found")
    http.Error(w, fmt.Sprintf("error no such column"),
      http.StatusInternalServerError)
    return
  }
  if len(data_type) > 1 {
    log.Println("error got multiple columns")
    http.Error(w, fmt.Sprintf("error matched multiple columns: %+v\n", data_type),
      http.StatusInternalServerError)
    return
  }
  send_json(w, data_type[0], "data type")
}

func (server *PgServer) idx(w http.ResponseWriter, r *http.Request) {
  var req_table pgrest.ReqTable
  if !unmarshal_body(w, r, &req_table) {
    return
  }
  indexes := make([]*pgrest.Index, 0)
  query := fmt.Sprintf("SELECT * FROM pg_indexes WHERE tablename = '%s'",
    req_table.TableName)
  err := pgxscan.Select(server.ctx, server.conn, &indexes, query)
  if check_err(w, err, "getting indexes") {
    return
  }
  send_json(w, indexes, "indexes")
}

func (server *PgServer) create(w http.ResponseWriter, r *http.Request) {
  var req_table pgrest.ReqTable
  if !unmarshal_body(w, r, &req_table) {
    return
  }
  stmt := fmt.Sprintf("CREATE TABLE \"%s\"()", req_table.TableName)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) createIndex(w http.ResponseWriter, r *http.Request) {
  var cre_idx pgrest.CreateIndex
  if !unmarshal_body(w, r, &cre_idx) {
    return
  }
  stmt := fmt.Sprintf("CREATE INDEX \"%s\" ON \"%s\" (\"%s\")", cre_idx.IndexName,
    cre_idx.TableName, cre_idx.ColumnName)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) read(w http.ResponseWriter, r *http.Request) {
  var read_cols pgrest.ReadColumns
  if !unmarshal_body(w, r, &read_cols) {
    return
  }
  var sel_cols string
  ncols := len(read_cols.ColumnNames)
  if ncols == 0 {
    sel_cols = "*"
  } else {
    quoted_cols := make([]string, ncols)
    for i, s := range read_cols.ColumnNames {
      quoted_cols[i] = fmt.Sprintf("\"%s\"", s)
    }
    sel_cols = strings.Join(quoted_cols, ", ")
  }
  query := fmt.Sprintf("SELECT %s FROM \"%s\"", sel_cols, read_cols.TableName)
  rows, err := server.conn.Query(server.ctx, query)
  if check_err(w, err, "getting rows") {
    return
  }
  defer rows.Close()
  rows_jsonl, err := rows_to_jsonl(w, rows)
  if check_err(w, err, "converting rows to json lines") {
    return
  }
  result := pgrest.Result {
    Success: rows_jsonl,
  }
  send_json(w, result, "result")
}

func (server *PgServer) insert(w http.ResponseWriter, r *http.Request) {
  var insert pgrest.Insert
  if !unmarshal_body(w, r, &insert) {
    return
  }
  var cols []string
  var vals []string
  for _, col_val := range insert.Values {
    cols = append(cols, "\"" + col_val.ColumnName + "\"")
    vals = append(vals, col_val.Value)
  }
  cols_string := strings.Join(cols, ",")
  vals_string := strings.Join(vals, ",")
  stmt := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", insert.TableName,
    cols_string, vals_string)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) upsert(w http.ResponseWriter, r *http.Request) {
  var insert pgrest.Insert
  if !unmarshal_body(w, r, &insert) {
    return
  }
  // get the primary key name
  conname := make([]*constraint_name, 0)
  query := fmt.Sprintf("SELECT conname FROM pg_constraint " +
    "WHERE conrelid = '%s'::regclass AND confrelid = 0",
    insert.TableName)
  err := pgxscan.Select(server.ctx, server.conn, &conname, query)
  if check_err(w, err, "getting primary key constraint name") {
    return
  }
  if len(conname) == 0 {
    errmsg := fmt.Sprintf("table '%s' has no primary key", insert.TableName)
    result := pgrest.Result {
      Error: &errmsg,
    }
    send_json_err(w, result, "result")
    return
  }
  pkey_conname := conname[0].Conname.String;
  keyname := make([]*column_name, 0)
  query = fmt.Sprintf(
    "SELECT column_name FROM information_schema.key_column_usage " +
    "WHERE table_name = '%s' AND constraint_name = '%s'",
    insert.TableName, pkey_conname)
  err = pgxscan.Select(server.ctx, server.conn, &keyname, query)
  if check_err(w, err, "getting primary key") {
    return
  }
  // do the upsert
  var cols []string
  var vals []string
  var updates []string
  for _, col_val := range insert.Values {
    col_string := "\"" + col_val.ColumnName + "\""
    cols = append(cols, col_string)
    vals = append(vals, col_val.Value)
    updates = append(updates, fmt.Sprintf("%s = %s", col_string, col_val.Value))
  }
  cols_string := strings.Join(cols, ",")
  vals_string := strings.Join(vals, ",")
  update_string := strings.Join(updates, ",")
  stmt := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s) " +
    "ON CONFLICT (%s) DO UPDATE SET %s",
    insert.TableName, cols_string, vals_string, keyname[0].Column_name.String,
    update_string)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) delete(w http.ResponseWriter, r *http.Request) {
  var delete pgrest.Delete
  if !unmarshal_body(w, r, &delete) {
    return
  }
  var cols []string
  for _, col := range delete.Cols {
    cols = append(cols, "\"" + col + "\"")
  }
  cols_string := strings.Join(cols, ",")
  stmt := fmt.Sprintf("ALTER TABLE \"%s\" DROP COLUMN %s", delete.TableName,
    cols_string)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) priv(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: priv")
}

func (server *PgServer) execSql(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  sql := string(body)
  if strings.HasPrefix(sql, "SELECT") {
    rows, err := server.conn.Query(server.ctx, sql)
    if check_err(w, err, "getting rows") {
      return
    }
    defer rows.Close()
    rows_jsonl, err := rows_to_jsonl(w, rows)
    if check_err(w, err, "converting rows to json lines") {
      return
    }
    result := pgrest.Result {
      Success: rows_jsonl,
    }
    send_json(w, result, "result")
  } else {
    server.exec_stmt(w, sql)
  }
}

func (server *PgServer) exec(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: exec")
}

func (server *PgServer) own(w http.ResponseWriter, r *http.Request) {
  var own pgrest.Own
  if !unmarshal_body(w, r, &own) {
    return
  }
  stmt := fmt.Sprintf("ALTER TABLE \"%s\" OWNER TO \"%s\"", own.TableName,
    own.Owner)
  server.exec_stmt(w, stmt)
}

func (server *PgServer) du(w http.ResponseWriter, r *http.Request) {
  users := make([]*pgrest.User, 0)
  err := pgxscan.Select(server.ctx, server.conn, &users,
    "SELECT usename FROM pg_user")
  if check_err(w, err, "getting users") {
    return
  }
  send_json(w, users, "users")
}

func (server *PgServer) add(w http.ResponseWriter, r *http.Request) {
  var create_user pgrest.CreateUser
  if !unmarshal_body(w, r, &create_user) {
    return
  }
  stmt := fmt.Sprintf("CREATE USER \"%s\"", create_user.UserName)
  server.exec_stmt(w, stmt)
}

// returns false on error
func (server *PgServer) exec_stmt(w http.ResponseWriter, stmt string) bool {
  tx, err := server.conn.Begin(server.ctx)
  if check_err(w, err, "beginning transaction") {
    return false
  }
  defer tx.Rollback(server.ctx)
  res, err := tx.Exec(server.ctx, stmt)
  if err != nil {
    err_string := err.Error()
    result := pgrest.Result {
      Error: &err_string,
    }
    send_json_err(w, result, "result")
    return false
  }
  err = tx.Commit(server.ctx)
  if check_err(w, err, "committing transaction") {
    return false
  }
  res_string := res.String()
  result := pgrest.Result {
    Success: &res_string,
  }
  send_json(w, result, "result")
  return true
}

// returns true if error
func check_err(w http.ResponseWriter, err error, msg string) bool {
  if err != nil {
    log.Printf("error %s: %+v\n", msg, err)
    http.Error(w, fmt.Sprintf("error %s: %+v\n", msg, err),
      http.StatusInternalServerError)
    return true
  } else {
    return false
  }
}

// returns false if failed
func unmarshal_body(w http.ResponseWriter, r *http.Request, t interface{}) bool {
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return false
  }
  defer r.Body.Close()
  err = json.Unmarshal(body, t)
  if check_err(w, err, "unmarshaling") {
    return false
  }
  return true
}

func send_json(w http.ResponseWriter, v interface{}, name string) {
  s, err := json.Marshal(v)
  if check_err(w, err, fmt.Sprintf("converting %s to json", name)) {
    return
  }
  fmt.Fprintln(w, string(s))
}

func send_json_err(w http.ResponseWriter, v interface{}, name string) {
  s, err := json.Marshal(v)
  if check_err(w, err, fmt.Sprintf("converting %s to json", name)) {
    return
  }
  http.Error(w, fmt.Sprintf("%s", string(s)), http.StatusInternalServerError)
}

func rows_to_jsonl(w http.ResponseWriter, rows pgx.Rows) (*string, error) {
  fields := rows.FieldDescriptions()
  col_names := make([]string, len(fields))
  for i, field := range fields {
    col_names[i] = string(field.Name)
  }
  //log.Println("column names:", col_names)
  var rows_jsonl string
  for rows.Next() {
    values := make([]interface{}, len(col_names))
    for i := range values {
      values[i] = new(interface{})
    }
    err := rows.Scan(values...)
    if check_err(w, err, "scanning values") {
      return nil, err
    }
    rows_jsonl += "{"
    for i, value := range values {
      if i != 0 {
        rows_jsonl += ","
      }
      var val string
      if str, ok := (*value.(*interface{})).(string); ok {
        val = fmt.Sprintf("\"%s\"", str)
      } else {
        val = fmt.Sprintf("%v", *value.(*interface{}))
      }
      if val == "<nil>" {
        val = "null"
      }
      rows_jsonl += fmt.Sprintf("\"%s\":%s", col_names[i], val)
    }
    rows_jsonl += ("}\n")
  }
  return &rows_jsonl, nil
}
