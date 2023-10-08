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
  //"github.com/jackc/pgx/v5/pgtype"
  json "github.com/goccy/go-json"
)

import (
  pgrest "pgrest/pgrestLib"
)

type PgServer struct {
  conn  *pgx.Conn
  ctx   context.Context
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
  log.Printf("URL: %v\n", r.URL)
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
  var tables []*pgrest.Table
  err := pgxscan.Select(server.ctx, server.conn, &tables,
    "SELECT * FROM pg_catalog.pg_tables where schemaname = 'public'")
  if check_err(w, err, "getting tables") {
    return
  }
  send_json(w, tables, "tables")
}

func (server *PgServer) dn(w http.ResponseWriter, r *http.Request) {
  var schemas []*pgrest.Schema
  err := pgxscan.Select(server.ctx, server.conn, &schemas,
    "SELECT * FROM information_schema.schemata")
  if check_err(w, err, "getting schemas") {
    return
  }
  send_json(w, schemas, "schemas")
}

func (server *PgServer) df(w http.ResponseWriter, r *http.Request) {
  var functions []*pgrest.Function
  err := pgxscan.Select(server.ctx, server.conn, &functions,
    "SELECT specific_schema, specific_name, type_udt_name FROM information_schema.routines WHERE specific_schema = 'public'")
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
  var columns []*pgrest.Column
  query := fmt.Sprintf("SELECT column_name, data_type, collation_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = '%s'",
    req_table.TableName)
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
  var data_type []*pgrest.DataType
  query := fmt.Sprintf("SELECT data_type FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'",
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
  var indexes []*pgrest.Index
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
  log.Fatalln("TODO: read")
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
  log.Fatalln("TODO: upsert")
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
  server.exec_stmt(w, string(body))
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
  var users []*pgrest.User
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
