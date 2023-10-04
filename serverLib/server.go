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
  s, err := json.Marshal(tables)
  if check_err(w, err, "converting tables to json") {
    return
  }
  tables_string := string(s)
  fmt.Fprintln(w, tables_string)
}

func (server *PgServer) dn(w http.ResponseWriter, r *http.Request) {
  var schemas []*pgrest.Schema
  err := pgxscan.Select(server.ctx, server.conn, &schemas,
    "SELECT * FROM information_schema.schemata")
  if check_err(w, err, "getting schemas") {
    return
  }
  s, err := json.Marshal(schemas)
  if check_err(w, err, "converting schemas to json") {
    return
  }
  tables_string := string(s)
  fmt.Fprintln(w, tables_string)
}

func (server *PgServer) df(w http.ResponseWriter, r *http.Request) {
  var functions []*pgrest.Function
  err := pgxscan.Select(server.ctx, server.conn, &functions,
    "SELECT specific_schema, specific_name, type_udt_name FROM information_schema.routines WHERE specific_schema = 'public'")
  if check_err(w, err, "getting functions") {
    return
  }
  s, err := json.Marshal(functions)
  if check_err(w, err, "converting functions to json") {
    return
  }
  functions_string := string(s)
  fmt.Fprintln(w, functions_string)
}

func (server *PgServer) d(w http.ResponseWriter, r *http.Request) {
  var columns []*pgrest.Column
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if check_err(w, err, "unmarshaling table req") {
    return
  }
  query := fmt.Sprintf("SELECT column_name, data_type, collation_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = '%s'",
    req_table.TableName)
  err = pgxscan.Select(server.ctx, server.conn, &columns, query)
  if check_err(w, err, "getting columns") {
    return
  }
  cols, err := json.Marshal(columns)
  if check_err(w, err, "converting columns to json") {
    return
  }
  columns_string := string(cols)
  fmt.Fprintln(w, columns_string)
}

func (server *PgServer) dc(w http.ResponseWriter, r *http.Request) {
  var data_type []*pgrest.DataType
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var req_col pgrest.ReqColumn
  err = json.Unmarshal(body, &req_col)
  if check_err(w, err, "unmarshaling table req") {
    return
  }
  query := fmt.Sprintf("SELECT data_type FROM information_schema.columns WHERE table_name = '%s' AND column_name = '%s'",
    req_col.TableName, req_col.ColumnName)
  err = pgxscan.Select(server.ctx, server.conn, &data_type, query)
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
  dt, err := json.Marshal(data_type[0])
  if check_err(w, err, "converting column data type to json") {
    return
  }
  data_type_string := string(dt)
  fmt.Fprintln(w, data_type_string)
}

func (server *PgServer) idx(w http.ResponseWriter, r *http.Request) {
  var indexes []*pgrest.Index
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if check_err(w, err, "unmarshaling table req") {
    return
  }
  query := fmt.Sprintf("SELECT * FROM pg_indexes WHERE tablename = '%s'",
    req_table.TableName)
  err = pgxscan.Select(server.ctx, server.conn, &indexes, query)
  if check_err(w, err, "getting indexes") {
    return
  }
  idxs, err := json.Marshal(indexes)
  if check_err(w, err, "converting indexes to json") {
    return
  }
  indexes_string := string(idxs)
  fmt.Fprintln(w, indexes_string)
}

func (server *PgServer) create(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if check_err(w, err, "unmarshaling table req") {
    return
  }
  tx, err := server.conn.Begin(server.ctx)
  if check_err(w, err, "beginning transaction") {
    return
  }
  defer tx.Rollback(server.ctx)
  res, err := tx.Exec(server.ctx,
    fmt.Sprintf("CREATE TABLE \"%s\"()", req_table.TableName))
  if err != nil {
    err_string := err.Error()
    result := pgrest.Result {
      Error: &err_string,
    }
    result_json, err := json.Marshal(result)
    if check_err(w, err, "converting result to json") {
      return
    }
    result_string := string(result_json)
    http.Error(w, fmt.Sprintf("%s", result_string),
      http.StatusInternalServerError)
    return
  }
  err = tx.Commit(server.ctx)
  if check_err(w, err, "committing transaction") {
    return
  }
  res_string := res.String()
  result := pgrest.Result {
    Success: &res_string,
  }
  result_json, err := json.Marshal(result)
  if check_err(w, err, "converting result to json") {
    return
  }
  result_string := string(result_json)
  fmt.Fprintln(w, result_string)
}

func (server *PgServer) createIndex(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var cre_idx pgrest.CreateIndex
  err = json.Unmarshal(body, &cre_idx)
  if check_err(w, err, "unmarshaling create index req") {
    return
  }
  tx, err := server.conn.Begin(server.ctx)
  if check_err(w, err, "beginning transaction") {
    return
  }
  defer tx.Rollback(server.ctx)
  res, err := tx.Exec(server.ctx,
    fmt.Sprintf("CREATE INDEX \"%s\" ON \"%s\" (\"%s\")", cre_idx.IndexName,
      cre_idx.TableName, cre_idx.ColumnName))
  if err != nil {
    err_string := err.Error()
    result := pgrest.Result {
      Error: &err_string,
    }
    result_json, err := json.Marshal(result)
    if check_err(w, err, "converting result to json") {
      return
    }
    result_string := string(result_json)
    http.Error(w, fmt.Sprintf("%s", result_string),
      http.StatusInternalServerError)
    return
  }
  err = tx.Commit(server.ctx)
  if check_err(w, err, "committing transaction") {
    return
  }
  res_string := res.String()
  result := pgrest.Result {
    Success: &res_string,
  }
  result_json, err := json.Marshal(result)
  if check_err(w, err, "converting result to json") {
    return
  }
  result_string := string(result_json)
  fmt.Fprintln(w, result_string)
}

func (server *PgServer) read(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: read")
}

func (server *PgServer) insert(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(r.Body)
  if check_err(w, err, "reading request body") {
    return
  }
  defer r.Body.Close()
  var insert pgrest.Insert
  err = json.Unmarshal(body, &insert)
  if check_err(w, err, "unmarshaling insert") {
    return
  }
  tx, err := server.conn.Begin(server.ctx)
  if check_err(w, err, "beginning transaction") {
    return
  }
  defer tx.Rollback(server.ctx)
  var cols []string
  var vals []string
  for _, col_val := range insert.Values {
    cols = append(cols, "\"" + col_val.ColumnName + "\"")
    vals = append(vals, col_val.Value)
  }
  cols_string := strings.Join(cols, ",")
  vals_string := strings.Join(vals, ",")
  res, err := tx.Exec(server.ctx,
    fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", insert.TableName,
      cols_string, vals_string))
  if err != nil {
    err_string := err.Error()
    result := pgrest.Result {
      Error: &err_string,
    }
    result_json, err := json.Marshal(result)
    if check_err(w, err, "converting result to json") {
      return
    }
    result_string := string(result_json)
    http.Error(w, fmt.Sprintf("%s", result_string),
      http.StatusInternalServerError)
    return
  }
  err = tx.Commit(server.ctx)
  if check_err(w, err, "committing transaction") {
    return
  }
  res_string := res.String()
  result := pgrest.Result {
    Success: &res_string,
  }
  result_json, err := json.Marshal(result)
  if check_err(w, err, "converting result to json") {
    return
  }
  result_string := string(result_json)
  fmt.Fprintln(w, result_string)
}

func (server *PgServer) upsert(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: upsert")
}

func (server *PgServer) delete(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: delete")
}

func (server *PgServer) priv(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: priv")
}

func (server *PgServer) execSql(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: execSql")
}

func (server *PgServer) exec(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: exec")
}

func (server *PgServer) own(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: own")
}

func (server *PgServer) du(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: du")
}

func (server *PgServer) add(w http.ResponseWriter, r *http.Request) {
  log.Fatalln("TODO: add")
}

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
