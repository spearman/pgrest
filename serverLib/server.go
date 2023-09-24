package server

import (
  "context"
  "fmt"
  "io/ioutil"
  "log"
  "net/http"
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
    case "/idx": server.idx(w, r)
    case "/create": server.create(w, r)
    default:
      http.Error(w, "Invalid request URL", http.StatusBadRequest)
  }
}

func (server *PgServer) dt(w http.ResponseWriter, r *http.Request) {
  var tables []*pgrest.Table
  err := pgxscan.Select(server.ctx, server.conn, &tables,
    "SELECT * FROM pg_catalog.pg_tables where schemaname = 'public'")
  if err != nil {
    log.Println("error getting tables:", err)
    http.Error(w, fmt.Sprintf("error getting tables: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  s, err := json.Marshal(tables)
  if err != nil {
    log.Println("error converting tables to json:", err)
    http.Error(w, fmt.Sprintf("error converting tables to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  tables_string := string(s)
  fmt.Fprintln(w, tables_string)
}

func (server *PgServer) dn(w http.ResponseWriter, r *http.Request) {
  var schemas []*pgrest.Schema
  err := pgxscan.Select(server.ctx, server.conn, &schemas,
    "SELECT * FROM information_schema.schemata")
  if err != nil {
    log.Println("error getting schemas:", err)
    http.Error(w, fmt.Sprintf("error getting schemas: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  s, err := json.Marshal(schemas)
  if err != nil {
    log.Println("error converting schemas to json:", err)
    http.Error(w, fmt.Sprintf("error converting schemas to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  tables_string := string(s)
  fmt.Fprintln(w, tables_string)
}

func (server *PgServer) df(w http.ResponseWriter, r *http.Request) {
  var functions []*pgrest.Function
  err := pgxscan.Select(server.ctx, server.conn, &functions,
    "SELECT specific_schema, specific_name, type_udt_name FROM information_schema.routines WHERE specific_schema = 'public'")
  if err != nil {
    log.Println("error getting functions:", err)
    http.Error(w, fmt.Sprintf("error getting functions: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  s, err := json.Marshal(functions)
  if err != nil {
    log.Println("error converting functions to json:", err)
    http.Error(w, fmt.Sprintf("error converting functions to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  functions_string := string(s)
  fmt.Fprintln(w, functions_string)
}

// TODO: this query does not find the requested table
func (server *PgServer) d(w http.ResponseWriter, r *http.Request) {
  var columns []*pgrest.Column
  body, err := ioutil.ReadAll(r.Body)
  if err != nil {
    http.Error(w, "error reading request body", http.StatusInternalServerError)
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if err != nil {
    http.Error(w, fmt.Sprintf("error unmarshaling table req: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  query := fmt.Sprintf("SELECT column_name, data_type, collation_name, is_nullable, column_default FROM information_schema.columns WHERE table_name = '%s'",
    req_table.TableName)
  err = pgxscan.Select(server.ctx, server.conn, &columns, query)
  if err != nil {
    log.Println("error getting columns:", err)
    http.Error(w, fmt.Sprintf("error getting columns: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  cols, err := json.Marshal(columns)
  if err != nil {
    log.Println("error converting columns to json:", err)
    http.Error(w, fmt.Sprintf("error converting columns to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  columns_string := string(cols)
  fmt.Fprintln(w, columns_string)
}

func (server *PgServer) idx(w http.ResponseWriter, r *http.Request) {
  var indexes []*pgrest.Index
  body, err := ioutil.ReadAll(r.Body)
  if err != nil {
    http.Error(w, "error reading request body", http.StatusInternalServerError)
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if err != nil {
    http.Error(w, fmt.Sprintf("error unmarshaling table req: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  query := fmt.Sprintf("SELECT * FROM pg_indexes WHERE tablename = '%s'",
    req_table.TableName)
  err = pgxscan.Select(server.ctx, server.conn, &indexes, query)
  if err != nil {
    log.Println("error getting indexes:", err)
    http.Error(w, fmt.Sprintf("error getting indexes: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  idxs, err := json.Marshal(indexes)
  if err != nil {
    log.Println("error converting indexes to json:", err)
    http.Error(w, fmt.Sprintf("error converting indexes to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  indexes_string := string(idxs)
  fmt.Fprintln(w, indexes_string)
}

func (server *PgServer) create(w http.ResponseWriter, r *http.Request) {
  body, err := ioutil.ReadAll(r.Body)
  if err != nil {
    http.Error(w, "error reading request body", http.StatusInternalServerError)
    return
  }
  defer r.Body.Close()
  var req_table pgrest.ReqTable
  err = json.Unmarshal(body, &req_table)
  if err != nil {
    http.Error(w, fmt.Sprintf("error unmarshaling table req: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  tx, err := server.conn.Begin(server.ctx)
  if err != nil {
    http.Error(w, fmt.Sprintf("error beginning transaction: %+v\n", err),
      http.StatusInternalServerError)
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
    if err != nil {
      log.Println("error converting result to json:", err)
      http.Error(w, fmt.Sprintf("error converting result to json: %+v\n", err),
        http.StatusInternalServerError)
      return
    }
    result_string := string(result_json)
    http.Error(w, fmt.Sprintf("%s", result_string),
      http.StatusInternalServerError)
    return
  }
  err = tx.Commit(server.ctx)
  if err != nil {
    http.Error(w, fmt.Sprintf("error committing transaction: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  res_string := res.String()
  result := pgrest.Result {
    Success: &res_string,
  }
  result_json, err := json.Marshal(result)
  if err != nil {
    log.Println("error converting result to json:", err)
    http.Error(w, fmt.Sprintf("error converting result to json: %+v\n", err),
      http.StatusInternalServerError)
    return
  }
  result_string := string(result_json)
  fmt.Fprintln(w, result_string)
}
