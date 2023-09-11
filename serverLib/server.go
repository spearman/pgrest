package server

import (
  "context"
  "fmt"
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

func MakeServer (connString string) (PgServer, error) {
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
    case "/dt":
      server.dt(w, r)
    case "/dn":
      server.dn(w, r)
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
