package main

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
  "pgserver/pgserverLib"
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
      var tables []*pgserverLib.Table
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

    default:
      http.Error(w, "Invalid request URL", http.StatusBadRequest)
  }
}

func main() {
  log.Println("main...")
  server, err := MakeServer("user=nixcloud")
  if err != nil {
    log.Fatalln("error creating pg server:", err)
  }
  s := &http.Server {
    Addr: ":12345",
    Handler: &server,
  }
  log.Println("starting server...")
  log.Fatal(s.ListenAndServe())
  log.Println("...main")
}