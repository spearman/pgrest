package main

import (
  "log"
)

import (
  "pgrest/clientLib"
)

func main() {
  log.Println("main...")
  client := client.MakeClient("http://127.0.0.1:12345")

  tables, err := client.Dt()
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("dt: %+v\n", tables)

  schemas, err := client.Dn()
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("dn: %+v\n", schemas)

  functions, err := client.Df()
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("df: %+v\n", functions)

  columns, err := client.D("document")
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("d: %+v\n", columns)

  indexes, err := client.Idx("document")
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("idx: %+v\n", indexes)

  res, err := client.Create("foo")
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("create: %+v\n", res)

  log.Println("...main")
}
