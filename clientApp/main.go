package main

import (
  "log"
)

import (
  "pgrest/clientLib"
  "pgrest/pgrestLib"
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

  data_type, err := client.Dc("foo", "mycol")
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("dc: %+v\n", data_type)

  indexes, err := client.Idx("document")
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("idx: %+v\n", indexes)

  {
    res, err := client.Create("foo")
    if err != nil {
      log.Fatal(err)
    }
    log.Printf("create: %+v\n", res)
  }

  {
    res, err := client.CreateIndex("myindex", "foo", "mycol")
    if err != nil {
      log.Fatal(err)
    }
    log.Printf("create index: %+v\n", res)
  }

  {
    var col_vals []pgrest.ColVal
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "mycol", Value: "99" })
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "mycol2", Value: "98" })
    res, err := client.Insert("foo", col_vals)
    if err != nil {
      log.Fatal(err)
    }
    log.Printf("insert: %+v\n", res)
  }

  {
    res, err := client.Delete("foo", []string{"mycol3"})
    if err != nil {
      log.Fatal(err)
    }
    log.Printf("delete: %+v\n", res)
  }

  log.Println("...main")
}
