package main

import (
  "log"
  json "github.com/goccy/go-json"
)

import (
  "pgrest/clientLib"
  "pgrest/pgrestLib"
)

func show(label string, t interface{}) {
  s, err := json.MarshalIndent(t, "", "  ")
  if err != nil {
    log.Println(err)
  }
  log.Printf("%s: %s\n", label, s)
}

func main() {
  log.Println("main...")
  client := client.MakeClient("http://127.0.0.1:12345")

  log.Printf("dt -------------------------------------------------------------")
  tables, err := client.Dt()
  if err != nil {
    log.Println(err)
  }
  show("dt", tables)

  log.Printf("dn -------------------------------------------------------------")
  schemas, err := client.Dn()
  if err != nil {
    log.Println(err)
  }
  show("dn", schemas)

  log.Printf("df -------------------------------------------------------------")
  functions, err := client.Df()
  if err != nil {
    log.Println(err)
  }
  show("df", functions)

  log.Printf("d --------------------------------------------------------------")
  columns, err := client.D("document")
  if err != nil {
    log.Println(err)
  }
  show("d", columns)

  log.Printf("dc -------------------------------------------------------------")
  data_type, err := client.Dc("foo", "mycol")
  if err != nil {
    log.Println(err)
  }
  show("dc", data_type)

  log.Printf("idx ------------------------------------------------------------")
  indexes, err := client.Idx("document")
  if err != nil {
    log.Println(err)
  }
  show("idx", indexes)

  log.Printf("create ---------------------------------------------------------")
  {
    res, err := client.Create("foo")
    if err != nil {
      log.Println(err)
    }
    show("create", res)
  }

  log.Printf("createIndex ----------------------------------------------------")
  {
    res, err := client.CreateIndex("myindex", "foo", "mycol")
    if err != nil {
      log.Println(err)
    }
    show("createIndex", res)
  }

  log.Printf("read ---------------------------------------------------------")
  {
    res, err := client.Read("foo", []string{"mycol2", "mycol3"})
    if err != nil {
      log.Println(err)
    }
    log.Printf("read: %s\n", *res)
  }
  log.Printf("insert ---------------------------------------------------------")
  {
    var col_vals []pgrest.ColVal
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "mycol", Value: "99" })
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "mycol2", Value: "98" })
    res, err := client.Insert("foo", col_vals)
    if err != nil {
      log.Println(err)
    }
    show("insert", res)
  }
  log.Printf("upsert ---------------------------------------------------------")
  {
    var col_vals []pgrest.ColVal
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "foo", Value: "2.2" })
    col_vals = append(col_vals, pgrest.ColVal { ColumnName: "bar", Value: "3" })
    res, err := client.Upsert("mytable", col_vals)
    if err != nil {
      log.Println(err)
    }
    show("upsert", res)
  }

  log.Printf("delete ---------------------------------------------------------")
  {
    res, err := client.Delete("foo", []string{"mycol4"})
    if err != nil {
      log.Println(err)
    }
    show("delete", res)
  }

  log.Printf("execSql --------------------------------------------------------")
  {
    res, err := client.ExecSql("SELECT * FROM foo")
    if err != nil {
      log.Println(err)
    }
    show("execSql", res)
  }

  log.Printf("own ------------------------------------------------------------")
  {
    res, err := client.Own("foo", "nixcloud")
    if err != nil {
      log.Println(err)
    }
    show("own", res)
  }

  log.Printf("du -------------------------------------------------------------")
  users, err := client.Du()
  if err != nil {
    log.Println(err)
  }
  show("du", users)

  log.Printf("add ------------------------------------------------------------")
  {
    res, err := client.Add("user_foo")
    if err != nil {
      log.Println(err)
    }
    show("add", res)
  }

  log.Println("...main")
}
