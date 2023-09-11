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
  log.Println("...main")
}
