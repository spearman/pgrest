package main

import (
  "log"
  "net/http"
)

import (
  "pgrest/serverLib"
)

func main() {
  log.Println("main...")
  server, err := server.MakeServer("user=nixcloud")
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
