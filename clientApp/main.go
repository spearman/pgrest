package main

import (
  "io/ioutil"
  "log"
  "net/http"
  "pgserver/pgserverLib"
  json "github.com/goccy/go-json"
)

type Client struct {
  url    string
  client *http.Client
}

func MakeClient (url string) Client {
  client := &http.Client{}
  return Client { url, client }
}

func (client *Client) Dt() ([]pgserverLib.Table, error) {
  resp, err := client.client.Get(client.url + "/dt")
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var tables []pgserverLib.Table
  err = json.Unmarshal(body, &tables)
  if err!= nil {
    log.Println("error converting json to tables:", err)
    return nil, err
  }
  return tables, err
}

func main() {
  log.Println("main...")
  client := MakeClient("http://127.0.0.1:12345")
  tables, err := client.Dt()
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("dt: %+v\n", tables)
  log.Println("...main")
}
