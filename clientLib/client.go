package client

import (
  "io/ioutil"
  "log"
  "net/http"
  pgrest "pgrest/pgrestLib"
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

func (client *Client) Dt() ([]pgrest.Table, error) {
  resp, err := client.client.Get(client.url + "/dt")
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var tables []pgrest.Table
  err = json.Unmarshal(body, &tables)
  if err!= nil {
    log.Println("error converting json to tables:", err)
    return nil, err
  }
  return tables, err
}

func (client *Client) Dn() ([]pgrest.Schema, error) {
  resp, err := client.client.Get(client.url + "/dn")
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var schemas []pgrest.Schema
  err = json.Unmarshal(body, &schemas)
  if err!= nil {
    log.Println("error converting json to schemas:", err)
    return nil, err
  }
  return schemas, err
}

/* TODO: information_schema.routines does not contain argument data types; there
* are more complicated queries involving pg_catalog
func (client *Client) Df() ([]pgrest.Function, error) {
}
*/
