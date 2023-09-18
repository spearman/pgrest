package client

import (
  "bytes"
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
  if err != nil {
    log.Println("error sending request:", err)
    return nil, err
  }
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
  if err != nil {
    log.Println("error sending request:", err)
    return nil, err
  }
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

func (client *Client) Df() ([]pgrest.Function, error) {
  resp, err := client.client.Get(client.url + "/df")
  if err != nil {
    log.Println("error sending request:", err)
    return nil, err
  }
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var functions []pgrest.Function
  err = json.Unmarshal(body, &functions)
  if err!= nil {
    log.Println("error converting json to functions:", err)
    return nil, err
  }
  return functions, err
}

func (client *Client) D(table_name string) ([]pgrest.Column, error) {
  req_table := pgrest.ReqTable { TableName: table_name }
  body_json, err := json.Marshal(req_table)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  req, err := http.NewRequest("GET", client.url + "/d", req_body)
  if err != nil {
    log.Println("error creating request:", err)
    return nil, err
  }
  resp, err := client.client.Do(req)
  if err != nil {
    log.Println("error sending request:", err)
    return nil, err
  }
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var columns []pgrest.Column
  err = json.Unmarshal(body, &columns)
  if err!= nil {
    log.Println("error converting json to columns:", err)
    return nil, err
  }
  return columns, err
}

func (client *Client) Idx(table_name string) ([]pgrest.Index, error) {
  req_table := pgrest.ReqTable { TableName: table_name }
  body_json, err := json.Marshal(req_table)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  req, err := http.NewRequest("GET", client.url + "/idx", req_body)
  if err != nil {
    log.Println("error creating request:", err)
    return nil, err
  }
  resp, err := client.client.Do(req)
  if err != nil {
    log.Println("error sending request:", err)
    return nil, err
  }
  log.Printf("resp: %+v\n", resp)
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    log.Println("error reading response:", err)
    return nil, err
  }
  var indexes []pgrest.Index
  err = json.Unmarshal(body, &indexes)
  if err!= nil {
    log.Println("error converting json to indexes:", err)
    return nil, err
  }
  return indexes, err
}
