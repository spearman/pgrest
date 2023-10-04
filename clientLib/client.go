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

func (client *Client) Dc(table_name string, column_name string) (*pgrest.DataType, error) {
  req_col := pgrest.ReqColumn { TableName: table_name, ColumnName: column_name }
  body_json, err := json.Marshal(req_col)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  req, err := http.NewRequest("GET", client.url + "/dc", req_body)
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
  var data_type pgrest.DataType
  err = json.Unmarshal(body, &data_type)
  if err!= nil {
    log.Println("error converting json to data type:", err)
    return nil, err
  }
  return &data_type, err
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

func (client *Client) Create(table_name string) (*pgrest.Result, error) {
  req_table := pgrest.ReqTable { TableName: table_name }
  body_json, err := json.Marshal(req_table)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  resp, err := http.Post(client.url + "/create", "", req_body)
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
  var result pgrest.Result
  err = json.Unmarshal(body, &result)
  if err!= nil {
    log.Println("error converting json to result:", err)
    return nil, err
  }
  return &result, err
}

func (client *Client) CreateIndex(
  index_name string, table_name string, column_name string,
) (*pgrest.Result, error) {
  cre_idx := pgrest.CreateIndex {
    IndexName: index_name, TableName: table_name, ColumnName: column_name,
  }
  body_json, err := json.Marshal(cre_idx)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  resp, err := http.Post(client.url + "/createIndex", "", req_body)
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
  var result pgrest.Result
  err = json.Unmarshal(body, &result)
  if err!= nil {
    log.Println("error converting json to result:", err)
    return nil, err
  }
  return &result, err
}

func (client *Client) Insert(
  table_name string, values []pgrest.ColVal,
) (*pgrest.Result, error) {
  insert := pgrest.Insert {
    TableName: table_name, Values: values,
  }
  body_json, err := json.Marshal(insert)
  if err != nil {
    log.Println("error marshaling body:", err)
    return nil, err
  }
  req_body := bytes.NewReader(body_json)
  resp, err := http.Post(client.url + "/insert", "", req_body)
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
  var result pgrest.Result
  err = json.Unmarshal(body, &result)
  if err!= nil {
    log.Println("error converting json to result:", err)
    return nil, err
  }
  return &result, err
}
