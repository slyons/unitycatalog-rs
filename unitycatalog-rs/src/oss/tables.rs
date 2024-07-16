use crate::{errors::UCRSError, request::RequestClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::errors::UCRSResult;

pub struct TablesClient<'a> {
    client: &'a RequestClient
}

impl <'a> TablesClient<'a> {
    pub fn new(client: &'a RequestClient) -> Self {
        Self { client }
    }

    pub async fn list(&self, catalog_name: &str, schema_name: &str, page_token:Option<String>, max_results: Option<i32>) -> UCRSResult<ListTablesResponse> {
        let mut url = self.client.base_url.clone().join("/api/2.1/unity-catalog/tables")
            .map_err(UCRSError::MalformedURL)?;
        url.query_pairs_mut()
            .append_pair("catalog_name", catalog_name)
            .append_pair("schema_name", schema_name);
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("page_token", &token);
        }
        if let Some(max_results) = max_results {
            url.query_pairs_mut().append_pair("max_results", &max_results.to_string());
        }
        self.client.get(url, None::<String>).await
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ListTablesResponse {
    tables: Vec<TableInfo>,
    next_page_token: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TableInfo {
    name: Option<String>,
    catalog_name: Option<String>,
    schema_name: Option<String>,
    table_type: Option<TableType>,
    data_source_format: Option<DataSourceFormat>,
    columns: Option<Vec<ColumnInfo>>,
    storage_location: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    table_id: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum TableType {
    MANAGED,
    EXTERNAL
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DataSourceFormat {
    DELTA,
    CSV,
    JSON,
    AVRO,
    PARQUET,
    ORC,
    TEXT
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ColumnInfo {
    name: Option<String>,
    type_text: Option<String>,
    type_json: Option<String>,
    type_name: Option<ColumnTypeName>,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
    type_interval_type: Option<String>,
    position: Option<u32>,
    comment: Option<String>,
    nullable: Option<bool>,
    partition_index: Option<i32>
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ColumnTypeName {
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DATE,
    TIMESTAMP,
    #[allow(non_camel_case_types)]
    TIMESTAMP_NTZ,
    STRING,
    BINARY,
    DECIMAL,
    INTERVAL,
    ARRAY,
    STRUCT,
    MAP,
    CHAR,
    NULL,
    #[allow(non_camel_case_types)]
    USER_DEFINED_TYPE,
    #[allow(non_camel_case_types)]
    TABLE_TYPE
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateTable {
    name: String,
    catalog_name: String,
    schema_name: String
}