use crate::{errors::UCRSError, request::RequestClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::errors::UCRSResult;
use derive_builder::{self, Builder};

pub struct TablesClient<'a> {
    client: &'a RequestClient
}

impl <'a> TablesClient<'a> {
    pub fn new(client: &'a RequestClient) -> Self {
        Self { client }
    }

    pub fn full_name(catalog_name: &str, schema_name: &str, name: &str) -> String {
        format!("{}.{}.{}", catalog_name, schema_name, name)
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

    pub async fn create(&self, props: CreateTable) -> UCRSResult<TableInfo> {
        let route = self.client.base_url.join("/api/2.1/unity-catalog/tables")
            .map_err(UCRSError::MalformedURL)?;

        let res = self.client.post(route, Some(&props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::CONFLICT) => Err(UCRSError::DuplicateTableName(props.name)),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn get(&self, full_name: &str) -> UCRSResult<TableInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/tables/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        let res = self.client.get(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::TableNotFound(full_name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn delete(&self, full_name: &str) -> UCRSResult<()> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/tables/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        let res = self.client.delete(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::TableNotFound(full_name.to_owned())),
                _ => res
            }
        } else if let Err(UCRSError::JSONParsingError(_)) = res {
            // This is because DELETE returns "200 OK" as a response body :/
            Ok(())
        } 
        else {
            res
        }
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TableType {
    MANAGED,
    EXTERNAL
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataSourceFormat {
    DELTA,
    CSV,
    JSON,
    AVRO,
    PARQUET,
    ORC,
    TEXT
}

#[derive(Serialize, Deserialize, Debug, Default, Builder, Clone)]
#[builder(setter(strip_option), default)]
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

impl ColumnInfo {
    pub fn builder() -> ColumnInfoBuilder {
        ColumnInfoBuilder::create_empty()
    }
}

impl ColumnInfoBuilder {
    pub fn generate_type_json(&mut self) -> &mut Self {
        //TODO: Better failure handling here, but the builder pattern makes it awkward
        let name = self.name.as_ref().unwrap().as_ref().unwrap();
        let r#type:&'static str = self.type_name.as_ref().unwrap().as_ref().unwrap().into();
        let nullable = self.nullable.unwrap().unwrap();
        let md = HashMap::new();
        let tj = TypeJSON {
            name: name.to_string(),
            r#type: r#type.to_owned(),
            nullable: nullable,
            metadata: md
        };
        self.type_json(serde_json::to_string(&tj).unwrap())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TypeJSON {
    name: String,
    r#type: String,
    nullable: bool,
    metadata: HashMap<String, String>
}

#[derive(Serialize, Deserialize, Debug, Clone, strum::IntoStaticStr)]
pub enum ColumnTypeName {
    #[strum(serialize = "boolean")]
    BOOLEAN,
    #[strum(serialize = "byte")]
    BYTE,
    #[strum(serialize = "short")]
    SHORT,
    #[strum(serialize = "int")]
    INT,
    #[strum(serialize = "long")]
    LONG,
    #[strum(serialize = "float")]
    FLOAT,
    #[strum(serialize = "double")]
    DOUBLE,
    #[strum(serialize = "date")]
    DATE,
    #[strum(serialize = "timestamp")]
    TIMESTAMP,
    #[allow(non_camel_case_types)]
    #[strum(serialize = "timestamp_ntz")]
    TIMESTAMP_NTZ,
    #[strum(serialize = "string")]
    STRING,
    #[strum(serialize = "binary")]
    BINARY,
    #[strum(serialize = "decimal")]
    DECIMAL,
    #[strum(serialize = "interval")]
    INTERVAL,
    #[strum(serialize = "array")]
    ARRAY,
    #[strum(serialize = "struct")]
    STRUCT,
    #[strum(serialize = "map")]
    MAP,
    #[strum(serialize = "char")]
    CHAR,
    #[strum(serialize = "null")]
    NULL,
    #[allow(non_camel_case_types)]
    #[strum(serialize = "user_defined_type")]
    USER_DEFINED_TYPE,
    #[allow(non_camel_case_types)]
    #[strum(serialize = "table_type")]
    TABLE_TYPE
}

#[derive(Serialize, Deserialize, Debug, Builder)]
//#[builder(setter(strip_option), private)]

pub struct CreateTable {
    name: String,
    catalog_name: String,
    schema_name: String,
    table_type: TableType,
    data_source_format: DataSourceFormat,
    columns: Vec<ColumnInfo>,
    storage_location: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>
}

impl CreateTable {
    pub fn builder() -> CreateTableBuilder {
        CreateTableBuilder::create_empty()
            .storage_location(None)
            .comment(None)
            .properties(None)
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::with_settings;
    use crate::testing::test_utils::{cleanup_user_model, test_with_uc};


    #[tokio::test]
    async fn test_round_trip() -> UCRSResult<()> {

        test_with_uc(|port| async move {
            let rc = RequestClient::new(&format!("http://localhost:{}", port), true)?;
            let client = TablesClient::new(&rc);

            let catalog_name = "unity";
            let schema_name = "default";
            let table_name = "mytable";
            let full_name = TablesClient::full_name(catalog_name, schema_name, table_name);

            let initial_list = client.list(&catalog_name, &schema_name, None, None).await?;
            let table_names = initial_list.tables.iter().map(|l| l.name.as_ref().unwrap().to_string()).collect::<Vec<String>>();
            let create_columns = vec![
                ColumnInfoBuilder::default()
                    .name("my_column".to_owned())
                    .type_text(Into::<&'static str>::into(ColumnTypeName::INT).to_owned())
                    .type_name(ColumnTypeName::INT)
                    .position(0)
                    .type_precision(0)
                    .type_scale(0)
                    .nullable(true)
                    .generate_type_json()
                    .build()
                    .unwrap()
            ];

            let table_create_props = CreateTable::builder()
                .catalog_name(catalog_name.to_string())
                .schema_name(schema_name.to_string())
                .name(table_name.to_string())
                .table_type(TableType::EXTERNAL)
                .storage_location(Some("file:///tmp/marksheet_uniform2".to_owned()))
                .data_source_format(DataSourceFormat::DELTA)
                .columns(create_columns)
                .build()
                .unwrap();

            let table_info = client.create(table_create_props).await?;
            let updated_list = client.list(&catalog_name, &schema_name, None, None).await?;

            client.delete(&full_name).await?;

            let final_list = client.list(&catalog_name, &schema_name, None, None).await?;

            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!((
                    initial_list,
                    table_info, 
                    updated_list,
                    final_list
                ));
            });

            Ok(())
        })
        .await
    }
}

