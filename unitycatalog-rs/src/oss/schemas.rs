use crate::{errors::UCRSError, request::RequestClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::errors::UCRSResult;

pub struct SchemasClient<'a> {
    client: &'a RequestClient
}

impl<'a> SchemasClient<'a> {
    pub fn new(client: &'a RequestClient) -> Self {
        Self { client }
    }

    pub fn full_name(catalog_name: &str, name: &str) -> String {
        format!("{}.{}", catalog_name, name)
    }

    pub async fn list(&self, catalog_name: &str, page_token: Option<String>, max_results: Option<i32>) -> UCRSResult<ListSchemasResponse> {
        let mut url = self.client.base_url.clone().join("/api/2.1/unity-catalog/schemas")
            .map_err(UCRSError::MalformedURL)?;
        url.query_pairs_mut().append_pair("catalog_name", catalog_name);
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("page_token", &token);
        }
        if let Some(max_results) = max_results {
            url.query_pairs_mut().append_pair("max_results", &max_results.to_string());
        }
        self.client.get(url, None::<String>).await
    }

    pub async fn create(&self, props: CreateSchema) -> UCRSResult<SchemaInfo> {
        let route = self.client.base_url.join("/api/2.1/unity-catalog/schemas")
            .map_err(UCRSError::MalformedURL)?;

        let res = self.client.post(route, Some(&props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::CONFLICT) => Err(UCRSError::DuplicateSchemaName(SchemasClient::full_name(&props.catalog_name, &props.name))),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn get(&self, full_name: &str) -> UCRSResult<SchemaInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        let res = self.client.get(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::SchemaNotFound(full_name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn delete(&self, full_name: &str, force: bool) -> UCRSResult<()> {
        let mut path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        path.query_pairs_mut().append_pair("force", &force.to_string());
        let res = self.client.delete(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::SchemaNotFound(full_name.to_owned())),
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

    pub async fn update(&self, full_name: &str, update_props: UpdateSchema)
        -> UCRSResult<SchemaInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        
        let res = self.client.patch(path, Some(&update_props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::SchemaNotFound(full_name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
pub struct ListSchemasResponse {
    schemas: Vec<SchemaInfo>,
    next_page_token: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
pub struct SchemaInfo {
    name: Option<String>,
    catalog_name: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
    full_name: Option<String>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    schema_id: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CreateSchema {
    name: String,
    catalog_name: String,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct UpdateSchema {
    new_name: Option<String>,
    properties: Option<HashMap<String, String>>,
    comment: Option<String>
}