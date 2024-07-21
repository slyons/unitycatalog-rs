use crate::errors::UCRSResult;
use crate::{errors::UCRSError, request::RequestClient};
use derive_builder::Builder;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct SchemasClient<'a> {
    client: &'a RequestClient,
}

impl<'a> SchemasClient<'a> {
    pub fn new(client: &'a RequestClient) -> Self {
        Self { client }
    }

    pub fn full_name(catalog_name: &str, name: &str) -> String {
        format!("{}.{}", catalog_name, name)
    }

    pub async fn list(
        &self,
        catalog_name: &str,
        page_token: Option<String>,
        max_results: Option<i32>,
    ) -> UCRSResult<ListSchemasResponse> {
        let mut url = self
            .client
            .base_url
            .clone()
            .join("/api/2.1/unity-catalog/schemas")
            .map_err(UCRSError::MalformedURL)?;
        url.query_pairs_mut()
            .append_pair("catalog_name", catalog_name);
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("page_token", &token);
        }
        if let Some(max_results) = max_results {
            url.query_pairs_mut()
                .append_pair("max_results", &max_results.to_string());
        }
        self.client.get(url, None::<String>).await
    }

    pub async fn create(&self, props: CreateSchema) -> UCRSResult<SchemaInfo> {
        let route = self
            .client
            .base_url
            .join("/api/2.1/unity-catalog/schemas")
            .map_err(UCRSError::MalformedURL)?;

        let res = self.client.post(route, Some(&props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::CONFLICT) => Err(UCRSError::DuplicateSchemaName(
                    SchemasClient::full_name(&props.catalog_name, &props.name),
                )),
                _ => res,
            }
        } else {
            res
        }
    }

    pub async fn get(&self, full_name: &str) -> UCRSResult<SchemaInfo> {
        let path = self
            .client
            .base_url
            .join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        let res = self.client.get(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => Err(UCRSError::SchemaNotFound(full_name.to_owned())),
                _ => res,
            }
        } else {
            res
        }
    }

    pub async fn delete(&self, full_name: &str, force: bool) -> UCRSResult<()> {
        let mut path = self
            .client
            .base_url
            .join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;
        path.query_pairs_mut()
            .append_pair("force", &force.to_string());
        let res = self.client.delete(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => Err(UCRSError::SchemaNotFound(full_name.to_owned())),
                _ => res,
            }
        } else if let Err(UCRSError::JSONParsingError(_)) = res {
            // This is because DELETE returns "200 OK" as a response body :/
            Ok(())
        } else {
            res
        }
    }

    pub async fn update(
        &self,
        full_name: &str,
        update_props: UpdateSchema,
    ) -> UCRSResult<SchemaInfo> {
        let path = self
            .client
            .base_url
            .join(&format!("/api/2.1/unity-catalog/schemas/{}", full_name))
            .map_err(UCRSError::MalformedURL)?;

        let res = self.client.patch(path, Some(&update_props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => Err(UCRSError::SchemaNotFound(full_name.to_owned())),
                _ => res,
            }
        } else {
            res
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default)]
pub struct ListSchemasResponse {
    schemas: Vec<SchemaInfo>,
    next_page_token: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Builder)]
pub struct SchemaInfo {
    name: Option<String>,
    catalog_name: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
    full_name: Option<String>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    schema_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default, Builder)]
pub struct CreateSchema {
    name: String,
    catalog_name: String,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Default, Builder)]
pub struct UpdateSchema {
    name: String,
    new_name: Option<String>,
    properties: Option<HashMap<String, String>>,
    comment: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::test_utils::{cleanup_user_model, test_with_uc};
    use insta::with_settings;

    #[tokio::test]
    async fn test_round_trip() -> UCRSResult<()> {
        test_with_uc(|port| async move {
            let rc = RequestClient::new(&format!("http://localhost:{}", port), true)?;
            let schema_client = SchemasClient::new(&rc);

            let catalog_name = "unity";
            let name = "myschema";
            let full_name = SchemasClient::full_name(catalog_name, name);
            let initial_schema_list = schema_client.list(catalog_name, None, None).await?;
            let create_props = CreateSchema {
                name: name.to_owned(),
                catalog_name: catalog_name.to_owned(),
                ..Default::default()
            };
            let schema = schema_client.create(create_props).await?;
            let second_list = schema_client.list(catalog_name, None, None).await?;
            assert_ne!(initial_schema_list, second_list);

            let update_props = UpdateSchema {
                name: name.to_owned(),
                comment: Some("New comment".to_owned()),
                ..Default::default()
            };
            let updated = schema_client.update(&full_name, update_props).await?;

            let fetch = schema_client.get(&full_name).await?;
            assert_eq!(updated, fetch);
            schema_client.delete(&full_name, false).await?;

            let after_delete = schema_client.list(catalog_name, None, None).await?;
            assert_eq!(initial_schema_list, after_delete);

            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!((
                    initial_schema_list,
                    schema,
                    second_list,
                    updated,
                    after_delete
                ));

            });

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_not_found() -> UCRSResult<()> {
        test_with_uc(|port| async move {
            let rc = RequestClient::new(&format!("http://localhost:{}", port), true)?;
            let schema_client = SchemasClient::new(&rc);

            let catalog_name = "unity";
            let name = "myschema2";
            let full_name = SchemasClient::full_name(catalog_name, name);
            let fetch = schema_client.get(&full_name).await;

            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!((
                    fetch
                ));
            });

            Ok(())
        })
        .await
    }
}
