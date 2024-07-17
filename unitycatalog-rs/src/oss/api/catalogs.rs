use crate::{errors::UCRSError, request::RequestClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::errors::UCRSResult;
use derive_builder::Builder;

pub struct CatalogsClient<'a> {
    client: &'a RequestClient
}

impl<'a> CatalogsClient<'a> {
    pub fn new(client: &'a RequestClient) -> Self {
        Self {client}
    }

    pub async fn list(&self, page_token:Option<String>, max_results: Option<i32>) -> UCRSResult<ListCatalogResponse> {
        let mut url = self.client.base_url.clone().join("/api/2.1/unity-catalog/catalogs")
            .map_err(UCRSError::MalformedURL)?;
        if let Some(token) = page_token {
            url.query_pairs_mut().append_pair("page_token", &token);
        }
        if let Some(max_results) = max_results {
            url.query_pairs_mut().append_pair("max_results", &max_results.to_string());
        }
        self.client.get(url, None::<String>).await
    }

    pub async fn create(&self, props: CreateCatalog) -> UCRSResult<CatalogInfo> {
        let route = self.client.base_url.join("/api/2.1/unity-catalog/catalogs")
            .map_err(UCRSError::MalformedURL)?;

        let res = self.client.post(route, Some(&props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::CONFLICT) => Err(UCRSError::DuplicateCatalogName(props.name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn get(&self, name: &str) -> UCRSResult<CatalogInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/catalogs/{}", name))
            .map_err(UCRSError::MalformedURL)?;
        let res = self.client.get(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::CatalogNotFound(name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }

    pub async fn delete(&self, name: &str, force: bool) -> UCRSResult<()> {
        let mut path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/catalogs/{}", name))
            .map_err(UCRSError::MalformedURL)?;
        path.query_pairs_mut().append_pair("force", &force.to_string());
        let res = self.client.delete(path, None::<String>).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::CatalogNotFound(name.to_owned())),
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

    pub async fn update(&self, name: &str, update_props: UpdateCatalog)
        -> UCRSResult<CatalogInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/catalogs/{}", name))
            .map_err(UCRSError::MalformedURL)?;
        
        let res = self.client.patch(path, Some(&update_props)).await;
        if let Err(UCRSError::RequestError(ref res_inner)) = res {
            match res_inner.status() {
                Some(StatusCode::NOT_FOUND) => 
                    Err(UCRSError::CatalogNotFound(name.to_owned())),
                _ => res
            }
        } else {
            res
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Builder)]
pub struct CreateCatalog {
    name: String,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct ListCatalogResponse {
    catalogs:Vec<CatalogInfo>,
    next_page_token: Option<String>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Default)]
pub struct CatalogInfo {
    name: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    id: Option<String>
}

#[derive(Serialize, Deserialize, Debug, Default, Builder)]
pub struct UpdateCatalog {
    new_name: Option<String>,
    properties: Option<HashMap<String, String>>,
    comment: Option<String>
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::with_settings;
    use crate::testing::test_utils::{cleanup_user_model, test_with_uc};

    #[tokio::test]
    async fn test_list() -> UCRSResult<()> {

        test_with_uc(|port| async move {
            let rc = RequestClient::new(&format!("http://localhost:{}", port), true)?;
            let client = CatalogsClient::new(&rc);
            let list = client.list(None, None).await;

            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!(list);
            });

            Ok(())
        })
        .await
        
    }

    #[tokio::test]
    async fn test_round_trip() -> UCRSResult<()> {

        test_with_uc(|port| async move {
            let rc = RequestClient::new(&format!("http://localhost:{}", port), true)?;
            let catalog_client = CatalogsClient::new(&rc);

            let initial_list = catalog_client.list(None, None).await?;
            let create_props = CreateCatalogBuilder::default()
                .name("mycatalog".to_string())
                .build()
                .unwrap();
            let cinfo = catalog_client.create(create_props).await?;
            let in_list = catalog_client.list(None, None).await?;
            let update_props = UpdateCatalog {
                comment: Some("new comment".to_string()),
                ..Default::default()
            };
            let cinfo_patched = catalog_client.update("mycatalog", update_props).await?;
            catalog_client.delete("mycatalog", false).await?;
            let after_list = catalog_client.list(None, None).await?;

            assert_eq!(initial_list.catalogs, after_list.catalogs);

            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!((
                    initial_list,
                    cinfo, 
                    in_list,
                    cinfo_patched,
                    after_list
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
            let catalog_client = CatalogsClient::new(&rc);

            let res = catalog_client.delete("mycatalog", false).await;
            with_settings!({
                filters => cleanup_user_model()
            }, {
                insta::assert_debug_snapshot!(res);

            });

            Ok(())
        })
        .await
        
    }
}