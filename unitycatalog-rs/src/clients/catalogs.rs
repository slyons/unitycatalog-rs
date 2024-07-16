use crate::{errors::UCRSError, request::RequestClient};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::errors::UCRSResult;

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

    pub async fn create(&self, name: &str, comment: Option<&str>, properties: Option<HashMap<String, String>>) -> UCRSResult<CatalogInfo> {
        let route = self.client.base_url.join("/api/2.1/unity-catalog/catalogs")
            .map_err(UCRSError::MalformedURL)?;
        let body = CreateCatalog {
            name: name.to_owned(),
            comment: comment.map(|c| c.to_owned()),
            properties
        };
        let res = self.client.post(route, Some(body)).await;
        if let Err(UCRSError::RequestError(ref resinner)) = res {
            if resinner.status().unwrap() == 409 {
                Err(UCRSError::DuplicateCatalogName(name.to_owned()))
            } else {
                res
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
        if force {
            path.query_pairs_mut().append_pair("force", "true");
        } else {
            path.query_pairs_mut().append_pair("force", "false");
        };
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

    pub async fn update(&self, name: &str, new_name: Option<&str>, properties: Option<HashMap<String, String>>, comment: Option<&str>)
        -> UCRSResult<CatalogInfo> {
        let path = self.client.base_url.join(&format!("/api/2.1/unity-catalog/catalogs/{}", name))
            .map_err(UCRSError::MalformedURL)?;
        let body = UpdateCatalog {
            new_name: new_name.map(|n| n.to_owned()),
            properties,
            comment: comment.map(|n| n.to_owned())
        };
        let res = self.client.patch(path, Some(body)).await;
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

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateCatalog {
    name: String,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct ListCatalogResponse {
    catalogs:Vec<CatalogInfo>,
    next_page_token: Option<String>
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct CatalogInfo {
    name: Option<String>,
    comment: Option<String>,
    properties: Option<HashMap<String, String>>,
    created_at: Option<i64>,
    updated_at: Option<i64>,
    id: Option<String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateCatalog {
    new_name: Option<String>,
    properties: Option<HashMap<String, String>>,
    comment: Option<String>
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::{assert_debug_snapshot, with_settings};

    #[tokio::test]
    async fn test_list() -> UCRSResult<()> {
        let rc = RequestClient::new("http://localhost:8080", true)?;
        let client = CatalogsClient::new(&rc);
        let list = client.list(None, None).await;

        with_settings!({
            filters => crate::testing::cleanup_user_model()
        }, {
            insta::assert_debug_snapshot!(list);
        });

        Ok(())
    }

    #[tokio::test]
    async fn test_round_trip() -> UCRSResult<()> {

        
        let rc = RequestClient::new("http://localhost:8080", true)?;
        let catalog_client = CatalogsClient::new(&rc);

        let initial_list = catalog_client.list(None, None).await?;
        let cinfo = catalog_client.create("mycatalog", None, None).await?;
        let in_list = catalog_client.list(None, None).await?;
        let cinfo_patched = catalog_client.update("mycatalog", None, None, Some("new comment")).await?;
        eprintln!("{:#?}", cinfo_patched);
        let str = catalog_client.delete("mycatalog", false).await?;
        let after_list = catalog_client.list(None, None).await?;

        assert_eq!(initial_list.catalogs, after_list.catalogs);

        with_settings!({
            filters => crate::testing::cleanup_user_model()
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
    }
}