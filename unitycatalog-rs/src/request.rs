use reqwest::{Client, header::HeaderMap, Method};
use url::Url;
use crate::errors::{UCRSError, UCRSResult};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub struct RequestClient {
    pub base_url: Url,
    client: Client
}

impl RequestClient {
    pub fn new(base_url: &str, disable_ssl: bool) -> UCRSResult<Self> {
        let base_url = Url::parse(base_url)
            .map_err(|e| UCRSError::MalformedURL(e))?;

        let client = Client::builder()
            .danger_accept_invalid_certs(disable_ssl)
            .build()
            .map_err(|e| {
                UCRSError::ClientBuildError(e)
            })?;

        Ok(Self {
            base_url,
            client
        })
    }

    pub fn new_with_headers(base_url: &str, disable_ssl: bool, headers: HeaderMap) -> UCRSResult<Self> {
        let base_url = Url::parse(base_url)
            .map_err(|e| UCRSError::MalformedURL(e))?;

        let client = Client::builder()
            .danger_accept_invalid_certs(disable_ssl)
            .default_headers(headers)
            .build()
            .map_err(|e| {
                UCRSError::ClientBuildError(e)
            })?;

        Ok(Self {
            base_url,
            client
        })
    }

    pub fn new_with_client(base_url: &str, client: Client) -> UCRSResult<Self> {
        let base_url = Url::parse(base_url)
            .map_err(|e| UCRSError::MalformedURL(e))?;

        Ok(Self {
            base_url,
            client
        })

    }

    pub async fn get<B, R>(&self, route: Url, body: Option<B>) -> UCRSResult<R>
        where 
            B: Serialize + std::fmt::Debug,
            R: DeserializeOwned {
        self.request(route, Method::GET,  body).await
    }

    pub async fn post<B, R>(&self, route: Url, body: Option<B>) -> UCRSResult<R>
        where 
            B: Serialize + std::fmt::Debug,
            R: DeserializeOwned {
        self.request(route, Method::POST,  body).await
    }

    pub async fn delete<B, R>(&self, route: Url, body: Option<B>) -> UCRSResult<R>
        where 
            B: Serialize + std::fmt::Debug,
            R: DeserializeOwned {
        self.request(route, Method::DELETE,  body).await
    }

    pub async fn patch<B, R>(&self, route: Url, body: Option<B>) -> UCRSResult<R>
        where 
            B: Serialize + std::fmt::Debug,
            R: DeserializeOwned {
        self.request(route, Method::PATCH,  body).await
    }

    #[tracing::instrument(skip(self))]
    async fn request<B, R>(&self, route: Url, method: reqwest::Method, body: Option<B>) -> UCRSResult<R> 
        where 
            B: Serialize + std::fmt::Debug,
            R: DeserializeOwned{
        let request = self.client.request(method, route);
        let body = body.map(|b| {
            serde_json::to_string(&b).map_err(|be| {
                UCRSError::JSONFormattingError(be)
            })
        });

        let request = match body {
            Some(b) => {
                let b = b?;
                eprintln!("Body is {}", b);
                request
                    .body(b)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
            },
            None => request
        };
        
        let response = request.send().await
            .map_err(|e| UCRSError::RequestError(e))?;

        if let Err(e) = response.error_for_status_ref() {
            let response_body = response.text().await
                .map_err(|e| UCRSError::RequestError(e))?;
            Err(UCRSError::RequestErrorWithResponse(e, response_body))
        } else {
            let response_body = response.json::<R>().await
                .map_err(|e| UCRSError::JSONParsingError(e))?;
            Ok(response_body)
        }
    }
}