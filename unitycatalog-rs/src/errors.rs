use thiserror::Error;

#[derive(Error, Debug)]
pub enum UCRSError {
    #[error("Malformed URL")]
    MalformedURL(#[source] url::ParseError),
    #[error("Error building client")]
    ClientBuildError(#[source] reqwest::Error),
    #[error("Error formatting request body")]
    JSONFormattingError(#[source] serde_json::Error),
    #[error("Request error")]
    RequestError(#[source] reqwest::Error),
    #[error("Request error with response")]
    RequestErrorWithResponse(#[source] reqwest::Error, String),
    #[error("JSON Parsing error")]
    JSONParsingError(#[source] reqwest::Error),
    #[error("Duplicate Catalog name")]
    DuplicateCatalogName(String),
    #[error("Duplicate Schema")]
    DuplicateSchemaName(String),
    #[error("Duplicate table name")]
    DuplicateTableName(String),
    #[error("Catalog not found")]
    CatalogNotFound(String),
    #[error("Schema not found")]
    SchemaNotFound(String),
    #[error("Table not found")]
    TableNotFound(String)
}

pub type UCRSResult<T> = Result<T, UCRSError>;