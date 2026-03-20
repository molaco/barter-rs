use csv_async::AsyncReaderBuilder;
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_util::io::StreamReader;

use crate::{error::DataError, trade::RestTrade};

/// Safety limit: no single day should produce more than 50 million records.
const MAX_RECORDS: u64 = 50_000_000;

/// Convert a [`reqwest::Response`] into a buffered async reader suitable for
/// piping into a decompressor (`GzipDecoder`, `Lz4Decoder`, etc.).
///
/// The returned reader streams HTTP chunks on demand — no bytes are buffered
/// beyond tokio's internal `BufReader` window (default 8 KiB).
pub(crate) fn response_to_async_read(
    response: reqwest::Response,
) -> impl AsyncBufRead + Send + Unpin {
    let byte_stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    BufReader::new(StreamReader::new(byte_stream))
}

/// Streaming CSV parser: deserialise records one at a time from any async
/// reader, convert each to [`RestTrade`], and collect into a `Vec`.
///
/// Parsing is streaming — only one CSV record is in flight at a time, so
/// the intermediate decompressed-bytes buffer is eliminated. The returned
/// `Vec<RestTrade>` still grows with the number of trades.
///
/// # Type parameters
/// - `R`: any async reader (e.g. a `GzipDecoder` wrapping a `StreamReader`)
/// - `T`: the CSV record type (e.g. `BybitBulkTrade`), must impl `DeserializeOwned`
/// - `F`: conversion closure `T -> Result<RestTrade, DataError>`
pub(crate) async fn parse_csv_stream<R, T, F>(
    reader: R,
    has_headers: bool,
    flexible: bool,
    convert: F,
) -> Result<Vec<RestTrade>, DataError>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned + Send,
    F: Fn(T) -> Result<RestTrade, DataError>,
{
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(flexible)
        .create_deserializer(reader);

    let mut trades = Vec::new();
    let mut records = csv_reader.deserialize::<T>();

    while let Some(result) = records.next().await {
        if trades.len() as u64 >= MAX_RECORDS {
            return Err(DataError::BulkArchive(format!(
                "record count exceeds safety limit of {MAX_RECORDS}"
            )));
        }

        let record: T =
            result.map_err(|e| DataError::DataParse(format!("CSV stream parse error: {e}")))?;
        trades.push(convert(record)?);
    }

    Ok(trades)
}
