use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::{bytes::Bytes, io::StreamReader};

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
) -> BufReader<StreamReader<impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send, Bytes>>
{
    let byte_stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    BufReader::new(StreamReader::new(byte_stream))
}

/// Streaming CSV parser: deserialise records one at a time from any async
/// reader, convert each to [`RestTrade`], and collect.
///
/// Peak memory is bounded to one CSV record at a time (plus the reader's
/// internal buffers), regardless of archive size.
///
/// # Type parameters
/// - `R`: any async reader (e.g. a `GzipDecoder` wrapping a `StreamReader`)
/// - `T`: the CSV record type (e.g. `BybitBulkTrade`), must impl `DeserializeOwned`
/// - `F`: conversion closure `T -> Result<RestTrade, DataError>`
pub(crate) async fn parse_csv_stream<R, T, F>(
    reader: R,
    has_headers: bool,
    convert: F,
) -> Result<Vec<RestTrade>, DataError>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned + Send,
    F: Fn(T) -> Result<RestTrade, DataError>,
{
    use csv_async::AsyncReaderBuilder;
    use tokio_stream::StreamExt;

    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(true)
        .create_deserializer(reader);

    let mut trades = Vec::new();
    let mut records = csv_reader.deserialize::<T>();

    while let Some(result) = records.next().await {
        let record: T =
            result.map_err(|e| DataError::DataParse(format!("CSV stream parse error: {e}")))?;
        trades.push(convert(record)?);

        if trades.len() as u64 >= MAX_RECORDS {
            return Err(DataError::BulkArchive(format!(
                "record count exceeds safety limit of {MAX_RECORDS}"
            )));
        }
    }

    Ok(trades)
}
