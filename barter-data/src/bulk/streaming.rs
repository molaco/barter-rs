use async_zip::base::read::stream::ZipFileReader;
use csv_async::AsyncReaderBuilder;
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
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

/// Streaming ZIP+CSV parser: open a ZIP stream, extract the first entry,
/// pipe it through [`parse_csv_stream`], and return the parsed trades.
///
/// Encapsulates the entire `async_zip` type-state lifecycle so callers
/// don't need to manage the `ZipFileReader` → `ZipEntryReader` transition.
///
/// # Type parameters
/// - `R`: any async buffered reader (e.g. an HTTP response stream)
/// - `T`: the CSV record type, must impl `DeserializeOwned`
/// - `F`: conversion closure `T -> Result<RestTrade, DataError>`
pub(crate) async fn parse_zip_csv_stream<R, T, F>(
    reader: R,
    has_headers: bool,
    flexible: bool,
    convert: F,
) -> Result<Vec<RestTrade>, DataError>
where
    R: AsyncBufRead + Unpin + Send,
    T: DeserializeOwned + Send,
    F: Fn(T) -> Result<RestTrade, DataError>,
{
    let zip = ZipFileReader::with_tokio(reader);

    // Get the first entry from the archive.
    let mut entry = match zip.next_with_entry().await {
        Ok(Some(entry)) => entry,
        // Empty archive — not an error. Distinct from 404 (handled by caller)
        // where the file doesn't exist at all; here the ZIP exists but has no
        // entries.
        Ok(None) => return Ok(Vec::new()),
        Err(e) => {
            return Err(DataError::BulkArchive(format!(
                "failed to read ZIP entry: {e}"
            )))
        }
    };

    // Scope the entry reader borrow so it's released before calling done().
    let trades = {
        let compat_reader = entry.reader_mut().compat();
        parse_csv_stream(compat_reader, has_headers, flexible, convert).await?
    };
    // compat_reader dropped here, &mut borrow on entry released.

    // Verify EOF was reached and advance past the data descriptor.
    entry
        .done()
        .await
        .map_err(|e| DataError::BulkArchive(format!("ZIP entry finalization failed: {e}")))?;

    Ok(trades)
}
