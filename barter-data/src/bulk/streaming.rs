use async_zip::base::read::stream::ZipFileReader;
use csv_async::AsyncReaderBuilder;
use futures::TryStreamExt;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncRead, BufReader};
use tokio_stream::StreamExt;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::StreamReader;

use tokio::sync::mpsc;

use crate::{error::DataError, trade::RestTrade};

/// Default batch size for channel-based streaming parsers.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 1000;

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

    // Drain any remaining bytes and advance past the data descriptor.
    entry
        .skip()
        .await
        .map_err(|e| DataError::BulkArchive(format!("failed to skip remaining ZIP entry data: {e}")))?;

    Ok(trades)
}

/// Channel-based CSV parser: deserialise records one at a time, batch them,
/// and send each batch through the provided `mpsc::Sender`.
///
/// Eliminates the `Vec<RestTrade>` return — trades flow to the consumer as
/// they are parsed. Memory is bounded by `batch_size` regardless of file size.
///
/// Returns the total number of records sent.
pub(crate) async fn parse_csv_into_sender<R, T, F>(
    reader: R,
    has_headers: bool,
    flexible: bool,
    convert: F,
    tx: &mpsc::Sender<Vec<RestTrade>>,
    batch_size: usize,
) -> Result<u64, DataError>
where
    R: AsyncRead + Unpin + Send,
    T: DeserializeOwned + Send,
    F: Fn(T) -> Result<RestTrade, DataError>,
{
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(flexible)
        .create_deserializer(reader);

    let mut batch = Vec::with_capacity(batch_size);
    let mut total: u64 = 0;
    let mut records = csv_reader.deserialize::<T>();

    while let Some(result) = records.next().await {
        if total >= MAX_RECORDS {
            return Err(DataError::BulkArchive(format!(
                "record count exceeds safety limit of {MAX_RECORDS}"
            )));
        }

        let record: T =
            result.map_err(|e| DataError::DataParse(format!("CSV stream parse error: {e}")))?;
        batch.push(convert(record)?);
        total += 1;

        if batch.len() >= batch_size {
            tx.send(std::mem::replace(&mut batch, Vec::with_capacity(batch_size)))
                .await
                .map_err(|_| DataError::BulkArchive("receiver dropped".into()))?;
        }
    }

    // Flush final partial batch.
    if !batch.is_empty() {
        tx.send(batch)
            .await
            .map_err(|_| DataError::BulkArchive("receiver dropped".into()))?;
    }

    Ok(total)
}

/// Channel-based ZIP+CSV parser: open a ZIP stream, extract the first entry,
/// pipe it through [`parse_csv_into_sender`].
///
/// Same lifecycle as [`parse_zip_csv_stream`] but sends batches through a
/// channel instead of collecting into a `Vec`.
///
/// Returns the total number of records sent.
pub(crate) async fn parse_zip_csv_into_sender<R, T, F>(
    reader: R,
    has_headers: bool,
    flexible: bool,
    convert: F,
    tx: &mpsc::Sender<Vec<RestTrade>>,
    batch_size: usize,
) -> Result<u64, DataError>
where
    R: AsyncBufRead + Unpin + Send,
    T: DeserializeOwned + Send,
    F: Fn(T) -> Result<RestTrade, DataError>,
{
    let zip = ZipFileReader::with_tokio(reader);

    let mut entry = match zip.next_with_entry().await {
        Ok(Some(entry)) => entry,
        Ok(None) => return Ok(0),
        Err(e) => {
            return Err(DataError::BulkArchive(format!(
                "failed to read ZIP entry: {e}"
            )))
        }
    };

    let total = {
        let compat_reader = entry.reader_mut().compat();
        parse_csv_into_sender(compat_reader, has_headers, flexible, convert, tx, batch_size)
            .await?
    };

    entry
        .skip()
        .await
        .map_err(|e| {
            DataError::BulkArchive(format!("failed to skip remaining ZIP entry data: {e}"))
        })?;

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    fn make_test_zip(csv_content: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let cursor = Cursor::new(&mut buf);
            let mut writer = zip::ZipWriter::new(cursor);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            writer.start_file("data.csv", options).unwrap();
            writer.write_all(csv_content).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    #[tokio::test]
    async fn test_parse_zip_csv_stream_basic() {
        #[derive(Debug, serde::Deserialize)]
        struct TestRecord {
            name: String,
            value: f64,
        }

        let csv = b"name,value\nalpha,1.5\nbeta,2.5\n";
        let zip_bytes = make_test_zip(csv);

        let reader = tokio::io::BufReader::new(&zip_bytes[..]);
        let trades = parse_zip_csv_stream::<_, TestRecord, _>(
            reader,
            true,
            false,
            |r: TestRecord| {
                Ok(RestTrade {
                    id: r.name,
                    time: chrono::DateTime::from_timestamp_millis(0).unwrap(),
                    price: r.value,
                    amount: 0.0,
                    side: barter_instrument::Side::Buy,
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].id, "alpha");
        assert_eq!(trades[0].price, 1.5);
        assert_eq!(trades[1].id, "beta");
        assert_eq!(trades[1].price, 2.5);
    }

    #[tokio::test]
    async fn test_parse_zip_csv_stream_empty_csv() {
        // ZIP with a single CSV entry that has only a header row (no data rows).
        let csv = b"name,value\n";
        let zip_bytes = make_test_zip(csv);

        #[derive(Debug, serde::Deserialize)]
        struct TestRecord {
            name: String,
            value: f64,
        }

        let reader = tokio::io::BufReader::new(&zip_bytes[..]);
        let trades = parse_zip_csv_stream::<_, TestRecord, _>(
            reader,
            true,
            false,
            |_| unreachable!("no data rows to convert"),
        )
        .await
        .unwrap();

        assert!(trades.is_empty());
    }

    /// Compile-time assertion that the future returned by `parse_zip_csv_stream`
    /// is `Send` (required for `tokio::spawn`). If `async_zip` changes its
    /// internal types to be `!Send`, this test will fail to compile.
    #[tokio::test]
    async fn test_parse_zip_csv_stream_is_send() {
        #[derive(Debug, serde::Deserialize)]
        struct Dummy {
            x: String,
        }

        let zip_bytes = make_test_zip(b"x\nhello\n");

        // Move zip_bytes into the spawned task so the reader has a 'static
        // lifetime, satisfying tokio::spawn's Send + 'static bound.
        let handle = tokio::spawn(async move {
            let reader = tokio::io::BufReader::new(&zip_bytes[..]);
            parse_zip_csv_stream::<_, Dummy, _>(reader, true, false, |_| {
                Ok(RestTrade {
                    id: String::new(),
                    time: chrono::DateTime::from_timestamp_millis(0).unwrap(),
                    price: 0.0,
                    amount: 0.0,
                    side: barter_instrument::Side::Buy,
                })
            })
            .await
        });
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
