pub mod trades;

use crate::{error::DataError, trade::RestTrade};
use futures::Stream;
use std::path::Path;
use trades::parse_trades;

/// Maximum allowed uncompressed archive entry size (2 GiB).
const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Standalone archive parser for Kraken bulk trade data.
///
/// Kraken publishes historical trade archives as ZIP files that must be
/// manually downloaded (e.g. from Google Drive). This parser reads a local
/// ZIP file and extracts trades for a given trading pair.
///
/// Each ZIP contains one CSV file per trading pair.
#[derive(Debug)]
pub struct KrakenArchiveParser;

impl KrakenArchiveParser {
    /// Parse all trades for a pair from a local Kraken archive ZIP file.
    ///
    /// Searches the ZIP for a CSV file whose name contains `pair` (case-insensitive)
    /// and parses all trade records from it.
    pub fn parse_zip_trades(zip_path: &Path, pair: &str) -> Result<Vec<RestTrade>, DataError> {
        let file = std::fs::File::open(zip_path).map_err(|e| {
            DataError::Io(format!(
                "failed to open ZIP file '{}': {e}",
                zip_path.display()
            ))
        })?;

        let mut archive = zip::ZipArchive::new(file).map_err(|e| {
            DataError::BulkArchive(format!(
                "failed to read ZIP archive '{}': {e}",
                zip_path.display()
            ))
        })?;

        let pair_lower = pair.to_lowercase();

        // Find the CSV file matching the pair name.
        let csv_index = (0..archive.len())
            .find(|&i| {
                archive
                    .by_index(i)
                    .map(|f| f.name().to_lowercase().contains(&pair_lower))
                    .unwrap_or(false)
            })
            .ok_or_else(|| {
                DataError::BulkArchive(format!(
                    "no CSV file found for pair '{pair}' in '{}'",
                    zip_path.display()
                ))
            })?;

        let csv_file = archive
            .by_index(csv_index)
            .map_err(|e| DataError::BulkArchive(format!("failed to read ZIP entry: {e}")))?;

        if csv_file.size() > MAX_DECOMPRESSED_SIZE {
            return Err(DataError::BulkArchive(format!(
                "Kraken ZIP entry too large: {} bytes (max {MAX_DECOMPRESSED_SIZE} bytes)",
                csv_file.size(),
            )));
        }

        let mut csv_data = Vec::with_capacity(csv_file.size() as usize);
        std::io::Read::read_to_end(&mut { csv_file }, &mut csv_data).map_err(|e| {
            DataError::BulkArchive(format!("failed to read CSV data from ZIP: {e}"))
        })?;

        parse_trades(&csv_data)
    }

    /// Stream batches of trades from a local Kraken archive ZIP file.
    ///
    /// Reads and parses the entire CSV on a blocking thread, then yields
    /// batches of `batch_size` trades as stream items.
    pub async fn stream_trades(
        zip_path: &Path,
        pair: &str,
        batch_size: usize,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let path = zip_path.to_path_buf();
        let pair = pair.to_string();
        let result = tokio::task::spawn_blocking(move || Self::parse_zip_trades(&path, &pair))
            .await
            .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")));

        match result {
            Ok(Ok(trades)) => {
                let batches: Vec<Result<Vec<RestTrade>, DataError>> = trades
                    .chunks(batch_size.max(1))
                    .map(|chunk| Ok(chunk.to_vec()))
                    .collect();
                futures::stream::iter(batches)
            }
            Ok(Err(e)) => futures::stream::iter(vec![Err(e)]),
            Err(e) => futures::stream::iter(vec![Err(e)]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicU32, Ordering},
    };

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// Create a temporary ZIP file containing a CSV with sample Kraken trade data.
    /// Returns the path; caller is responsible for cleanup.
    fn create_test_zip(pair: &str, csv_content: &[u8]) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!(
            "barter_kraken_test_{}_{}_{pair}.zip",
            std::process::id(),
            id
        ));
        let file = std::fs::File::create(&path).unwrap();
        let mut zip_writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        zip_writer
            .start_file(format!("{pair}.csv"), options)
            .unwrap();
        zip_writer.write_all(csv_content).unwrap();
        zip_writer.finish().unwrap();
        path
    }

    #[test]
    fn test_parse_zip_trades() {
        let csv = b"42000.00000,0.10000000,1704067200.1234,b,m,,100\n\
                     42001.00000,0.20000000,1704067201.5678,s,l,,101\n";
        let path = create_test_zip("XBTUSD", csv);

        let trades = KrakenArchiveParser::parse_zip_trades(&path, "XBTUSD").unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].price, 42000.0);
        assert_eq!(trades[1].price, 42001.0);
    }

    #[test]
    fn test_parse_zip_trades_case_insensitive_pair() {
        let csv = b"42000.00000,0.10000000,1704067200.0,b,m,,100\n";
        let path = create_test_zip("XBTUSD", csv);

        let trades = KrakenArchiveParser::parse_zip_trades(&path, "xbtusd").unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(trades.len(), 1);
    }

    #[test]
    fn test_parse_zip_trades_pair_not_found() {
        let csv = b"42000.00000,0.10000000,1704067200.0,b,m,,100\n";
        let path = create_test_zip("XBTUSD", csv);

        let result = KrakenArchiveParser::parse_zip_trades(&path, "ETHUSD");
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_zip_trades_invalid_path() {
        let result = KrakenArchiveParser::parse_zip_trades(Path::new("/nonexistent.zip"), "BTC");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stream_trades_batching() {
        let csv = b"42000.00000,0.10000000,1704067200.0,b,m,,100\n\
                     42001.00000,0.20000000,1704067201.0,s,l,,101\n\
                     42002.00000,0.30000000,1704067202.0,b,m,,102\n";
        let path = create_test_zip("XBTUSD", csv);

        let stream = KrakenArchiveParser::stream_trades(&path, "XBTUSD", 2).await;
        let batches: Vec<_> = futures::StreamExt::collect(stream).await;

        let _ = std::fs::remove_file(&path);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].as_ref().unwrap().len(), 2);
        assert_eq!(batches[1].as_ref().unwrap().len(), 1);
    }
}
