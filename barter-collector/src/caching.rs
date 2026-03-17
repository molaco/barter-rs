use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Compute the SHA-256 hex digest of the given data.
pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Check whether a `.verified` marker file exists with a matching checksum.
///
/// The marker file format is: `<sha256hex>  <original_filename>\n`.
/// Returns `true` if the marker exists and its stored hash matches `expected_hash`.
pub fn should_skip(marker_path: &Path, expected_hash: &str) -> bool {
    std::fs::read_to_string(marker_path)
        .ok()
        .and_then(|s| s.split_whitespace().next().map(|h| h.to_string()))
        .map(|stored| stored == expected_hash.to_lowercase())
        .unwrap_or(false)
}

/// Write a `.verified` marker file atomically (tmp + rename).
///
/// Format: `<sha256hex>  <original_filename>\n`.
/// Failures are non-fatal and logged as warnings.
pub fn write_verified_marker(marker_path: &Path, hash: &str, original_filename: &str) {
    let content = format!("{hash}  {original_filename}\n");

    // Ensure parent directory exists
    if let Some(parent) = marker_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::warn!(
                path = %marker_path.display(),
                error = %e,
                "failed to create marker directory, skipping marker write"
            );
            return;
        }
    }

    // Atomic write via tmp + rename
    let tmp_path = marker_path.with_extension("verified.tmp");
    match std::fs::write(&tmp_path, &content) {
        Ok(()) => {
            if let Err(e) = std::fs::rename(&tmp_path, marker_path) {
                tracing::warn!(
                    path = %marker_path.display(),
                    error = %e,
                    "failed to rename marker file"
                );
                // Clean up tmp file on rename failure
                let _ = std::fs::remove_file(&tmp_path);
            }
        }
        Err(e) => {
            tracing::warn!(
                path = %tmp_path.display(),
                error = %e,
                "failed to write marker tmp file, skipping marker"
            );
        }
    }
}

/// Build the `.verified` marker path for a given archive URL and cache directory.
pub fn marker_path_for_url(cache_dir: &Path, url: &str) -> PathBuf {
    // Extract filename from the URL (last path segment).
    let filename = url.rsplit('/').next().unwrap_or("unknown");
    cache_dir.join(format!("{filename}.verified"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_sha256() {
        let hash = compute_sha256(b"hello world");
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_should_skip_valid_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("test.zip.verified");
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        std::fs::write(&marker, format!("{hash}  test.zip\n")).unwrap();
        assert!(should_skip(&marker, hash));
    }

    #[test]
    fn test_should_skip_missing_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("nonexistent.verified");
        assert!(!should_skip(&marker, "abc123"));
    }

    #[test]
    fn test_should_skip_wrong_hash() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("test.zip.verified");
        std::fs::write(
            &marker,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  test.zip\n",
        )
        .unwrap();
        assert!(!should_skip(
            &marker,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn test_write_verified_marker_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("subdir/test.zip.verified");
        let hash = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        write_verified_marker(&marker, hash, "test.zip");
        assert!(marker.exists());
        assert!(should_skip(&marker, hash));
    }

    #[test]
    fn test_marker_path_for_url() {
        let dir = std::path::Path::new("/tmp/cache");
        let url = "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-01-01.zip";
        let path = marker_path_for_url(dir, url);
        assert_eq!(
            path,
            PathBuf::from("/tmp/cache/BTCUSDT-aggTrades-2024-01-01.zip.verified")
        );
    }
}
