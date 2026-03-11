use crate::error::DataError;
use sha2::{Digest, Sha256};
use std::path::Path;

/// Verify a SHA-256 checksum against data bytes.
///
/// The `expected` string should be the hex-encoded SHA-256 hash (lowercase),
/// matching the format used by Binance Vision `.CHECKSUM` files.
pub fn verify_sha256(data: &[u8], expected: &str) -> Result<(), DataError> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let actual = format!("{:x}", hasher.finalize());

    if actual == expected.to_lowercase() {
        Ok(())
    } else {
        Err(DataError::Socket(format!(
            "checksum mismatch: expected {expected}, got {actual}"
        )))
    }
}

/// Compute the SHA-256 hex digest of the given data.
pub fn compute_sha256(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Parse a Binance Vision `.CHECKSUM` file content.
///
/// Format: `<sha256_hex>  <filename>\n`
/// Returns the hex checksum string.
pub fn parse_binance_checksum(content: &str) -> Result<String, DataError> {
    let checksum = content
        .split_whitespace()
        .next()
        .ok_or_else(|| DataError::Socket("empty checksum file".to_string()))?;

    if checksum.len() != 64 {
        return Err(DataError::Socket(format!(
            "invalid checksum length: expected 64 hex chars, got {}",
            checksum.len()
        )));
    }

    Ok(checksum.to_lowercase())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_sha256_valid() {
        let data = b"hello world";
        // SHA-256 of "hello world"
        let expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        assert!(verify_sha256(data, expected).is_ok());
    }

    #[test]
    fn test_verify_sha256_invalid() {
        let data = b"hello world";
        let expected = "0000000000000000000000000000000000000000000000000000000000000000";
        let result = verify_sha256(data, expected);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("checksum mismatch")
        );
    }

    #[test]
    fn test_verify_sha256_case_insensitive() {
        let data = b"hello world";
        let expected = "B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9";
        assert!(verify_sha256(data, expected).is_ok());
    }

    #[test]
    fn test_parse_binance_checksum() {
        let content = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9  BTCUSDT-aggTrades-2024-01-01.zip\n";
        let checksum = parse_binance_checksum(content).unwrap();
        assert_eq!(
            checksum,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn test_parse_binance_checksum_empty() {
        let result = parse_binance_checksum("");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_binance_checksum_invalid_length() {
        let content = "abc123  file.zip\n";
        let result = parse_binance_checksum(content);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid checksum length")
        );
    }

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
}
