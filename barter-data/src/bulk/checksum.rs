use crate::error::DataError;
use sha2::{Digest, Sha256};

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
        assert!(result.unwrap_err().to_string().contains("checksum mismatch"));
    }

    #[test]
    fn test_verify_sha256_case_insensitive() {
        let data = b"hello world";
        let expected = "B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9";
        assert!(verify_sha256(data, expected).is_ok());
    }

    #[test]
    fn test_parse_binance_checksum() {
        let content =
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9  BTCUSDT-aggTrades-2024-01-01.zip\n";
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
        assert!(result.unwrap_err().to_string().contains("invalid checksum length"));
    }
}
