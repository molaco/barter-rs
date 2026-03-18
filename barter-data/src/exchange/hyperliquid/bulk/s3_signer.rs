//! Minimal AWS SigV4 signer for S3 GET requests.
//!
//! Only supports unsigned-payload GET requests (sufficient for S3 downloads).
//! Uses hmac + sha2 already in the workspace — no additional dependencies.

use chrono::Utc;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// AWS credentials loaded from environment.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

impl AwsCredentials {
    /// Load credentials using the standard AWS resolution order:
    /// 1. Environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
    /// 2. Shared credentials file (`~/.aws/credentials`, `[default]` profile)
    pub fn from_env() -> Option<Self> {
        Self::from_env_vars().or_else(Self::from_credentials_file)
    }

    fn from_env_vars() -> Option<Self> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Some(Self {
            access_key,
            secret_key,
            session_token,
        })
    }

    fn from_credentials_file() -> Option<Self> {
        let path = std::env::var("AWS_SHARED_CREDENTIALS_FILE")
            .ok()
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .map(|h| std::path::PathBuf::from(h).join(".aws").join("credentials"))
            })?;

        let contents = std::fs::read_to_string(&path).ok()?;

        let mut in_default = false;
        let mut access_key = None;
        let mut secret_key = None;
        let mut session_token = None;

        for line in contents.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('[') {
                in_default = trimmed == "[default]";
                continue;
            }
            if !in_default {
                continue;
            }
            if let Some((key, value)) = trimmed.split_once('=') {
                match key.trim() {
                    "aws_access_key_id" => access_key = Some(value.trim().to_string()),
                    "aws_secret_access_key" => secret_key = Some(value.trim().to_string()),
                    "aws_session_token" => session_token = Some(value.trim().to_string()),
                    _ => {}
                }
            }
        }

        Some(Self {
            access_key: access_key?,
            secret_key: secret_key?,
            session_token,
        })
    }
}

/// Headers produced by SigV4 signing that must be added to the request.
#[non_exhaustive]
#[derive(Debug)]
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: &'static str,
    pub x_amz_security_token: Option<String>,
}

/// Sign an S3 GET request using AWS SigV4.
///
/// `url` must be a full `https://bucket.s3.amazonaws.com/path` URL.
/// Region is extracted as `us-east-1` (Hyperliquid bucket location).
pub fn sign_s3_get(url: &str, creds: &AwsCredentials, region: &str) -> SignedHeaders {
    let parsed: url::Url = url.parse().expect("invalid URL");
    let host = parsed.host_str().expect("URL has no host");
    let path = parsed.path();

    let now = Utc::now();
    let date_stamp = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

    let scope = format!("{date_stamp}/{region}/s3/aws4_request");

    // Canonical headers (must be sorted by name)
    let mut canonical_headers = format!(
        "host:{host}\nx-amz-content-sha256:{UNSIGNED_PAYLOAD}\nx-amz-date:{amz_date}\nx-amz-request-payer:requester\n"
    );
    let mut signed_headers = "host;x-amz-content-sha256;x-amz-date;x-amz-request-payer".to_string();

    if let Some(ref token) = creds.session_token {
        canonical_headers = format!(
            "host:{host}\nx-amz-content-sha256:{UNSIGNED_PAYLOAD}\nx-amz-date:{amz_date}\nx-amz-request-payer:requester\nx-amz-security-token:{token}\n"
        );
        signed_headers =
            "host;x-amz-content-sha256;x-amz-date;x-amz-request-payer;x-amz-security-token"
                .to_string();
    }

    // Canonical request
    let canonical_request =
        format!("GET\n{path}\n\n{canonical_headers}\n{signed_headers}\n{UNSIGNED_PAYLOAD}");

    let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));

    // String to sign
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}");

    // Signing key
    let signing_key = derive_signing_key(&creds.secret_key, &date_stamp, region, "s3");

    // Signature
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        creds.access_key
    );

    SignedHeaders {
        authorization,
        x_amz_date: amz_date,
        x_amz_content_sha256: UNSIGNED_PAYLOAD,
        x_amz_security_token: creds.session_token.clone(),
    }
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_signing_key_deterministic() {
        let key1 = derive_signing_key("secret", "20260309", "us-east-1", "s3");
        let key2 = derive_signing_key("secret", "20260309", "us-east-1", "s3");
        assert_eq!(key1, key2);
        assert_eq!(key1.len(), 32);
    }

    #[test]
    fn test_sign_produces_valid_auth_header() {
        let creds = AwsCredentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
        };
        let signed = sign_s3_get(
            "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com/node_fills_by_block/hourly/2024-06-15/03",
            &creds,
            "us-east-1",
        );
        assert!(
            signed
                .authorization
                .starts_with("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/")
        );
        assert!(signed.authorization.contains("/us-east-1/s3/aws4_request"));
        assert!(!signed.x_amz_date.is_empty());
        assert!(signed.x_amz_security_token.is_none());
    }

    #[test]
    fn test_sign_with_session_token() {
        let creds = AwsCredentials {
            access_key: "AKID".to_string(),
            secret_key: "SECRET".to_string(),
            session_token: Some("TOKEN123".to_string()),
        };
        let signed = sign_s3_get(
            "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com/node_fills_by_block/hourly/2024-06-15/03",
            &creds,
            "us-east-1",
        );
        assert!(signed.authorization.contains("x-amz-security-token"));
        assert_eq!(signed.x_amz_security_token, Some("TOKEN123".to_string()));
    }

    #[test]
    fn test_unsigned_payload_constant() {
        assert_eq!(UNSIGNED_PAYLOAD, "UNSIGNED-PAYLOAD");
    }
}
