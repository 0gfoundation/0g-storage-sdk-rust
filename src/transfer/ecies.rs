//! ECIES key derivation matching the Go client (`0g-storage-client/core/ecies.go`).
//!
//! Wire-compatibility:
//! - secp256k1 ECDH; raw 32-byte X-coordinate as IKM
//! - HKDF-SHA256 with empty salt and info = b"0g-storage-client/ecies/v1/aes-256"
//! - Output is a 32-byte AES-256 key
//! - Ephemeral pubkey is the 33-byte SEC1-compressed form

use anyhow::{Context, Result};
use hkdf::Hkdf;
use k256::elliptic_curve::sec1::ToEncodedPoint;
use k256::{ecdh::diffie_hellman, PublicKey, SecretKey};
use rand::rngs::OsRng;
use sha2::Sha256;

pub const EPHEMERAL_PUBKEY_SIZE: usize = 33;
pub const ECIES_HKDF_INFO: &[u8] = b"0g-storage-client/ecies/v1/aes-256";

/// Generate a fresh ephemeral keypair, perform ECDH against `recipient_pub_compressed`
/// (33-byte SEC1 compressed secp256k1 point), derive a 32-byte AES-256 key via HKDF-SHA256,
/// and return `(aes_key, ephemeral_pub_compressed)`.
///
/// Note: callers should zeroize the returned AES key after use.
pub fn derive_ecies_encrypt_key(
    recipient_pub_compressed: &[u8],
) -> Result<([u8; 32], [u8; EPHEMERAL_PUBKEY_SIZE])> {
    let recipient_pub = PublicKey::from_sec1_bytes(recipient_pub_compressed)
        .context("invalid recipient public key")?;

    let ephemeral_priv = SecretKey::random(&mut OsRng);
    let ephemeral_pub = ephemeral_priv.public_key();

    let shared = diffie_hellman(
        ephemeral_priv.to_nonzero_scalar(),
        recipient_pub.as_affine(),
    );
    // raw_secret_bytes() returns &GenericArray<u8, U32>; deref via as_slice().
    let aes_key = hkdf_expand_aes(shared.raw_secret_bytes().as_slice())?;

    let mut compressed = [0u8; EPHEMERAL_PUBKEY_SIZE];
    let encoded = ephemeral_pub.to_encoded_point(true);
    let bytes = encoded.as_bytes();
    debug_assert_eq!(
        bytes.len(),
        EPHEMERAL_PUBKEY_SIZE,
        "k256 invariant: compressed secp256k1 point is 33 bytes"
    );
    compressed.copy_from_slice(bytes);
    Ok((aes_key, compressed))
}

/// Recover the AES-256 key from `recipient_priv` (32-byte secp256k1 scalar)
/// and `ephemeral_pub_compressed` (33-byte SEC1 compressed) read from the file header.
///
/// Note: callers are responsible for zeroizing `recipient_priv` and the returned key after use.
pub fn derive_ecies_decrypt_key(
    recipient_priv: &[u8; 32],
    ephemeral_pub_compressed: &[u8; EPHEMERAL_PUBKEY_SIZE],
) -> Result<[u8; 32]> {
    let priv_key =
        SecretKey::from_slice(recipient_priv).context("invalid recipient private key")?;
    let ephemeral_pub = PublicKey::from_sec1_bytes(ephemeral_pub_compressed)
        .context("invalid ephemeral public key")?;

    let shared = diffie_hellman(priv_key.to_nonzero_scalar(), ephemeral_pub.as_affine());
    hkdf_expand_aes(shared.raw_secret_bytes().as_slice())
}

fn hkdf_expand_aes(ikm: &[u8]) -> Result<[u8; 32]> {
    let hk = Hkdf::<Sha256>::new(None, ikm);
    let mut out = [0u8; 32];
    hk.expand(ECIES_HKDF_INFO, &mut out)
        .map_err(|e| anyhow::anyhow!("hkdf expand failed: {}", e))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::SecretKey;
    use rand::rngs::OsRng;

    #[test]
    fn roundtrip_basic() {
        // Recipient generates a keypair
        let recipient_priv = SecretKey::random(&mut OsRng);
        let recipient_pub = recipient_priv.public_key();
        let recipient_pub_compressed = recipient_pub.to_sec1_bytes();

        // Sender derives encrypt key + ephemeral pub
        let (sender_key, ephemeral_pub) =
            derive_ecies_encrypt_key(&recipient_pub_compressed).unwrap();

        // Recipient recovers the same key
        let mut priv_bytes = [0u8; 32];
        priv_bytes.copy_from_slice(&recipient_priv.to_bytes());
        let recipient_key = derive_ecies_decrypt_key(&priv_bytes, &ephemeral_pub).unwrap();

        assert_eq!(sender_key, recipient_key);
    }

    #[test]
    fn ephemeral_keys_differ_per_call() {
        let recipient_priv = SecretKey::random(&mut OsRng);
        let recipient_pub_compressed = recipient_priv.public_key().to_sec1_bytes();

        let (k1, e1) = derive_ecies_encrypt_key(&recipient_pub_compressed).unwrap();
        let (k2, e2) = derive_ecies_encrypt_key(&recipient_pub_compressed).unwrap();

        assert_ne!(k1, k2, "AES keys must be distinct across calls");
        assert_ne!(e1, e2, "Ephemeral pubkeys must be distinct across calls");
    }

    #[test]
    fn rejects_bad_recipient_pubkey() {
        let bad = [0u8; 33];
        assert!(derive_ecies_encrypt_key(&bad).is_err());
    }

    #[test]
    fn rejects_bad_ephemeral_pubkey() {
        let priv_bytes = [1u8; 32];
        let bad_eph = [0u8; 33];
        assert!(derive_ecies_decrypt_key(&priv_bytes, &bad_eph).is_err());
    }

    #[test]
    fn rejects_zero_private_key() {
        let priv_bytes = [0u8; 32];
        let recipient_priv = SecretKey::random(&mut OsRng);
        let eph_compressed: [u8; 33] = recipient_priv
            .public_key()
            .to_sec1_bytes()
            .as_ref()
            .try_into()
            .unwrap();
        assert!(derive_ecies_decrypt_key(&priv_bytes, &eph_compressed).is_err());
    }

    /// Cross-check HKDF output with a known-answer test (KAT) using a fixed shared secret.
    /// Expected = HKDF-SHA256(salt=None, ikm=zero32, info="0g-storage-client/ecies/v1/aes-256")[..32].
    /// Computed locally with the python snippet below (RFC 5869: when salt=None, HKDF-Extract
    /// uses a HashLen-byte zero string as the HMAC key):
    ///   python3 -c "import hashlib, hmac; \
    ///     salt=bytes(32); ikm=bytes(32); info=b'0g-storage-client/ecies/v1/aes-256'; \
    ///     prk=hmac.new(salt, ikm, hashlib.sha256).digest(); \
    ///     print(hmac.new(prk, info+b'\\x01', hashlib.sha256).digest().hex())"
    /// If this test fails, ECIES_HKDF_INFO is wrong or salt-handling differs from RFC 5869,
    /// and Go interop will break.
    #[test]
    fn hkdf_kat_zero_ikm() {
        let zero_ikm = [0u8; 32];
        let key = hkdf_expand_aes(&zero_ikm).unwrap();
        let expected_hex = "b08fd24fb2c33deba245530a40019fa12cbd995fb67fda35772806b32c78bfea";
        assert_eq!(
            hex::encode(key),
            expected_hex,
            "HKDF KAT mismatch — ECIES_HKDF_INFO or HKDF setup is wrong; Go interop will break"
        );
    }

    /// Cross-implementation interop test: decrypt a Go-produced v2 ECIES message
    /// with the Rust implementation. Pinned fixtures live at
    /// `tests/fixtures/ecies/`. If this test fails the Rust impl is no longer
    /// wire-compatible with Go.
    #[test]
    fn decrypts_go_produced_v2_ciphertext() {
        use crate::transfer::encryption::{crypt_at, EncryptionHeader, ENCRYPTION_VERSION_V2};

        let wire = std::fs::read("tests/fixtures/ecies/v2_message.bin")
            .expect("fixture missing — regenerate via tests/fixtures/ecies/README");
        let expected = std::fs::read("tests/fixtures/ecies/v2_plaintext.txt").unwrap();
        let priv_hex = std::fs::read_to_string("tests/fixtures/ecies/recipient_priv.hex").unwrap();

        let priv_bytes = hex::decode(priv_hex.trim()).unwrap();
        let mut priv32 = [0u8; 32];
        priv32.copy_from_slice(&priv_bytes);

        let header = EncryptionHeader::parse(&wire).unwrap();
        assert_eq!(
            header.version, ENCRYPTION_VERSION_V2,
            "fixture must be v2 ECIES"
        );
        let eph = header.ephemeral_pub.unwrap();

        let aes_key = derive_ecies_decrypt_key(&priv32, &eph).unwrap();
        let header_size = header.size();
        let mut plaintext = wire[header_size..].to_vec();
        crypt_at(&aes_key, &header.nonce, 0, &mut plaintext);

        assert_eq!(
            plaintext, expected,
            "Go-produced v2 ciphertext failed to decrypt with Rust — wire format drift"
        );
    }
}
