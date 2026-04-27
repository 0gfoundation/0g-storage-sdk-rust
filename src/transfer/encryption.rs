use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use anyhow::Result;
use rand::Rng;

type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

// --- Header constants ----------------------------------------------------
// v1 = symmetric (AES key supplied out-of-band): 1B version + 16B nonce
// v2 = ECIES (recipient supplies wallet private key): 1B version + 33B compressed
//      ephemeral secp256k1 pubkey + 16B nonce
pub const ENCRYPTION_VERSION_V1: u8 = 1;
pub const ENCRYPTION_VERSION_V2: u8 = 2;
pub const ENCRYPTION_HEADER_SIZE_V1: usize = 17;
pub const ENCRYPTION_HEADER_SIZE_V2: usize = 50;
pub const MAX_ENCRYPTION_HEADER_SIZE: usize = ENCRYPTION_HEADER_SIZE_V2;
pub const EPHEMERAL_PUBKEY_SIZE: usize = 33;

/// Backward-compat alias: callers in dependent crates may still import
/// `ENCRYPTION_HEADER_SIZE` and treat it as the v1 size. New code should
/// call `EncryptionHeader::size()` instead.
#[deprecated(note = "use EncryptionHeader::size() (returns 17 or 50) — this constant is the v1 size only")]
pub const ENCRYPTION_HEADER_SIZE: usize = ENCRYPTION_HEADER_SIZE_V1;
/// Backward-compat alias.
#[deprecated(note = "use ENCRYPTION_VERSION_V1 / ENCRYPTION_VERSION_V2 explicitly")]
pub const ENCRYPTION_VERSION: u8 = ENCRYPTION_VERSION_V1;

#[derive(Clone, Debug)]
pub struct EncryptionHeader {
    pub version: u8,
    pub nonce: [u8; 16],
    /// Compressed secp256k1 ephemeral public key. `None` for v1, `Some(_)` for v2.
    pub ephemeral_pub: Option<[u8; EPHEMERAL_PUBKEY_SIZE]>,
}

impl Default for EncryptionHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionHeader {
    /// New v1 (symmetric) header with a random nonce.
    pub fn new() -> Self {
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill(&mut nonce);
        Self {
            version: ENCRYPTION_VERSION_V1,
            nonce,
            ephemeral_pub: None,
        }
    }

    /// New v2 (ECIES) header carrying the supplied compressed ephemeral pubkey
    /// and a fresh random nonce. The caller is responsible for deriving and
    /// using the matching AES key (see `transfer::ecies::derive_ecies_encrypt_key`).
    pub fn new_ecies(ephemeral_pub: [u8; EPHEMERAL_PUBKEY_SIZE]) -> Self {
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill(&mut nonce);
        Self {
            version: ENCRYPTION_VERSION_V2,
            nonce,
            ephemeral_pub: Some(ephemeral_pub),
        }
    }

    /// Serialized size of this header (17 for v1, 50 for v2).
    pub fn size(&self) -> usize {
        match self.version {
            ENCRYPTION_VERSION_V2 => ENCRYPTION_HEADER_SIZE_V2,
            _ => ENCRYPTION_HEADER_SIZE_V1,
        }
    }

    /// Parse a header by dispatching on the leading version byte.
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            anyhow::bail!("Data too short for encryption header: 0 bytes");
        }
        let version = data[0];
        match version {
            ENCRYPTION_VERSION_V1 => {
                if data.len() < ENCRYPTION_HEADER_SIZE_V1 {
                    anyhow::bail!(
                        "Data too short for v1 encryption header: {} < {}",
                        data.len(),
                        ENCRYPTION_HEADER_SIZE_V1
                    );
                }
                let mut nonce = [0u8; 16];
                nonce.copy_from_slice(&data[1..17]);
                Ok(Self {
                    version,
                    nonce,
                    ephemeral_pub: None,
                })
            }
            ENCRYPTION_VERSION_V2 => {
                if data.len() < ENCRYPTION_HEADER_SIZE_V2 {
                    anyhow::bail!(
                        "Data too short for v2 encryption header: {} < {}",
                        data.len(),
                        ENCRYPTION_HEADER_SIZE_V2
                    );
                }
                let mut eph = [0u8; EPHEMERAL_PUBKEY_SIZE];
                eph.copy_from_slice(&data[1..34]);
                let mut nonce = [0u8; 16];
                nonce.copy_from_slice(&data[34..50]);
                Ok(Self {
                    version,
                    nonce,
                    ephemeral_pub: Some(eph),
                })
            }
            other => anyhow::bail!("Unsupported encryption version: {}", other),
        }
    }

    /// Serialize the header (size depends on version: 17 or 50 bytes).
    ///
    /// # Panics
    /// Panics if `version == ENCRYPTION_VERSION_V2` but `ephemeral_pub` is `None`.
    /// In normal use this state is unreachable: `new_ecies()` always sets the field,
    /// and `parse()` rejects v2 headers that lack the ephemeral pubkey. The panic
    /// only fires if a caller constructs the struct directly with mismatched fields.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self.version {
            ENCRYPTION_VERSION_V2 => {
                let mut buf = vec![0u8; ENCRYPTION_HEADER_SIZE_V2];
                buf[0] = ENCRYPTION_VERSION_V2;
                let eph = self
                    .ephemeral_pub
                    .as_ref()
                    .expect("v2 header without ephemeral_pub");
                buf[1..34].copy_from_slice(eph);
                buf[34..50].copy_from_slice(&self.nonce);
                buf
            }
            _ => {
                let mut buf = vec![0u8; ENCRYPTION_HEADER_SIZE_V1];
                buf[0] = ENCRYPTION_VERSION_V1;
                buf[1..17].copy_from_slice(&self.nonce);
                buf
            }
        }
    }
}

/// Encrypt or decrypt bytes in-place at a given data offset.
/// AES-256-CTR is symmetric: encrypt and decrypt are the same operation.
/// `offset` is the byte offset within the plaintext data stream (not counting the header).
pub fn crypt_at(key: &[u8; 32], nonce: &[u8; 16], offset: u64, data: &mut [u8]) {
    let mut cipher = Aes256Ctr::new(key.into(), nonce.into());
    cipher.seek(offset);
    cipher.apply_keystream(data);
}

/// Decrypt a single fragment from a multi-fragment encrypted file.
///
/// For fragment 0 (`is_first = true`): strips the 17-byte header and decrypts
/// the remaining bytes starting at CTR offset 0.
/// For subsequent fragments: decrypts all bytes using `data_offset` into the
/// plaintext stream (the cumulative count of plaintext bytes written so far).
///
/// Returns `(plaintext, new_data_offset)` where `new_data_offset` should be
/// passed as `data_offset` for the next fragment.
#[allow(deprecated)]
pub fn decrypt_fragment_data(
    key: &[u8; 32],
    header: &EncryptionHeader,
    fragment_data: &[u8],
    is_first: bool,
    data_offset: u64,
) -> Result<(Vec<u8>, u64)> {
    if is_first {
        if fragment_data.len() < ENCRYPTION_HEADER_SIZE {
            anyhow::bail!(
                "First fragment too short for encryption header: {} bytes",
                fragment_data.len()
            );
        }
        let mut plaintext = fragment_data[ENCRYPTION_HEADER_SIZE..].to_vec();
        crypt_at(key, &header.nonce, 0, &mut plaintext);
        let new_offset = plaintext.len() as u64;
        Ok((plaintext, new_offset))
    } else {
        let mut plaintext = fragment_data.to_vec();
        crypt_at(key, &header.nonce, data_offset, &mut plaintext);
        let new_offset = data_offset + plaintext.len() as u64;
        Ok((plaintext, new_offset))
    }
}

/// Decrypt a full downloaded file in-place: strip header, decrypt remaining bytes.
/// Returns the decrypted data (without header).
#[allow(deprecated)]
pub fn decrypt_file(key: &[u8; 32], encrypted: &[u8]) -> Result<Vec<u8>> {
    if encrypted.len() < ENCRYPTION_HEADER_SIZE {
        anyhow::bail!("Encrypted data too short");
    }
    let header = EncryptionHeader::parse(encrypted)?;
    let mut data = encrypted[ENCRYPTION_HEADER_SIZE..].to_vec();
    crypt_at(key, &header.nonce, 0, &mut data);
    Ok(data)
}

/// Decrypt a single downloaded segment.
/// For segment 0: parses header from the first 17 bytes, decrypts the remaining data.
/// For other segments: decrypts using the provided header's nonce with the correct data offset.
///
/// `segment_size` is the standard segment size (e.g. DEFAULT_SEGMENT_SIZE = 256KB).
#[allow(deprecated)]
pub fn decrypt_segment(
    key: &[u8; 32],
    segment_index: u64,
    segment_size: u64,
    segment_data: &[u8],
    header: &EncryptionHeader,
) -> Vec<u8> {
    if segment_index == 0 {
        // First segment: skip header bytes, decrypt the rest starting at data offset 0
        let encrypted = &segment_data[ENCRYPTION_HEADER_SIZE..];
        let mut data = encrypted.to_vec();
        crypt_at(key, &header.nonce, 0, &mut data);
        data
    } else {
        // Other segments: all bytes are encrypted data
        // Data offset = segment_index * segment_size - ENCRYPTION_HEADER_SIZE
        let data_offset = segment_index * segment_size - ENCRYPTION_HEADER_SIZE as u64;
        let mut data = segment_data.to_vec();
        crypt_at(key, &header.nonce, data_offset, &mut data);
        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(deprecated)]
    fn test_header_roundtrip() {
        let header = EncryptionHeader::new();
        let bytes = header.to_bytes();
        let parsed = EncryptionHeader::parse(&bytes).unwrap();
        assert_eq!(parsed.version, ENCRYPTION_VERSION);
        assert_eq!(parsed.nonce, header.nonce);
    }

    #[test]
    fn test_crypt_roundtrip() {
        let key = [0x42u8; 32];
        let nonce = [0x13u8; 16];
        let original = b"hello world encryption test data";
        let mut buf = original.to_vec();

        // Encrypt
        crypt_at(&key, &nonce, 0, &mut buf);
        assert_ne!(&buf, original);

        // Decrypt (same operation for CTR)
        crypt_at(&key, &nonce, 0, &mut buf);
        assert_eq!(&buf, original);
    }

    #[test]
    fn test_crypt_at_offset() {
        let key = [0x42u8; 32];
        let nonce = [0x13u8; 16];
        let original = vec![0u8; 100];

        // Encrypt full
        let mut full = original.clone();
        crypt_at(&key, &nonce, 0, &mut full);

        // Encrypt in two parts at different offsets
        let mut part1 = original[..50].to_vec();
        let mut part2 = original[50..].to_vec();
        crypt_at(&key, &nonce, 0, &mut part1);
        crypt_at(&key, &nonce, 50, &mut part2);

        assert_eq!(&full[..50], &part1);
        assert_eq!(&full[50..], &part2);
    }

    #[test]
    fn test_decrypt_file() {
        let key = [0x42u8; 32];
        let original = b"test data for encryption";

        // Build encrypted file: header + encrypted data
        let header = EncryptionHeader::new();
        let mut encrypted_data = original.to_vec();
        crypt_at(&key, &header.nonce, 0, &mut encrypted_data);

        let mut encrypted_file = header.to_bytes().to_vec();
        encrypted_file.extend_from_slice(&encrypted_data);

        // Decrypt
        let decrypted = decrypt_file(&key, &encrypted_file).unwrap();
        assert_eq!(&decrypted, original);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decrypt_segment_zero() {
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let segment_size = 256 * 1024u64; // 256KB

        // Build segment 0: header + encrypted plaintext
        let plaintext = vec![0xABu8; (segment_size as usize) - ENCRYPTION_HEADER_SIZE];
        let mut encrypted = plaintext.clone();
        crypt_at(&key, &header.nonce, 0, &mut encrypted);

        let mut segment_data = header.to_bytes().to_vec();
        segment_data.extend_from_slice(&encrypted);
        assert_eq!(segment_data.len(), segment_size as usize);

        // decrypt_segment for segment 0 returns plaintext without header
        let decrypted = decrypt_segment(&key, 0, segment_size, &segment_data, &header);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decrypt_segment_nonzero() {
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let segment_size = 256 * 1024u64;

        // Segment 1's data offset is segment_size - HEADER_SIZE
        let data_offset = segment_size - ENCRYPTION_HEADER_SIZE as u64;
        let plaintext = vec![0xCDu8; segment_size as usize];
        let mut encrypted = plaintext.clone();
        crypt_at(&key, &header.nonce, data_offset, &mut encrypted);

        let decrypted = decrypt_segment(&key, 1, segment_size, &encrypted, &header);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decrypt_segment_padded_preserves_header() {
        // Simulates what DownloadContext::download_segment_padded does for segment 0
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let segment_size = 256 * 1024u64;

        let plaintext = vec![0xEFu8; (segment_size as usize) - ENCRYPTION_HEADER_SIZE];
        let mut encrypted = plaintext.clone();
        crypt_at(&key, &header.nonce, 0, &mut encrypted);

        let mut raw_segment = header.to_bytes().to_vec();
        raw_segment.extend_from_slice(&encrypted);

        // Decrypt in-place after header (what download_segment_padded does)
        let mut result = raw_segment.clone();
        crypt_at(
            &key,
            &header.nonce,
            0,
            &mut result[ENCRYPTION_HEADER_SIZE..],
        );

        // Header preserved, data decrypted
        assert_eq!(&result[..ENCRYPTION_HEADER_SIZE], &header.to_bytes());
        assert_eq!(&result[ENCRYPTION_HEADER_SIZE..], &plaintext);
        assert_eq!(result.len(), segment_size as usize);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decrypt_fragment_data_first() {
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let plaintext = b"hello fragment zero data";

        // Build fragment 0: header + encrypted plaintext
        let mut encrypted = plaintext.to_vec();
        crypt_at(&key, &header.nonce, 0, &mut encrypted);
        let mut fragment0 = header.to_bytes().to_vec();
        fragment0.extend_from_slice(&encrypted);

        let (got, new_offset) = decrypt_fragment_data(&key, &header, &fragment0, true, 0).unwrap();
        assert_eq!(got, plaintext);
        assert_eq!(new_offset, plaintext.len() as u64);
    }

    #[test]
    fn test_decrypt_fragment_data_subsequent() {
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();

        let frag0_plain = b"first fragment data.";
        let frag1_plain = b"second fragment data.";
        let data_offset = frag0_plain.len() as u64; // offset after first fragment

        let mut encrypted = frag1_plain.to_vec();
        crypt_at(&key, &header.nonce, data_offset, &mut encrypted);

        let (got, new_offset) =
            decrypt_fragment_data(&key, &header, &encrypted, false, data_offset).unwrap();
        assert_eq!(got, frag1_plain);
        assert_eq!(new_offset, data_offset + frag1_plain.len() as u64);
    }

    #[test]
    fn test_decrypt_fragment_data_multi_fragment_roundtrip() {
        // Encrypt a plaintext, split into 3 fragments, decrypt each, verify concatenation.
        let key = [0xABu8; 32];
        let header = EncryptionHeader::new();

        let plaintext: Vec<u8> = (0u8..=200).collect(); // 201 bytes

        // Build the encrypted file as it would be stored: header + AES stream
        let mut full_encrypted = plaintext.clone();
        crypt_at(&key, &header.nonce, 0, &mut full_encrypted);
        let mut file_bytes = header.to_bytes().to_vec();
        file_bytes.extend_from_slice(&full_encrypted);

        // Split into 3 roughly equal fragments
        let total = file_bytes.len();
        let split1 = total / 3;
        let split2 = 2 * total / 3;
        let frag0 = file_bytes[..split1].to_vec();
        let frag1 = file_bytes[split1..split2].to_vec();
        let frag2 = file_bytes[split2..].to_vec();

        // Decrypt fragment 0
        let (dec0, off0) = decrypt_fragment_data(&key, &header, &frag0, true, 0).unwrap();
        // Decrypt fragment 1
        let (dec1, off1) = decrypt_fragment_data(&key, &header, &frag1, false, off0).unwrap();
        // Decrypt fragment 2
        let (dec2, _) = decrypt_fragment_data(&key, &header, &frag2, false, off1).unwrap();

        let mut result = dec0;
        result.extend(dec1);
        result.extend(dec2);
        assert_eq!(result, plaintext);
    }

    #[test]
    #[allow(deprecated)]
    fn test_decrypt_fragment_data_first_too_short() {
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let short = vec![0u8; ENCRYPTION_HEADER_SIZE - 1];
        assert!(decrypt_fragment_data(&key, &header, &short, true, 0).is_err());
    }

    #[test]
    fn test_v2_header_roundtrip() {
        let eph_pub = [0xAAu8; 33];
        let header = EncryptionHeader::new_ecies(eph_pub);
        assert_eq!(header.version, ENCRYPTION_VERSION_V2);
        assert_eq!(header.size(), ENCRYPTION_HEADER_SIZE_V2);
        assert_eq!(header.ephemeral_pub, Some(eph_pub));

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), ENCRYPTION_HEADER_SIZE_V2);
        assert_eq!(bytes[0], ENCRYPTION_VERSION_V2);
        assert_eq!(&bytes[1..34], &eph_pub);
        assert_eq!(&bytes[34..50], &header.nonce);

        let parsed = EncryptionHeader::parse(&bytes).unwrap();
        assert_eq!(parsed.version, ENCRYPTION_VERSION_V2);
        assert_eq!(parsed.nonce, header.nonce);
        assert_eq!(parsed.ephemeral_pub, Some(eph_pub));
        assert_eq!(parsed.size(), ENCRYPTION_HEADER_SIZE_V2);
    }

    #[test]
    #[allow(deprecated)]
    fn test_v1_header_size_unchanged() {
        let header = EncryptionHeader::new();
        assert_eq!(header.size(), ENCRYPTION_HEADER_SIZE_V1);
        assert_eq!(header.size(), ENCRYPTION_HEADER_SIZE);
        assert!(header.ephemeral_pub.is_none());
    }

    #[test]
    fn test_parse_rejects_v2_short() {
        let mut data = vec![0u8; 49];
        data[0] = ENCRYPTION_VERSION_V2;
        assert!(EncryptionHeader::parse(&data).is_err());
    }

    #[test]
    fn test_parse_rejects_unknown_version() {
        let mut data = vec![0u8; 50];
        data[0] = 99;
        assert!(EncryptionHeader::parse(&data).is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn test_multi_segment_decrypt_matches_full_file() {
        // Encrypt a file spanning 2 segments, decrypt per-segment, verify matches full decrypt
        let key = [0x42u8; 32];
        let header = EncryptionHeader::new();
        let segment_size = 256u64; // Small for testing

        let plaintext = vec![0x77u8; (segment_size as usize) * 2 - ENCRYPTION_HEADER_SIZE];
        let mut full_encrypted = plaintext.clone();
        crypt_at(&key, &header.nonce, 0, &mut full_encrypted);

        // Build encrypted file
        let mut file_data = header.to_bytes().to_vec();
        file_data.extend_from_slice(&full_encrypted);

        // Segment 0: first segment_size bytes of the file
        let seg0_data = &file_data[..segment_size as usize];
        let seg0_decrypted = decrypt_segment(&key, 0, segment_size, seg0_data, &header);

        // Segment 1: remaining bytes
        let seg1_data = &file_data[segment_size as usize..];
        let seg1_decrypted = decrypt_segment(&key, 1, segment_size, seg1_data, &header);

        // Concatenated decrypted segments should equal original plaintext
        let mut combined = seg0_decrypted;
        combined.extend_from_slice(&seg1_decrypted);
        assert_eq!(combined, plaintext);
    }
}
