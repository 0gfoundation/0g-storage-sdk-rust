use aes::cipher::{KeyIvInit, StreamCipher, StreamCipherSeek};
use anyhow::Result;
use rand::Rng;

type Aes256Ctr = ctr::Ctr128BE<aes::Aes256>;

pub const ENCRYPTION_HEADER_SIZE: usize = 17;
pub const ENCRYPTION_VERSION: u8 = 1;

#[derive(Clone)]
pub struct EncryptionHeader {
    pub version: u8,
    pub nonce: [u8; 16],
}

impl Default for EncryptionHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl EncryptionHeader {
    pub fn new() -> Self {
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill(&mut nonce);
        Self {
            version: ENCRYPTION_VERSION,
            nonce,
        }
    }

    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < ENCRYPTION_HEADER_SIZE {
            anyhow::bail!(
                "Data too short for encryption header: {} < {}",
                data.len(),
                ENCRYPTION_HEADER_SIZE
            );
        }
        let version = data[0];
        if version != ENCRYPTION_VERSION {
            anyhow::bail!("Unsupported encryption version: {}", version);
        }
        let mut nonce = [0u8; 16];
        nonce.copy_from_slice(&data[1..17]);
        Ok(Self { version, nonce })
    }

    pub fn to_bytes(&self) -> [u8; ENCRYPTION_HEADER_SIZE] {
        let mut buf = [0u8; ENCRYPTION_HEADER_SIZE];
        buf[0] = self.version;
        buf[1..17].copy_from_slice(&self.nonce);
        buf
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

/// Decrypt a full downloaded file in-place: strip header, decrypt remaining bytes.
/// Returns the decrypted data (without header).
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
