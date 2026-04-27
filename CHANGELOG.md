# Changelog

## 0.2.0

### Added
- ECIES (asymmetric, version-2) encryption alongside the existing symmetric (v1) mode.
  Wire-compatible with `0g-storage-client@6d39443`.
- `transfer::ecies::derive_ecies_encrypt_key` / `derive_ecies_decrypt_key` —
  HKDF-SHA256 over secp256k1 ECDH, info string `"0g-storage-client/ecies/v1/aes-256"`.
- `EncryptedData::new_ecies(inner, recipient_pub_compressed)` for upload-side wrapping.
- `DownloadContext::with_wallet_private_key(priv_key)` for v2 decryption on downloads.
- `EncryptionHeader::new_ecies` / `size()` / version-dispatching `parse()`.
- `FragmentDecryptConfig` enum (`None` / `Symmetric` / `Ecies`) for `download_fragments`.
- CLI flags: `--encrypt` (upload), `--decrypt` + `--private-key` (download, download_segment).
- Cross-implementation interop test against pinned Go-produced fixtures at
  `tests/fixtures/ecies/`.

### Changed
- **Breaking:** `Downloader::download_fragments` signature changed from
  `encryption_key: Option<&[u8;32]>` to `decrypt_cfg: FragmentDecryptConfig`.
  Migration: replace `enc_key.as_ref()` with
  `match enc_key { Some(k) => FragmentDecryptConfig::Symmetric { key: *k }, None => FragmentDecryptConfig::None }`.
- **Breaking:** `IndexerClient::download_fragments` signature changed in lockstep.
- `EncryptionHeader::to_bytes` now returns `Vec<u8>` (was `[u8; 17]`); v2 headers are 50 bytes.
- `EncryptionHeader` gained a public field `ephemeral_pub: Option<[u8; 33]>`.

### Deprecated
- `ENCRYPTION_HEADER_SIZE` (= 17): use `EncryptionHeader::size()` for the actual size,
  or `ENCRYPTION_HEADER_SIZE_V1` / `ENCRYPTION_HEADER_SIZE_V2` if a specific version is meant.
- `ENCRYPTION_VERSION` (= 1): use `ENCRYPTION_VERSION_V1` or `ENCRYPTION_VERSION_V2` explicitly.

## 0.1.0

Initial release.
