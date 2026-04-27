# ECIES interop fixtures

These binary fixtures are produced by the Go reference client
(`0g-storage-client`) and consumed by the Rust SDK's interop test
(`src/transfer/ecies.rs::tests::decrypts_go_produced_v2_ciphertext`).

If wire-format changes are intentional (e.g. HKDF info string update,
header layout change), regenerate from the Go side and update both
implementations:

```bash
# From a checkout of github.com/0gfoundation/0g-storage-client:
go run /tmp/gen_ecies_fixture.go
cp /tmp/ecies-fixtures/* tests/fixtures/ecies/
```

The generator program (`/tmp/gen_ecies_fixture.go`) is a small main()
that calls `core.DeriveECIESEncryptKey` + `core.CryptAt` with a fixed
recipient key (`0x1111...1111`) for reproducibility.

## Files

- `v2_message.bin` — 110 bytes. Wire-format v2 ciphertext: 50-byte header
  (1B version=2 + 33B compressed secp256k1 ephemeral pubkey + 16B nonce) +
  60 bytes AES-256-CTR ciphertext.
- `v2_plaintext.txt` — Expected decrypted plaintext.
- `recipient_priv.hex` — 32-byte secp256k1 recipient private key (hex,
  no `0x` prefix).
