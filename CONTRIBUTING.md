# Contributing to MongoVault

Contributions, bug reports, and pull requests are welcome.

## Development Workflow

1. **Install dependencies:**

   ```bash
   npm install
   ```

2. **Run the test suite:**

   ```bash
   npm test
   ```

3. **Run static checks and build:**
   ```bash
   npm run check
   ```
   `npm run check` runs `tsc --noEmit`, `eslint .`, `prettier --check .`, and `tsc` to ensure strict type safety and consistent formatting before any commit.

## Guidelines

- **Zero Temporary Disk Invariant:** Changes to backup or restore pipelines (`src/backup.ts`, `src/restore.ts`, `src/physical.ts`) must preserve end-to-end Node.js stream backpressure without buffering full archives in memory or on local disk.
- **Cryptographic Security & Key Separation:** Any modification to `src/crypto.ts` or `.manifest.json` must adhere strictly to chunked AEAD (`aes-256-gcm-chunked`) framing and RFC 5869 HKDF sub-key derivation (`deriveWrapKey`, `deriveHmacKey`), accompanied by unit tests in `tests/crypto.test.js`.
- **Tests:** Include unit or pipeline tests in `tests/` for new features or bug fixes.
