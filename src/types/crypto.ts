import type { Transform } from "node:stream";

export interface EncryptionEnvelope {
  algorithm: string;
  keyId: string;
  wrappedKey: string;
  iv: string;
  chunkSize?: number;
  authTag?: string;
  manifestHmac?: string;
}

export interface Encryptor {
  stream: Transform;
  envelope: () => EncryptionEnvelope;
}
