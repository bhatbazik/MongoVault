import { createHash, type Hash } from "node:crypto";
import { Transform, type TransformCallback } from "node:stream";

export class DigestStream extends Transform {
  #hash: Hash = createHash("sha256");
  #bytes: number = 0;
  #finalDigest: string | null = null;

  override _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    this.#hash.update(chunk);
    this.#bytes += chunk.length;
    callback(null, chunk);
  }

  get bytes(): number {
    return this.#bytes;
  }

  digest(): string {
    if (this.#finalDigest === null) {
      this.#finalDigest = this.#hash.digest("hex");
    }
    return this.#finalDigest;
  }
}
