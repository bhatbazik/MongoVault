import { createHash } from "node:crypto";
import { Transform } from "node:stream";

export class DigestStream extends Transform {
  #hash = createHash("sha256");
  #bytes = 0;

  _transform(chunk, _encoding, callback) {
    this.#hash.update(chunk);
    this.#bytes += chunk.length;
    callback(null, chunk);
  }

  get bytes() {
    return this.#bytes;
  }

  digest() {
    return this.#hash.digest("hex");
  }
}
