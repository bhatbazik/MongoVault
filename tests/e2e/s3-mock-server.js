import { createHash, randomUUID } from "node:crypto";
import http from "node:http";

function computeEtag(buf) {
  return createHash("md5").update(buf).digest("hex");
}

function normalizeEtag(val) {
  return String(val ?? "")
    .replace(/^W\//i, "")
    .replace(/"/g, "")
    .trim();
}

function escapeXml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function unescapeXml(str) {
  return String(str)
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

function parseObjectLockHeaders(headers) {
  const mode = headers["x-amz-object-lock-mode"]
    ? String(headers["x-amz-object-lock-mode"])
    : undefined;
  const retainRaw = headers["x-amz-object-lock-retain-until-date"]
    ? String(headers["x-amz-object-lock-retain-until-date"])
    : undefined;
  const legalHold = headers["x-amz-object-lock-legal-hold"]
    ? String(headers["x-amz-object-lock-legal-hold"])
    : undefined;

  if (!mode && !retainRaw && !legalHold) return undefined;
  const retainUntilDate = retainRaw ? new Date(retainRaw) : undefined;
  return {
    mode,
    retainUntilDate:
      retainUntilDate && !Number.isNaN(retainUntilDate.getTime()) ? retainUntilDate : undefined,
    legalHold,
  };
}

function parseMetadataHeaders(headers) {
  const metadata = {};
  for (const [name, value] of Object.entries(headers)) {
    if (name.startsWith("x-amz-meta-") && value !== undefined) {
      metadata[name.slice(11)] = Array.isArray(value) ? value.join(",") : String(value);
    }
  }
  return metadata;
}

function buildMetadataHeaders(metadata = {}) {
  const out = {};
  for (const [k, v] of Object.entries(metadata)) {
    out[`x-amz-meta-${k}`] = String(v);
  }
  return out;
}

function isWormProtected(obj, now = new Date()) {
  if (!obj?.objectLock) return false;
  if (obj.objectLock.legalHold === "ON") return true;
  if (obj.objectLock.retainUntilDate && obj.objectLock.retainUntilDate > now) return true;
  return false;
}

function sendXmlError(res, status, code, message, extraHeaders = {}, headOnly = false) {
  const xml = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>${escapeXml(code)}</Code><Message>${escapeXml(message ?? code)}</Message></Error>`;
  res.writeHead(status, {
    "Content-Type": "application/xml",
    "Content-Length": Buffer.byteLength(xml),
    ...extraHeaders,
  });
  res.end(headOnly ? undefined : xml);
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function parseDeleteObjectsKeys(xmlBody) {
  const keys = [];
  const keyRegex = /<Key>([\s\S]*?)<\/Key>/g;
  let match;
  while ((match = keyRegex.exec(xmlBody)) !== null) {
    keys.push(unescapeXml(match[1]));
  }
  const quiet = /<Quiet>\s*true\s*<\/Quiet>/i.test(xmlBody);
  return { keys, quiet };
}

function parseCompleteMultipartParts(xmlBody) {
  const parts = [];
  const partBlockRegex = /<Part>([\s\S]*?)<\/Part>/g;
  let blockMatch;
  while ((blockMatch = partBlockRegex.exec(xmlBody)) !== null) {
    const block = blockMatch[1];
    const numMatch = /<PartNumber>(\d+)<\/PartNumber>/.exec(block);
    const etagMatch = /<ETag>([\s\S]*?)<\/ETag>/.exec(block);
    if (numMatch && etagMatch) {
      parts.push({
        partNumber: Number(numMatch[1]),
        etag: normalizeEtag(unescapeXml(etagMatch[1])),
      });
    }
  }
  return parts;
}

export async function startMockS3Server({
  defaultBucket = "test-bucket",
  listMaxKeys = 1000,
} = {}) {
  const buckets = new Map();
  const uploads = new Map();
  const faults = [];
  const requestLog = [];
  let currentListMaxKeys = listMaxKeys;

  const stats = {
    requests: 0,
    putObjects: 0,
    getObjects: 0,
    headObjects: 0,
    listRequests: 0,
    deleteObjects: 0,
    batchDeleteRequests: 0,
    multipartCreates: 0,
    uploadParts: 0,
    multipartCompletes: 0,
    multipartAborts: 0,
    get activeUploads() {
      return uploads.size;
    },
  };

  function getOrCreateBucket(bucket) {
    let map = buckets.get(bucket);
    if (!map) {
      map = new Map();
      buckets.set(bucket, map);
    }
    return map;
  }

  function matchKeyPattern(pattern, target) {
    if (!pattern) return true;
    if (pattern instanceof RegExp) return pattern.test(target);
    return String(target).includes(String(pattern));
  }

  function resolveOp(req, searchParams, key) {
    if (req.method === "GET" && searchParams.get("list-type") === "2" && key === "") {
      return "ListObjectsV2";
    }
    if (req.method === "POST" && searchParams.has("delete") && key === "") {
      return "DeleteObjects";
    }
    if (req.method === "POST" && searchParams.has("uploads") && key !== "") {
      return "CreateMultipartUpload";
    }
    if (req.method === "PUT" && searchParams.has("partNumber") && searchParams.has("uploadId")) {
      return "UploadPart";
    }
    if (req.method === "POST" && searchParams.has("uploadId")) {
      return "CompleteMultipartUpload";
    }
    if (req.method === "DELETE" && searchParams.has("uploadId")) {
      return "AbortMultipartUpload";
    }
    if (req.method === "PUT") return "PutObject";
    if (req.method === "GET") return "GetObject";
    if (req.method === "HEAD") return "HeadObject";
    if (req.method === "DELETE") return "DeleteObject";
    return "Unknown";
  }

  function consumeMatchingFaults(req, op, key, extraKeys = []) {
    const matched = [];
    for (let i = 0; i < faults.length; i++) {
      const rule = faults[i];
      if (rule.remaining <= 0) continue;
      if (rule.method && rule.method.toUpperCase() !== req.method) continue;
      if (rule.op && rule.op.toLowerCase() !== op.toLowerCase()) continue;
      if (rule.hasIfMatch !== undefined && Boolean(req.headers["if-match"]) !== rule.hasIfMatch) {
        continue;
      }
      if (rule.hasRange !== undefined && Boolean(req.headers.range) !== rule.hasRange) {
        continue;
      }
      if (rule.keyPattern) {
        const candidates = extraKeys.length > 0 ? [key, ...extraKeys] : [key];
        if (!candidates.some((k) => matchKeyPattern(rule.keyPattern, k))) {
          continue;
        }
      }
      rule.remaining--;
      matched.push(rule);
      if (rule.type !== "delay") {
        break;
      }
    }
    for (let i = faults.length - 1; i >= 0; i--) {
      if (faults[i].remaining <= 0) {
        faults.splice(i, 1);
      }
    }
    return matched;
  }

  const server = http.createServer(async (req, res) => {
    stats.requests++;
    try {
      const url = new URL(req.url || "/", "http://127.0.0.1");
      const trimmed = url.pathname.replace(/^\/+/, "");
      const slashIdx = trimmed.indexOf("/");
      const bucket = decodeURIComponent(
        (slashIdx === -1 ? trimmed : trimmed.slice(0, slashIdx)) || defaultBucket,
      );
      const key = slashIdx === -1 ? "" : decodeURIComponent(trimmed.slice(slashIdx + 1));
      const op = resolveOp(req, url.searchParams, key);

      let preReadBody = null;
      let extraKeys = [];
      if (op === "DeleteObjects") {
        preReadBody = await readBody(req);
        extraKeys = parseDeleteObjectsKeys(preReadBody.toString("utf8")).keys;
      }

      const matchedFaults = consumeMatchingFaults(req, op, key, extraKeys);
      for (const f of matchedFaults) {
        if (f.type === "delay" && f.delayMs > 0) {
          await new Promise((r) => setTimeout(r, f.delayMs));
        }
      }

      const terminalFault = matchedFaults.find((f) => f.type !== "delay");
      if (terminalFault?.type === "http-error") {
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: terminalFault.status ?? 500,
        });
        sendXmlError(
          res,
          terminalFault.status ?? 500,
          terminalFault.code ?? "InternalError",
          terminalFault.message ?? terminalFault.code ?? "Injected fault",
          {},
          req.method === "HEAD",
        );
        return;
      }

      const bucketMap = getOrCreateBucket(bucket);

      if (op === "ListObjectsV2") {
        stats.listRequests++;
        const prefix = url.searchParams.get("prefix") || "";
        const reqMaxRaw = url.searchParams.get("max-keys");
        const reqMax = reqMaxRaw !== null ? Number(reqMaxRaw) : NaN;
        const effectiveMaxKeys =
          Number.isFinite(reqMax) && reqMax > 0
            ? Math.min(reqMax, currentListMaxKeys)
            : currentListMaxKeys;

        const contToken = url.searchParams.get("continuation-token");
        const startIndex = contToken
          ? Number(Buffer.from(contToken, "base64").toString("utf8"))
          : 0;

        const matched = [...bucketMap.entries()]
          .filter(([k]) => k.startsWith(prefix))
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

        const page = matched.slice(startIndex, startIndex + effectiveMaxKeys);
        const isTruncated = startIndex + page.length < matched.length;
        const nextToken = isTruncated
          ? Buffer.from(String(startIndex + page.length)).toString("base64")
          : null;

        const contentsXml = page
          .map(
            ([k, obj]) =>
              `<Contents><Key>${escapeXml(k)}</Key><LastModified>${obj.lastModified.toISOString()}</LastModified><ETag>&quot;${escapeXml(obj.etag)}&quot;</ETag><Size>${obj.body.length}</Size></Contents>`,
          )
          .join("");

        const xml =
          `<?xml version="1.0" encoding="UTF-8"?>` +
          `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
          `<Name>${escapeXml(bucket)}</Name>` +
          `<Prefix>${escapeXml(prefix)}</Prefix>` +
          `<KeyCount>${page.length}</KeyCount>` +
          `<MaxKeys>${effectiveMaxKeys}</MaxKeys>` +
          `<IsTruncated>${isTruncated ? "true" : "false"}</IsTruncated>` +
          (nextToken
            ? `<NextContinuationToken>${escapeXml(nextToken)}</NextContinuationToken>`
            : "") +
          contentsXml +
          `</ListBucketResult>`;

        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          "Content-Type": "application/xml",
          "Content-Length": Buffer.byteLength(xml),
        });
        res.end(xml);
        return;
      }

      if (op === "DeleteObjects") {
        stats.batchDeleteRequests++;
        const rawXml = (preReadBody ?? (await readBody(req))).toString("utf8");
        const { keys, quiet } = parseDeleteObjectsKeys(rawXml);
        const deleted = [];
        const errors = [];

        for (const targetKey of keys) {
          const obj = bucketMap.get(targetKey);
          if (
            terminalFault?.type === "delete-key-error" &&
            matchKeyPattern(terminalFault.keyPattern, targetKey)
          ) {
            errors.push({
              key: targetKey,
              code: terminalFault.code ?? "InternalError",
              message: terminalFault.message ?? "Injected delete key error",
            });
          } else if (obj && isWormProtected(obj)) {
            errors.push({
              key: targetKey,
              code: "AccessDenied",
              message: "Access Denied",
            });
          } else {
            bucketMap.delete(targetKey);
            deleted.push(targetKey);
          }
        }

        const deletedXml = quiet
          ? ""
          : deleted.map((k) => `<Deleted><Key>${escapeXml(k)}</Key></Deleted>`).join("");
        const errorsXml = errors
          .map(
            (e) =>
              `<Error><Key>${escapeXml(e.key)}</Key><Code>${escapeXml(e.code)}</Code><Message>${escapeXml(e.message)}</Message></Error>`,
          )
          .join("");

        const xml =
          `<?xml version="1.0" encoding="UTF-8"?>` +
          `<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${deletedXml}${errorsXml}</DeleteResult>`;

        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          "Content-Type": "application/xml",
          "Content-Length": Buffer.byteLength(xml),
        });
        res.end(xml);
        return;
      }

      if (op === "CreateMultipartUpload") {
        stats.multipartCreates++;
        await readBody(req);
        const uploadId = randomUUID();
        uploads.set(uploadId, {
          bucket,
          key,
          contentType: String(req.headers["content-type"] || "application/octet-stream"),
          metadata: parseMetadataHeaders(req.headers),
          objectLock: parseObjectLockHeaders(req.headers),
          parts: new Map(),
        });
        const xml =
          `<?xml version="1.0" encoding="UTF-8"?>` +
          `<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
          `<Bucket>${escapeXml(bucket)}</Bucket>` +
          `<Key>${escapeXml(key)}</Key>` +
          `<UploadId>${escapeXml(uploadId)}</UploadId>` +
          `</InitiateMultipartUploadResult>`;
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          "Content-Type": "application/xml",
          "Content-Length": Buffer.byteLength(xml),
        });
        res.end(xml);
        return;
      }

      if (op === "UploadPart") {
        stats.uploadParts++;
        const uploadId = url.searchParams.get("uploadId");
        const partNumber = Number(url.searchParams.get("partNumber"));
        const body = await readBody(req);
        const upload = uploadId ? uploads.get(uploadId) : undefined;
        if (!upload) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 404,
          });
          sendXmlError(res, 404, "NoSuchUpload", "The specified upload does not exist.");
          return;
        }
        const etag = computeEtag(body);
        upload.parts.set(partNumber, { body, etag });
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, { ETag: `"${etag}"` });
        res.end();
        return;
      }

      if (op === "CompleteMultipartUpload") {
        stats.multipartCompletes++;
        const uploadId = url.searchParams.get("uploadId");
        const upload = uploadId ? uploads.get(uploadId) : undefined;
        const xmlReq = (await readBody(req)).toString("utf8");
        if (!upload) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 404,
          });
          sendXmlError(res, 404, "NoSuchUpload", "The specified upload does not exist.");
          return;
        }
        const requestedParts = parseCompleteMultipartParts(xmlReq);
        requestedParts.sort((a, b) => a.partNumber - b.partNumber);
        const buffers = [];
        for (const p of requestedParts) {
          const storedPart = upload.parts.get(p.partNumber);
          if (!storedPart || storedPart.etag !== p.etag) {
            requestLog.push({
              method: req.method,
              op,
              bucket,
              key,
              headers: { ...req.headers },
              status: 400,
            });
            sendXmlError(res, 400, "InvalidPart", `Invalid part ${p.partNumber} or ETag mismatch`);
            return;
          }
          buffers.push(storedPart.body);
        }
        const finalBody = Buffer.concat(buffers);
        const finalEtag = computeEtag(finalBody);
        bucketMap.set(key, {
          body: finalBody,
          etag: finalEtag,
          contentType: upload.contentType,
          lastModified: new Date(),
          metadata: upload.metadata,
          objectLock: upload.objectLock,
        });
        uploads.delete(uploadId);

        const xml =
          `<?xml version="1.0" encoding="UTF-8"?>` +
          `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
          `<Location>http://127.0.0.1/${escapeXml(bucket)}/${escapeXml(key)}</Location>` +
          `<Bucket>${escapeXml(bucket)}</Bucket>` +
          `<Key>${escapeXml(key)}</Key>` +
          `<ETag>&quot;${escapeXml(finalEtag)}&quot;</ETag>` +
          `</CompleteMultipartUploadResult>`;

        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          "Content-Type": "application/xml",
          "Content-Length": Buffer.byteLength(xml),
        });
        res.end(xml);
        return;
      }

      if (op === "AbortMultipartUpload") {
        const uploadId = url.searchParams.get("uploadId");
        await readBody(req);
        if (uploadId && uploads.delete(uploadId)) {
          stats.multipartAborts++;
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 204,
          });
          res.writeHead(204);
          res.end();
        } else {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 404,
          });
          sendXmlError(res, 404, "NoSuchUpload", "The specified upload does not exist.");
        }
        return;
      }

      if (op === "PutObject") {
        stats.putObjects++;
        const body = await readBody(req);
        const existing = bucketMap.get(key);
        const ifNoneMatch = req.headers["if-none-match"];
        const ifMatch = req.headers["if-match"];

        if (ifNoneMatch === "*" && existing) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 412,
          });
          sendXmlError(res, 412, "PreconditionFailed", "At least one pre-condition failed");
          return;
        }
        if (ifMatch !== undefined) {
          if (!existing || normalizeEtag(ifMatch) !== existing.etag) {
            requestLog.push({
              method: req.method,
              op,
              bucket,
              key,
              headers: { ...req.headers },
              status: 412,
            });
            sendXmlError(res, 412, "PreconditionFailed", "At least one pre-condition failed");
            return;
          }
        }

        const etag = computeEtag(body);
        bucketMap.set(key, {
          body,
          etag,
          contentType: String(req.headers["content-type"] || "application/octet-stream"),
          lastModified: new Date(),
          metadata: parseMetadataHeaders(req.headers),
          objectLock: parseObjectLockHeaders(req.headers),
        });
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, { ETag: `"${etag}"` });
        res.end();
        return;
      }

      if (op === "GetObject") {
        stats.getObjects++;
        const obj = bucketMap.get(key);
        if (!obj) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 404,
          });
          sendXmlError(res, 404, "NoSuchKey", "The specified key does not exist.");
          return;
        }
        const ifMatch = req.headers["if-match"];
        if (ifMatch !== undefined && normalizeEtag(ifMatch) !== obj.etag) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 412,
          });
          sendXmlError(res, 412, "PreconditionFailed", "At least one pre-condition failed");
          return;
        }

        const baseHeaders = {
          "Content-Type": obj.contentType,
          ETag: `"${obj.etag}"`,
          "Last-Modified": obj.lastModified.toUTCString(),
          "Accept-Ranges": "bytes",
          ...buildMetadataHeaders(obj.metadata),
        };

        if (
          terminalFault?.type === "tcp-reset-after-bytes" ||
          terminalFault?.type === "mutate-object-after-bytes"
        ) {
          const splitAt = Math.max(0, Math.min(terminalFault.bytes ?? 0, obj.body.length));
          const partial = obj.body.subarray(0, splitAt);
          if (terminalFault.type === "mutate-object-after-bytes") {
            const mutatedBody = Buffer.isBuffer(terminalFault.newBody)
              ? terminalFault.newBody
              : Buffer.from(terminalFault.newBody ?? "mutated");
            bucketMap.set(key, {
              ...obj,
              body: mutatedBody,
              etag: computeEtag(mutatedBody),
              lastModified: new Date(),
            });
          }
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 200,
            fault: terminalFault.type,
          });
          res.writeHead(200, {
            ...baseHeaders,
            "Content-Length": obj.body.length,
          });
          res.write(partial, () => {
            setImmediate(() => {
              const sock = res.socket || req.socket;
              if (sock && !sock.destroyed) {
                if (typeof sock.resetAndDestroy === "function") sock.resetAndDestroy();
                else sock.destroy();
              }
            });
          });
          return;
        }

        if (terminalFault?.type === "http-200-ignore-range") {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 200,
            fault: "http-200-ignore-range",
          });
          res.writeHead(200, {
            ...baseHeaders,
            "Content-Length": obj.body.length,
          });
          res.end(obj.body);
          return;
        }

        const rangeHeader = req.headers.range;
        if (rangeHeader) {
          const m = /^bytes=(\d+)-(\d*)$/.exec(String(rangeHeader));
          if (!m) {
            requestLog.push({
              method: req.method,
              op,
              bucket,
              key,
              headers: { ...req.headers },
              status: 416,
            });
            res.writeHead(416, { "Content-Range": `bytes */${obj.body.length}` });
            res.end();
            return;
          }
          const start = Number(m[1]);
          const end = m[2] ? Math.min(Number(m[2]), obj.body.length - 1) : obj.body.length - 1;
          if ((obj.body.length > 0 && start >= obj.body.length) || start > end) {
            requestLog.push({
              method: req.method,
              op,
              bucket,
              key,
              headers: { ...req.headers },
              status: 416,
            });
            res.writeHead(416, { "Content-Range": `bytes */${obj.body.length}` });
            res.end();
            return;
          }
          const sub = obj.body.subarray(start, end + 1);
          const contentRange = `bytes ${start}-${end}/${obj.body.length}`;
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 206,
            contentRange,
          });
          res.writeHead(206, {
            ...baseHeaders,
            "Content-Range": contentRange,
            "Content-Length": sub.length,
          });
          res.end(sub);
          return;
        }

        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          ...baseHeaders,
          "Content-Length": obj.body.length,
        });
        res.end(obj.body);
        return;
      }

      if (op === "HeadObject") {
        stats.headObjects++;
        const obj = bucketMap.get(key);
        if (!obj) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 404,
          });
          res.writeHead(404);
          res.end();
          return;
        }
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 200,
        });
        res.writeHead(200, {
          "Content-Type": obj.contentType,
          "Content-Length": obj.body.length,
          "Last-Modified": obj.lastModified.toUTCString(),
          ETag: `"${obj.etag}"`,
          ...buildMetadataHeaders(obj.metadata),
        });
        res.end();
        return;
      }

      if (op === "DeleteObject") {
        stats.deleteObjects++;
        const obj = bucketMap.get(key);
        const ifMatch = req.headers["if-match"];
        if (ifMatch !== undefined) {
          if (!obj || normalizeEtag(ifMatch) !== obj.etag) {
            requestLog.push({
              method: req.method,
              op,
              bucket,
              key,
              headers: { ...req.headers },
              status: 412,
            });
            sendXmlError(res, 412, "PreconditionFailed", "At least one pre-condition failed");
            return;
          }
        }
        if (obj && isWormProtected(obj)) {
          requestLog.push({
            method: req.method,
            op,
            bucket,
            key,
            headers: { ...req.headers },
            status: 403,
          });
          sendXmlError(res, 403, "AccessDenied", "Access Denied");
          return;
        }
        bucketMap.delete(key);
        requestLog.push({
          method: req.method,
          op,
          bucket,
          key,
          headers: { ...req.headers },
          status: 204,
        });
        res.writeHead(204);
        res.end();
        return;
      }

      requestLog.push({
        method: req.method,
        op,
        bucket,
        key,
        headers: { ...req.headers },
        status: 400,
      });
      sendXmlError(res, 400, "InvalidRequest", `Unsupported S3 request: ${req.method} ${req.url}`);
    } catch (err) {
      if (!res.headersSent) {
        if (err instanceof URIError) {
          sendXmlError(res, 400, "InvalidURI", "Malformed URI", {}, req.method === "HEAD");
        } else {
          sendXmlError(
            res,
            500,
            "InternalError",
            err instanceof Error ? err.message : String(err),
            {},
            req.method === "HEAD",
          );
        }
      } else {
        res.destroy();
      }
    }
  });

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const addr = server.address();
  const port = typeof addr === "object" && addr ? addr.port : 0;

  return {
    port,
    endpoint: `http://127.0.0.1:${port}`,
    server,
    buckets,
    uploads,
    stats,
    requestLog,
    addFault(rule) {
      const entry = {
        ...rule,
        remaining: rule.once ? 1 : (rule.count ?? Infinity),
      };
      faults.push(entry);
      return entry;
    },
    clearFaults() {
      faults.length = 0;
    },
    setListMaxKeys(n) {
      currentListMaxKeys = Math.max(1, Number(n) || 1);
    },
    putRaw(bucket, key, bodyOrStr, extra = {}) {
      const bucketMap = getOrCreateBucket(bucket);
      const body = Buffer.isBuffer(bodyOrStr) ? bodyOrStr : Buffer.from(String(bodyOrStr));
      const etag = extra.etag ? normalizeEtag(extra.etag) : computeEtag(body);
      const stored = {
        body,
        etag,
        contentType: extra.contentType ?? "application/octet-stream",
        lastModified: extra.lastModified ? new Date(extra.lastModified) : new Date(),
        metadata: extra.metadata ?? {},
        objectLock: extra.objectLock,
      };
      bucketMap.set(key, stored);
      return stored;
    },
    getRaw(bucket, key) {
      return buckets.get(bucket)?.get(key) ?? null;
    },
    deleteRaw(bucket, key) {
      return buckets.get(bucket)?.delete(key) ?? false;
    },
    reset() {
      buckets.clear();
      uploads.clear();
      faults.length = 0;
      requestLog.length = 0;
      currentListMaxKeys = listMaxKeys;
      stats.requests = 0;
      stats.putObjects = 0;
      stats.getObjects = 0;
      stats.headObjects = 0;
      stats.listRequests = 0;
      stats.deleteObjects = 0;
      stats.batchDeleteRequests = 0;
      stats.multipartCreates = 0;
      stats.uploadParts = 0;
      stats.multipartCompletes = 0;
      stats.multipartAborts = 0;
    },
    async stop() {
      server.closeAllConnections?.();
      await new Promise((resolve) => server.close(() => resolve()));
    },
  };
}
