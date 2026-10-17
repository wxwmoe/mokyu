import assert from 'node:assert/strict';
import { readdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  constants, gzipSync, gunzipSync, brotliCompressSync, brotliDecompressSync,
  zstdCompressSync, zstdDecompressSync,
} from 'node:zlib';

const codecs = [
  ['gz', data => gzipSync(data, { level: 6 }), gunzipSync],
  ['br', data => brotliCompressSync(data, { params: {
    [constants.BROTLI_PARAM_QUALITY]: 5, [constants.BROTLI_PARAM_LGWIN]: 20,
  } }), brotliDecompressSync],
  ['zst', data => zstdCompressSync(data, { params: {
    [constants.ZSTD_c_compressionLevel]: 3, [constants.ZSTD_c_windowLog]: 20,
  } }), zstdDecompressSync],
];

async function compress(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await compress(path);
    } else if (/\.(html|js|css|txt|json|webmanifest|svg)$/.test(entry.name)) {
      const data = await readFile(path);
      if (data.length < 1024) continue;
      for (const [extension, encode, decode] of codecs) {
        const encoded = encode(data);
        assert.deepEqual(decode(encoded), data, path);
        if (data.length - encoded.length >= Math.max(128, Math.ceil(data.length / 50))) {
          await writeFile(`${path}.${extension}`, encoded);
        }
      }
    }
  }
}

await compress(fileURLToPath(new URL('../dist', import.meta.url)));
