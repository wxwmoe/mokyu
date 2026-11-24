import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import en from '../src/locales/i18n.en.js'
import zh from '../src/locales/i18n.zh-CN.js'
import ja from '../src/locales/i18n.ja.js'
const keys = Object.keys(en).sort()
const parameters = value => [...value.matchAll(/\{(\w+)\}/g)].map(match => match[1]).sort()
for (const dictionary of [zh, ja]) {
  assert.deepEqual(Object.keys(dictionary).sort(), keys)
  for (const key of keys) {
    assert.ok(dictionary[key].trim(), key)
    assert.deepEqual(parameters(dictionary[key]), parameters(en[key]), key)
  }
}
const source = fileURLToPath(new URL('../src/', import.meta.url))
for (const file of readdirSync(source, { recursive: true })) {
  if (!/\.(?:vue|ts|js)$/.test(file)) continue
  const contents = readFileSync(join(source, file), 'utf8')
  for (const match of contents.matchAll(/\bt\(\s*(['"])([\w.-]+)\1\s*[,)]/g)) {
    assert.ok(Object.hasOwn(en, match[2]), `${file}: missing translation for ${match[2]}`)
  }
}
console.log(`PASS ${keys.length} messages, interpolation parameters and literal translation calls`)
