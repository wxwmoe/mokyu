import assert from 'node:assert/strict'
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
console.log(`PASS ${keys.length} messages and interpolation parameters in all three locales`)
