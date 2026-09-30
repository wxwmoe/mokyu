import { readFile, readdir, writeFile } from 'node:fs/promises'
import path from 'node:path'

const lock = JSON.parse(await readFile('package-lock.json', 'utf8'))
const notices = ['Mokyu frontend third-party notices\n']
for (const [directory, item] of Object.entries(lock.packages)) {
  if (!directory.startsWith('node_modules/') || directory.includes('..') || item.dev) continue
  const pkg = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'))
  const names = (await readdir(directory)).filter(name => /^(license|licence|copying|ofl|notice)(\.|$)/i.test(name)).sort()
  notices.push(`\n${'='.repeat(72)}\n${pkg.name} ${pkg.version}\nLicense: ${pkg.license || item.license || 'See package distribution'}\n`)
  for (const name of names) notices.push(await readFile(path.join(directory, name), 'utf8'))
}
await writeFile('dist/THIRD_PARTY_NOTICES.txt', notices.join('\n'))
