// Run with an isolated fault-injection service and AWS SDK for JavaScript v3.
import assert from 'node:assert/strict'
import { randomBytes } from 'node:crypto'
import { readFileSync, writeFileSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import {
  S3Client, PutObjectCommand, GetObjectCommand, HeadObjectCommand,
  CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand,
} from '@aws-sdk/client-s3'
import { NodeHttpHandler } from '@smithy/node-http-handler'

assert.equal(process.env.MOKYU_TEST_ALLOW_STATE_CHANGES, 'isolated-only')
const fixture = JSON.parse(readFileSync(process.argv[2], 'utf8'))
const marker = join(fixture.fault_dir, 'multipart-completing')
const wireStatuses = []
class RecordedHandler extends NodeHttpHandler {
  async handle(request, options) {
    const result = await super.handle(request, options)
    wireStatuses.push(result.response.statusCode)
    return result
  }
}
const options = {
  endpoint: fixture.endpoint, region: 'us-east-1', forcePathStyle: true,
  credentials: { accessKeyId: fixture.access_key, secretAccessKey: fixture.secret_key },
  requestChecksumCalculation: 'WHEN_REQUIRED', responseChecksumValidation: 'WHEN_REQUIRED',
  requestHandler: new RecordedHandler({ connectionTimeout: 5000, socketTimeout: 8000 }),
}
const client = new S3Client({ ...options, maxAttempts: 3 })
const once = new S3Client({ ...options, maxAttempts: 1 })
const body = randomBytes(5 * 1024 * 1024 + 29)
const prefix = 'js-sdk-' + randomBytes(8).toString('hex')
const destination = Key => ({ Bucket: fixture.bucket, Key: prefix + '/' + Key })
const clear = () => { try { unlinkSync(marker) } catch (error) { if (error.code !== 'ENOENT') throw error } }

async function verify(target) {
  const reply = await client.send(new GetObjectCommand(target))
  assert.deepEqual(Buffer.from(await reply.Body.transformToByteArray()), body)
  assert.equal(reply.ContentLength, body.length)
}

async function upload(key) {
  const target = destination(key)
  const { UploadId } = await client.send(new CreateMultipartUploadCommand(target))
  const parts = []
  for (const [index, bytes] of [body.subarray(0, 5 * 1024 * 1024), body.subarray(5 * 1024 * 1024)].entries()) {
    const { ETag } = await client.send(new UploadPartCommand({ ...target, UploadId, PartNumber: index + 1, Body: bytes }))
    parts.push({ ETag, PartNumber: index + 1 })
  }
  return { ...target, UploadId, MultipartUpload: { Parts: parts } }
}

try {
  const target = destination('put')
  await client.send(new PutObjectCommand({ ...target, Body: body }))
  await verify(target)
  assert.equal((await client.send(new HeadObjectCommand(target))).ContentLength, body.length)
  const range = await client.send(new GetObjectCommand({ ...target, Range: 'bytes=17-139' }))
  assert.deepEqual(Buffer.from(await range.Body.transformToByteArray()), body.subarray(17, 140))

  const failed = await upload('late-error')
  writeFileSync(marker, '')
  const failedStart = wireStatuses.length
  await assert.rejects(once.send(new CompleteMultipartUploadCommand(failed)), error => {
    assert.equal(error.name, 'InternalError')
    assert.equal(error.$metadata.attempts, 1)
    return true
  })
  // The SDK can rewrite the embedded error's status to 503 for its retry policy.
  assert.deepEqual(wireStatuses.slice(failedStart), [200])
  clear()
  await client.send(new CompleteMultipartUploadCommand(failed))
  await verify(failed)

  const retried = await upload('retry')
  writeFileSync(marker, '')
  const retryStart = wireStatuses.length
  const release = setTimeout(clear, 15000)
  let result
  try { result = await client.send(new CompleteMultipartUploadCommand(retried)) }
  finally { clearTimeout(release); clear() }
  assert.ok(result.$metadata.attempts >= 2, 'SDK must retry the HTTP 200 Error XML')
  assert.ok(wireStatuses.slice(retryStart).every(status => status === 200))
  assert.ok(result.ETag)
  const replay = await client.send(new CompleteMultipartUploadCommand(retried))
  assert.equal(replay.ETag, result.ETag)
  await verify(retried)
  console.log(JSON.stringify({ result: 'PASS', node: process.version, checks: ['PUT/GET/HEAD/Range', 'two-part Complete',
    '200 Error rejection', '5s heartbeat with 8s socket timeout', 'automatic retry', 'result replay'], attempts: result.$metadata.attempts }))
} finally {
  clear()
  client.destroy()
  once.destroy()
}
