// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
'use strict';

// Offline contracts for endpoint isolation and transparent wire adaptation.
// Run with node --test tests/test_dynalite_adapter.cjs; no npm install needed.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const adapter = require('../devtools/dynalite_adapter.cjs');
const config = { endpoint: new URL('https://127.0.0.1:18443'), region: 'us-east-1', ca: Buffer.from('test CA') };
const source = { host: 'dynamodb.us-east-1.amazonaws.com', method: 'POST' };

test('configuration requires a credentialed loopback HTTPS origin', t => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'dynalite-adapter-'));
  t.after(() => fs.rmSync(tmp, { recursive: true }));
  const cert = path.join(tmp, 'ca.pem');
  fs.writeFileSync(cert, 'CA');
  const env = { EXTENDDB_TEST_ENDPOINT: config.endpoint.href, AWS_ACCESS_KEY_ID: 'test-key',
    AWS_SECRET_ACCESS_KEY: 'test-secret', AWS_CA_BUNDLE: cert };
  assert.equal(adapter.configuration(env).endpoint.origin, config.endpoint.origin);
  for (const endpoint of ['https://example.com', 'http://localhost:18443',
    'https://127.0.0.1:18443/evil', 'https://127.0.0.1:18443/?x=y',
    'https://user:pw@localhost:18443', 'https://localhost:18443/#fragment']) {
    assert.throws(() => adapter.configuration({ ...env, EXTENDDB_TEST_ENDPOINT: endpoint }));
  }
  for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_CA_BUNDLE']) {
    assert.throws(() => adapter.configuration({ ...env, [key]: '' }), /Missing isolated/);
  }
});

test('routing preserves wire input and does not mutate the caller', () => {
  const input = { ...source, path: '/?X-Amz-Algorithm', noSign: true, body: '{invalid',
    headers: { Authorization: 'AWS4-', 'x-amz-target': 'incorrect' } };
  const snapshot = structuredClone(input);
  const routed = adapter.routeOptions(input, config);
  assert.deepEqual(input, snapshot);
  for (const key of ['body', 'path', 'method', 'noSign', 'headers']) assert.deepEqual(routed[key], input[key]);
  assert.equal(routed.host, '127.0.0.1');
  assert.equal(routed.port, 18443);
  assert.equal(routed.protocol, 'https:');
  assert.equal(routed.rejectUnauthorized, true);
  assert.equal(routed.ca, config.ca);
  assert.deepEqual(adapter.routeOptions(routed, config), routed);
});

test('only the exact upstream alias or isolated destination is accepted', () => {
  for (const overrides of [
    { host: 'dynamodb.us-east-1.amazonaws.com.attacker.test' },
    { host: 'dynamodb.eu-west-1.amazonaws.com' }, { port: 8000 },
    { host: 'localhost', port: 18443, protocol: 'https:' },
    { host: '127.0.0.1', port: 18444, protocol: 'https:' },
    { host: '127.0.0.1', port: 18443, protocol: 'http:' },
    { socketPath: '/tmp/bypass.sock' }, { createConnection: () => {} },
    { agent: {} }, { lookup: () => {} }, { protocol: 'ftp:' },
  ]) assert.throws(() => adapter.routeOptions({ ...source, ...overrides }, config));
  assert.throws(() => adapter.routeOptions('https://example.com/', config));
});

test('IPv6 destination keeps a valid local transport address', () => {
  const ipv6 = { ...config, endpoint: new URL('https://[::1]:18443') };
  const routed = adapter.routeOptions(source, ipv6);
  assert.equal(routed.hostname, '::1');
  assert.equal(adapter.routeOptions(routed, ipv6).port, 18443);
});

test('signing uses the local authority; unsigned authentication failures remain unsigned', () => {
  const transmitted = [];
  let signs = 0;
  const aws4 = { sign(opts) {
    signs++;
    assert.equal(opts.host, '127.0.0.1');
    assert.equal(opts.service, 'dynamodb');
    opts.headers.Authorization = 'signed-locally';
    return opts;
  } };
  const http = { request() { throw new Error('Plain HTTP must not be used'); }, get() {} };
  const response = { statusCode: 418, headers: { 'x-test': 'unaltered' } };
  const https = { request(opts, callback) {
    transmitted.push(opts);
    callback?.(response);
    return { end() {} };
  }, get() {} };
  const before = { sign: aws4.sign, request: http.request, httpsRequest: https.request, get: https.get };
  const restore = adapter.installTransport(config, aws4, http, https);
  try {
    const signed = aws4.sign({ ...source, body: '{}', headers: {} });
    http.request(signed, res => assert.equal(res, response));
    const unsigned = { ...source, headers: { Authorization: 'AWS4-' }, noSign: true };
    http.request(unsigned);
    assert.equal(signs, 1);
    assert.equal(transmitted[0].headers.Authorization, 'signed-locally');
    assert.equal(transmitted[1].headers.Authorization, 'AWS4-');
    assert.throws(() => https.request({ host: 'example.com' }));
    assert.throws(() => http.get('http://example.com'));
    assert.throws(() => https.get('https://example.com'));
    assert.equal(transmitted.length, 2);
  } finally { restore(); }
  assert.equal(aws4.sign, before.sign);
  assert.equal(http.request, before.request);
  assert.equal(https.request, before.httpsRequest);
  assert.equal(https.get, before.get);
});

test('only the in-process Dynalite TLS case is marked pending', () => {
  const cases = [adapter.INTERNAL_SSL_TEST, 'dynalite connections basic should return 404 if a PUT',
    'putItem valid request'].map(name => ({ fullTitle: () => name, pending: false }));
  assert.equal(adapter.skipInternalTests({ eachTest: fn => cases.forEach(fn) }), 1);
  assert.deepEqual(cases.map(item => item.pending), [true, false, false]);
});

test('concurrent callback assertions stay assigned to the originating test', async () => {
  const extras = [];
  const owner = { fullTitle: () => 'owning test', fn(done) {
    setImmediate(() => { throw new Error('first assertion'); });
    setTimeout(() => { throw new Error('second assertion'); }, 10);
  } };
  adapter.scopeCallbackTests({ eachTest: fn => fn(owner) }, extras);
  let completions = 0;
  await new Promise(resolve => owner.fn.call({}, error => {
    completions++;
    assert.equal(error.message, 'first assertion');
    resolve();
  }));
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(completions, 1);
  assert.equal(extras.length, 1);
  assert.equal(extras[0].title, 'owning test');
  assert.equal(extras[0].message, 'second assertion');
  assert.equal(extras[0].ownerAlreadyFailed, true);
});

test('callback success, direct throw and explicit failure retain their outcome', async () => {
  for (const [fn, message] of [
    [done => done(), undefined],
    [done => done(new Error('callback failure')), 'callback failure'],
    [function (done) { throw new Error('direct assertion'); }, 'direct assertion'],
  ]) {
    const owner = { fullTitle: () => 'test', fn };
    adapter.scopeCallbackTests({ eachTest: visit => visit(owner) }, []);
    const actual = await new Promise(resolve => owner.fn.call({}, error => resolve(error?.message)));
    assert.equal(actual, message);
  }
});

test('completion waits for already-started HTTP requests to close', async () => {
  const { EventEmitter } = require('node:events');
  const requests = [];
  const https = { request() { const req = new EventEmitter(); requests.push(req); return req; } };
  const http = {};
  const aws4 = { sign: opts => opts };
  const restore = adapter.installTransport(config, aws4, http, https);
  try {
    let completed = false;
    const owner = { fullTitle: () => 'parallel requests', fn(done) {
      http.request(source);
      http.request(source);
      done(new Error('assertion'));
    } };
    adapter.scopeCallbackTests({ eachTest: visit => visit(owner) }, []);
    const finished = new Promise(resolve => owner.fn.call({}, error => {
      completed = true;
      assert.equal(error.message, 'assertion');
      resolve();
    }));
    requests[0].emit('close');
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(completed, false);
    requests[1].emit('close');
    await finished;
    assert.equal(completed, true);
  } finally { restore(); }
});

test('a repeated successful callback is retained as an unaccounted error', async () => {
  const extras = [];
  const owner = { fullTitle: () => 'duplicate callback', fn(done) { done(); done(); } };
  adapter.scopeCallbackTests({ eachTest: visit => visit(owner) }, extras);
  await new Promise(resolve => owner.fn.call({}, resolve));
  assert.equal(extras.length, 1);
  assert.equal(extras[0].ownerAlreadyFailed, false);
  assert.match(extras[0].message, /more than once/);
});
