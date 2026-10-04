// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
'use strict';

/** Run the pinned Dynalite wire tests on an isolated ExtendDB endpoint.
 *
 * The upstream REMOTE mode supplies fixtures without starting Dynalite. Its
 * one hard-coded AWS hostname is translated BEFORE SigV4 signing and before
 * unsigned requests. Bodies, negative-auth inputs, responses and assertions
 * are never rewritten. Both Node HTTP transports are fenced to this endpoint.
 * See docs/testing-dynalite.md for scope, limitations and reproduction.
 */
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const { execFileSync } = require('node:child_process');
const { parseArgs } = require('node:util');
const { urlToHttpOptions } = require('node:url');
const { AsyncLocalStorage } = require('node:async_hooks');
const domain = require('node:domain');
const activeTest = new AsyncLocalStorage();

const REVISION = 'c5e5b46ef5e51e7d907411c001db7839dd146088';
const INTERNAL_SSL_TEST = 'dynalite connections basic should connect to SSL';

function configuration(env) {
  const endpoint = new URL(env.EXTENDDB_TEST_ENDPOINT || 'missing:');
  if (endpoint.protocol !== 'https:' ||
      !['127.0.0.1', 'localhost', '[::1]'].includes(endpoint.hostname) ||
      endpoint.username || endpoint.password || endpoint.pathname !== '/' ||
      endpoint.search || endpoint.hash) {
    throw new Error('An explicit loopback HTTPS origin is required');
  }
  for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_CA_BUNDLE']) {
    if (!env[key]) throw new Error(`Missing isolated test configuration: ${key}`);
  }
  const region = env.AWS_REGION || 'us-east-1';
  if (!/^[a-z0-9-]+$/.test(region)) throw new Error('Invalid test region');
  return { endpoint, region, ca: fs.readFileSync(env.AWS_CA_BUNDLE) };
}

function routeOptions(input, config) {
  const opts = typeof input === 'string' || input instanceof URL
    ? urlToHttpOptions(new URL(input)) : { ...input };
  if (!opts || opts.socketPath || opts.createConnection || opts.lookup || opts.agent) {
    throw new Error('Custom connection routing is not allowed');
  }
  const { endpoint, region } = config;
  const host = opts.hostname || opts.host;
  const port = String(opts.port || (opts.protocol === 'https:' ? 443 : 80));
  const localHost = endpoint.hostname.replace(/^\[|\]$/g, '');
  const alias = host === `dynamodb.${region}.amazonaws.com` &&
    ['80', '443'].includes(port) && ['http:', 'https:', undefined].includes(opts.protocol);
  const local = [endpoint.hostname, localHost].includes(host) &&
    port === (endpoint.port || '443') && opts.protocol === 'https:';
  if (!alias && !local) throw new Error('Request outside the isolated endpoint');
  return {
    ...opts, host: localHost, hostname: localHost,
    port: Number(endpoint.port || 443), protocol: 'https:',
    service: opts.service || 'dynamodb', region: opts.region || region,
    headers: { ...opts.headers }, ca: config.ca, rejectUnauthorized: true,
  };
}

function installTransport(config, aws4, http, https) {
  const original = { sign: aws4.sign, httpRequest: http.request, httpGet: http.get,
    httpsRequest: https.request, httpsGet: https.get };
  aws4.sign = function (opts, credentials) {
    // Upstream retains opts for retries: the signed authority must remain the
    // local authority on subsequent requests, and noSign inputs bypass this.
    Object.assign(opts, routeOptions(opts, config));
    if (!opts.headers.Host && !opts.headers.host) opts.headers.Host = config.endpoint.host;
    return original.sign.call(this, opts, credentials);
  };
  function request(opts, callback) {
    const routed = routeOptions(opts, config);
    const scope = activeTest.getStore();
    if (scope) scope.active++;
    try {
      const req = original.httpsRequest.call(https, routed, callback);
      if (scope) req.once('close', () => { scope.active--; scope.finish(); });
      return req;
    } catch (error) {
      if (scope) scope.active--;
      throw error;
    }
  }
  function get(opts, callback) {
    const req = request(opts, callback);
    req.end();
    return req;
  }
  http.request = https.request = request;
  http.get = https.get = get;
  return () => {
    aws4.sign = original.sign;
    http.request = original.httpRequest;
    http.get = original.httpGet;
    https.request = original.httpsRequest;
    https.get = original.httpsGet;
  };
}

function scopeCallbackTests(suite, supplementalErrors) {
  // Upstream assertType fans out HTTP callbacks without catching assertions.
  // Mocha normally assigns those uncaught exceptions to whichever test happens
  // to be running NEXT. A per-test domain retains the original assertion and
  // stack, attributes it to its owner, and drains in-flight requests before
  // handing completion to Mocha. Additional errors remain in the report.
  // This changes error plumbing only, never expected values or test bodies.
  suite.eachTest(test => {
    if (!test.fn || !test.fn.length || test.pending) return;
    const original = test.fn;
    test.fn = function (done) {
      const context = this;
      const errors = domain.create();
      let requested = false;
      let completed = false;
      let failure;
      const scope = { active: 0, finish() {
        if (!requested || scope.active || completed) return;
        completed = true;
        setImmediate(() => {
          if (!test.state && !test.timedOut) done(failure);
        });
      } };
      function complete(error) {
        if (requested && !error) error = new Error('Upstream callback completed more than once');
        if (error) {
          if (!failure && !completed) failure = error;
          else supplementalErrors.push({ title: test.fullTitle(), afterCompletion: completed,
            ownerAlreadyFailed: Boolean(failure), message: error.message, stack: error.stack });
        }
        requested = true;
        scope.finish();
      }
      errors.on('error', complete);
      activeTest.run(scope, () => errors.run(() => {
        try { original.call(context, complete); }
        catch (error) { complete(error); }
      }));
    };
  });
}

function verifyCheckout(checkout) {
  const git = (...args) => execFileSync('git', ['-C', checkout, ...args], { encoding: 'utf8' }).trim();
  if (git('rev-parse', 'HEAD') !== REVISION) throw new Error(`Expected Dynalite revision ${REVISION}`);
  // Ignore only untracked dependency installation outputs, never tracked edits.
  if (git('diff', 'HEAD', '--name-only')) throw new Error('The upstream checkout has tracked changes');
}

function skipInternalTests(suite) {
  let skipped = 0;
  suite.eachTest(test => {
    if (test.fullTitle() === INTERNAL_SSL_TEST) {
      // This starts a separate Dynalite server; passing it says nothing about
      // ExtendDB TLS. Keep it visible as pending, not as a success or deletion.
      test.pending = true;
      skipped++;
    }
  });
  return skipped;
}

async function main(argv = process.argv.slice(2)) {
  const { values } = parseArgs({ args: argv, options: {
    checkout: { type: 'string' }, output: { type: 'string' },
    suite: { type: 'string', multiple: true }, grep: { type: 'string' },
  } });
  if (!values.checkout || !values.output) throw new Error('--checkout and --output are required');
  const checkout = fs.realpathSync(values.checkout);
  verifyCheckout(checkout);
  const config = configuration(process.env);
  const requireUpstream = createRequire(path.join(checkout, 'package.json'));
  const Mocha = requireUpstream('mocha');
  requireUpstream('should');
  process.env.REMOTE = '1';
  process.env.SLOW_TESTS = '1';
  delete process.env.AWS_SESSION_TOKEN;
  delete process.env.AWS_SECURITY_TOKEN;
  const restore = installTransport(config, requireUpstream('aws4'), require('node:http'), require('node:https'));
  const output = path.resolve(values.output);
  fs.mkdirSync(path.dirname(output), { recursive: true });
  const events = fs.openSync(output.replace(/\.json$/, '') + '.ndjson', 'w');
  const secrets = [process.env.AWS_ACCESS_KEY_ID, process.env.AWS_SECRET_ACCESS_KEY,
    process.env.EXTENDDB_ADMIN_PASSWORD].filter(Boolean);
  const safeJson = value => JSON.stringify(value, (_key, item) =>
    typeof item === 'string' ? secrets.reduce((s, secret) => s.split(secret).join('[REDACTED]'), item) : item);
  const clean = test => ({ title: test.fullTitle(), file: path.basename(test.file || ''),
    duration: test.duration, error: test.err ? { message: test.err.message, stack: test.err.stack } : undefined });
  const results = { revision: REVISION, slowTests: true, suites: [],
    adaptations: ['loopback HTTPS + SigV4 authority', `pending: ${INTERNAL_SSL_TEST}`],
    passes: [], failures: [], pending: [], supplementalErrors: [] };
  const mocha = new Mocha({ timeout: 30000, reporter: function (runner) {
    const event = (kind, data) => fs.writeSync(events, safeJson([kind, data]) + '\n');
    runner.on('start', () => event('start', { total: runner.total }));
    for (const [kind, bucket] of [['pass', 'passes'], ['fail', 'failures'], ['pending', 'pending']]) {
      runner.on(kind, (test, error) => {
        if (error) test.err = error;
        const entry = clean(test);
        results[bucket].push(entry);
        event(kind, entry);
      });
    }
    runner.on('end', () => {
      results.stats = runner.stats;
      results.valid = results.passes.length + results.failures.length + results.pending.length === runner.total &&
        results.supplementalErrors.every(error => error.ownerAlreadyFailed);
      event('end', runner.stats);
      fs.writeFileSync(output, safeJson(results) + '\n');
      fs.closeSync(events);
      console.log(JSON.stringify(runner.stats));
    });
  } });
  const available = fs.readdirSync(path.join(checkout, 'test')).filter(name => name.endsWith('.js') && name !== 'helpers.js');
  const suites = values.suite ? values.suite.map(name => `${name}.js`) : available;
  for (const name of suites) {
    if (!available.includes(name)) throw new Error(`Unknown suite: ${name}`);
    mocha.addFile(path.join(checkout, 'test', name));
  }
  results.suites = suites;
  if (values.grep) mocha.grep(new RegExp(values.grep));
  try {
    mocha.loadFiles();
    skipInternalTests(mocha.suite);
    scopeCallbackTests(mocha.suite, results.supplementalErrors);
    return await new Promise(resolve => mocha.run(failures => resolve(results.valid ? (failures ? 1 : 0) : 2)));
  } finally {
    restore();
  }
}

module.exports = { REVISION, INTERNAL_SSL_TEST, configuration, routeOptions, installTransport,
  verifyCheckout, skipInternalTests, scopeCallbackTests };
if (require.main === module) {
  // Upstream fire-and-forget cleanup/polling can survive failed assertions.
  // The enclosing run-tikv-tests lifecycle owns final namespace destruction.
  main().then(code => process.exit(code), error => { console.error(error); process.exit(2); });
}
