// Node SSE server for the streaming audit.
//
// One process serves every Node framework, selected by name, so no framework
// gets a different runtime, a different warm-up, or a different port strategy.
//
// The frame is byte-identical to the Go servers'. That is enforced by the
// equivalence test in the driver, and it is the whole reason this file exists
// rather than four separate scripts: a byte that drifts here invalidates every
// table in the report.
//
// Usage: node server.js <framework>   where framework is fastify|express|http
'use strict';

const http = require('http');
const { URL } = require('url');

const EVENT_NAME = 'token';
const PAYLOAD = ' the quick brown fox jumps over the lazy dog';
// Precomputed once. The benchmark measures streaming, not string concatenation:
// a server that rebuilt the frame per event would be measuring template
// literals, which is a language difference rather than a framework one.
const FRAME = `event: ${EVENT_NAME}\ndata: ${PAYLOAD}\n\n`;

// The same header set photon's Stream commits, so the framework layer is what is
// compared rather than each server's idea of a streaming response. Node adds
// its own Date and Transfer-Encoding to all of them.
const HEADERS = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-cache, no-transform',
  Connection: 'keep-alive',
  'X-Accel-Buffering': 'no',
};

/** Clamped so a malformed request cannot make one stream run for hours. */
function count(url) {
  const n = Number.parseInt(url.searchParams.get('n'), 10);
  if (!Number.isFinite(n) || n <= 0) return 50;
  return Math.min(n, 1_000_000);
}

let delivered = 0;

/**
 * Writes the stream to a Node response.
 *
 * `write` returns false when the socket buffer is full, and the correct response
 * is to wait for 'drain' before writing the next event. Ignoring that is how a
 * streaming server silently buffers the whole response in memory, which is the
 * exact failure the audit is measuring the cost of avoiding.
 *
 * The event is `once`, not `on`. An earlier version used `once` but the handler
 * was not removed when the write completed synchronously, so under sustained load
 * listeners accumulated until Node aborted the process with
 * MaxListenersExceededWarning - a leak in the benchmark server, not a property
 * of Node.
 */
function stream(res, n) {
  let i = 0;
  let draining = false;
  let finished = false;

  const finish = () => {
    if (finished) return;
    finished = true;
    res.end();
    delivered += n;
  };

  const pump = () => {
    draining = false;
    while (i < n && !res.writableEnded) {
      if (res.write(FRAME) === false) {
        // Socket buffer full: stop and resume when it drains. Registered with
        // `once` and guarded by `draining`, so at most one listener is ever
        // pending.
        draining = true;
        res.once('drain', pump);
        return;
      }
      i += 1;
    }
    if (i >= n) finish();
  };

  res.once('error', () => { finished = true; });
  res.once('close', () => { finished = true; });
  pump();
}

/** Reports peak RSS and delivered events, read in-process. */
function memHandler(req, res) {
  const rss = process.memoryUsage().rss;
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end(`rss=${rss} events=${delivered}`);
}

function rawHandler(req, res) {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname === '/mem') return memHandler(req, res);
  res.writeHead(200, HEADERS);
  stream(res, count(url));
}

// ------------------------------------------------------------------ express

function expressApp() {
  const express = require('express');
  const app = express();

  // express's own res is a thin wrapper, so streaming through it is the honest
  // figure for a team that has adopted express: the framework's own per-request
  // bookkeeping is included.
  app.get('/stream', (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    res.writeHead(200, HEADERS);
    stream(res, count(url));
  });
  app.get('/mem', memHandler);
  return app;
}

// ----------------------------------------------------------------- fastify

function fastifyApp() {
  const fastify = require('fastify')({ logger: false });

  // Fastify's idiomatic streaming path is reply.raw.write, which bypasses its
  // own serialiser. That is the recommended SSE approach in Fastify's docs, so it
  // is what is measured - and it means this row largely measures node:http. The
  // /stream-payload variant below covers the alternative.
  fastify.get('/stream', async (req, reply) => {
    const url = new URL(req.url, 'http://localhost');
    reply.raw.writeHead(200, HEADERS);
    stream(reply.raw, count(url));
  });

  // The non-idiomatic path: a real Fastify payload stream, which goes through
  // Fastify's own machinery. Slower, and that is the finding: Fastify's value
  // is in serialising objects, and for SSE that machinery is pure overhead.
  fastify.get('/stream-payload', async (req, reply) => {
    const url = new URL(req.url, 'http://localhost');
    const n = count(url);
    reply.raw.writeHead(200, HEADERS);
    for (let i = 0; i < n; i += 1) {
      reply.raw.write(FRAME);
    }
    reply.raw.end();
  });

  fastify.get('/mem', async (req, reply) => {
    const rss = process.memoryUsage().rss;
    reply.header('Content-Type', 'text/plain; charset=utf-8');
    return `rss=${rss} events=${delivered}`;
  });
  return fastify;
}

// --------------------------------------------------------------------- main

async function main() {
  const which = process.argv[2] || 'http';
  let handler;
  let server;

  switch (which) {
    case 'fastify': {
      const app = fastifyApp();
      await app.ready();
      handler = app.routing;
      break;
    }
    case 'express': {
      const app = expressApp();
      handler = app;
      break;
    }
    case 'http':
      handler = rawHandler;
      break;
    default:
      process.stderr.write(`unknown framework ${which}\n`);
      process.exit(2);
  }

  server = http.createServer(handler);
  // Nagle would batch small frames, so a token stream would arrive in bursts and
  // time-to-first-event would measure the kernel's timer rather than the server.
  server.on('connection', (s) => s.setNoDelay(true));

  // Port 0 plus a printed address, so the driver never guesses a port or sleeps
  // and hopes the server is up.
  server.listen(0, '127.0.0.1', () => {
    process.stdout.write(`READY node/${which} ${server.address().address}:${server.address().port}\n`);
  });

  const shutdown = () => {
    server.close(() => process.exit(0));
    // Force-exit if connections are still draining: the driver kills the process
    // anyway, and a hung close would leave a listener behind for the next run.
    setTimeout(() => process.exit(0), 500).unref();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

main().catch((err) => {
  process.stderr.write(`${err && err.stack}\n`);
  process.exit(1);
});
