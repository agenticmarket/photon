"""Python SSE server for the streaming audit.

One process serves every Python stack, selected by name, so no framework gets a
different interpreter, a different event loop, or a different warm-up.

The frame is byte-identical to the Go and Node servers'. The equivalence gate in
the driver enforces it, and it is the whole reason this is one file rather than
three: a byte that drifts here invalidates every table in the report.

Usage: python server.py <framework>   where framework is asgi|starlette|fastapi
"""

import argparse
import asyncio
import os
import sys

EVENT_NAME = "token"
PAYLOAD = " the quick brown fox jumps over the lazy dog"
# Precomputed once, and written as one unit per event. A server that built the
# frame per event would be measuring Python string formatting, which is a
# language cost rather than a framework one.
FRAME = f"event: {EVENT_NAME}\ndata: {PAYLOAD}\n\n".encode()

# photon's Stream commits these, so the framework layer is what is compared
# rather than each server's idea of a streaming response. uvicorn adds the same
# Date and Transfer-Encoding to all of them.
HEADERS = [
    (b"content-type", b"text/event-stream; charset=utf-8"),
    (b"cache-control", b"no-cache, no-transform"),
    (b"connection", b"keep-alive"),
    (b"x-accel-buffering", b"no"),
]

# Clamped so a malformed request cannot make one stream run for hours.
MAX_EVENTS = 1_000_000

delivered = 0


def count_from(query: str) -> int:
    """Extract n from a query string, defaulting to 50.

    Takes the query string itself, not the path: in raw ASGI the query is
    delivered separately in scope["query_string"] and scope["path"] never
    contains it, so parsing a path here would silently return the default for
    every request. That is exactly the bug the equivalence gate exists to catch -
    the server streamed the right bytes, just fifty of them.
    """
    for pair in query.split("&"):
        if pair.startswith("n="):
            try:
                n = int(pair[2:])
            except ValueError:
                return 50
            if n <= 0:
                return 50
            return min(n, MAX_EVENTS)
    return 50


def rss() -> int:
    """Peak RSS of this process, in bytes.

    ru_maxrss is kilobytes on Linux and bytes on macOS; this harness runs on
    Windows, where resource is unavailable, so the working set is read from the
    OS instead. The driver reads the same quantity from every runtime, so
    whatever is measured here is what the report calls peak RSS.
    """
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        import ctypes
        from ctypes import wintypes

        class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = PROCESS_MEMORY_COUNTERS()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
        # GetCurrentProcess returns a pseudo-handle whose value exceeds 32 bits
        # (-1 on Windows), so its restype must be a 64-bit pointer type. With the
        # default restype ctypes truncated it to int and the call raised an
        # OverflowError, which the broad handler below turned into a silent
        # 0 - a memory figure of zero for every Python server in the report.
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        # argtypes must be declared too, not just the restype of the callee. With
        # the default ctypes passes the handle as a 32-bit int and the pseudo
        # handle (-1, 0xFFFFFFFFFFFFFFFF) raises OverflowError before the call is
        # ever made - which is why the figure was silently zero.
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(PROCESS_MEMORY_COUNTERS),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        ok = psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        )
        if not ok:
            raise OSError(ctypes.get_last_error(), "GetProcessMemoryInfo failed")
        # PeakWorkingSetSize is the comparable figure across runtimes: what the
        # OS has committed to the process at its high-water mark.
        return counters.PeakWorkingSetSize
    except Exception as exc:  # noqa: BLE001
        # Reported rather than silently returning 0, so a broken measurement is
        # visible in the output instead of looking like a server that uses no
        # memory at all.
        sys.stderr.write(f"rss measurement failed: {exc!r}\n")
        sys.stderr.flush()
        return 0


# ------------------------------------------------------------------ raw ASGI


async def asgi_app(scope, receive, send):
    """A bare ASGI application: the floor, with no framework at all.

    Included for the same reason net/http is in the Go table. Without it,
    "FastAPI versus photon" compares two whole stacks; with it, the cost of the
    framework layer is visible.
    """
    global delivered
    if scope["type"] != "http":
        return

    path = scope.get("path", "/")
    if path == "/mem":
        body = f"rss={rss()} events={delivered}".encode()
        await send({"type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/plain; charset=utf-8")]})
        await send({"type": "http.response.body", "body": body})
        return

    n = count_from(scope.get("query_string", b"").decode("latin-1"))
    await send({"type": "http.response.start", "status": 200, "headers": HEADERS})
    for _ in range(n):
        await send({"type": "http.response.body", "body": FRAME, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})
    delivered += n


# ---------------------------------------------------------------- starlette


def starlette_app():
    """Starlette, with a StreamingResponse - its idiomatic SSE path."""
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse, StreamingResponse
    from starlette.routing import Route

    async def mem(request):
        return PlainTextResponse(f"rss={rss()} events={delivered}")

    async def stream(request):
        n = count_from(request.url.query)

        async def gen():
            for _ in range(n):
                yield FRAME

        return StreamingResponse(gen(), headers=dict(
            (k.decode(), v.decode()) for k, v in HEADERS))

    return Starlette(routes=[
        Route("/stream", stream),
        Route("/mem", mem),
    ])


# ------------------------------------------------------------------ fastapi


def fastapi_app():
    """FastAPI, which is Starlette plus validation, dependency injection, and
    OpenAPI generation.

    None of that is on the SSE path, which is the point of including it: it
    shows what a full framework costs on a workload the framework does not help
    with.
    """
    from fastapi import FastAPI, Request
    from fastapi.responses import PlainTextResponse, StreamingResponse

    app = FastAPI()

    @app.get("/mem")
    async def mem():
        return PlainTextResponse(f"rss={rss()} events={delivered}")

    @app.get("/stream")
    async def stream(request: Request):
        n = count_from(request.url.query)

        async def gen():
            for _ in range(n):
                yield FRAME

        return StreamingResponse(gen(), headers=dict(
            (k.decode(), v.decode()) for k, v in HEADERS))

    return app


# --------------------------------------------------------------------- main


async def serve(framework: str) -> None:
    """Start uvicorn and print the READY handshake once the socket is bound."""
    import uvicorn

    if framework == "asgi":
        app = asgi_app
    elif framework == "starlette":
        app = starlette_app()
    elif framework == "fastapi":
        app = fastapi_app()
    else:
        sys.stderr.write(f"unknown framework {framework}\n")
        sys.exit(2)

    # Port 0 with a printed address, so the driver never guesses a port or
    # sleeps and hopes the server is up.
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=0,
        log_level="error",
        access_log=False,
        # Loopback only, so this cannot be reached from the network while a
        # benchmark run is in progress.
        lifespan="off",
    )
    server = uvicorn.Server(config)

    # uvicorn does not expose its bound port before serving, so the socket is
    # created here and handed to uvicorn. That is the only way to get a real
    # port-0 handshake rather than a fixed port the driver might collide with.
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(2048)
    sock.setblocking(False)
    port = sock.getsockname()[1]

    serve_task = asyncio.create_task(server.serve(sockets=[sock]))

    # Wait for uvicorn to report the socket is live before announcing it. The
    # budget is generous because importing FastAPI on a cold filesystem is slow
    # on Windows; a driver that gave up early would report a timeout rather than
    # the fact that Python starts slower than Go.
    for _ in range(600):
        if server.started:
            break
        await asyncio.sleep(0.05)
    else:
        sys.stderr.write("uvicorn did not start within 30s\n")
        sys.exit(1)

    sys.stdout.write(f"READY python/{framework} 127.0.0.1:{port}\n")
    sys.stdout.flush()

    await serve_task


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("framework", nargs="?", default="asgi",
                        choices=["asgi", "starlette", "fastapi"])
    args = parser.parse_args()

    # uvloop would make this faster and is a realistic production choice, but it
    # also changes the event loop being measured. The default asyncio loop is
    # used so the figure is CPython plus uvicorn, which is the honest floor.
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    asyncio.run(serve(args.framework))


if __name__ == "__main__":
    main()
