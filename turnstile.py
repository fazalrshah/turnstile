"""
Turnstile — a concurrency-capping FIFO queue in front of a single local LLM.

When N agents hit one local model server (MLX / Ollama / vLLM / llama.cpp) at once, they contend for the
same GPU, each slows down, and later requests time out. Turnstile sits in front and admits at most
MAX_CONCURRENCY requests to the model at a time; the rest wait in an orderly FIFO queue with their
connection held open — so they WAIT IN LINE instead of failing.

It's an OpenAI-compatible passthrough proxy (it doesn't care what the upstream is), supports streaming,
and exposes /healthz so you can watch the queue depth.

    all clients ──► Turnstile (:8080) ──► your local LLM (:11434 / :8000 / ...)

READ THIS FIRST (the honest part): you might not need Turnstile at all. Most local servers already
serialize internally, so concurrent-agent timeouts are often a *client-side timeout* problem — the
client gives up before its turn. Try raising your client's request timeout first. Reach for Turnstile
when you specifically need a hard concurrency cap, FIFO fairness, or backpressure/observability.
"""
import os
import asyncio
import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, Response, JSONResponse

UPSTREAM        = os.environ.get("UPSTREAM", "http://localhost:11434")  # your local LLM (OpenAI-compatible)
MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "1"))           # requests allowed at the model at once
UPSTREAM_TIMEOUT= float(os.environ.get("UPSTREAM_TIMEOUT", "600"))      # seconds to wait on the model
PORT            = int(os.environ.get("PORT", "8080"))

app = FastAPI(title="turnstile")
_sem = asyncio.Semaphore(MAX_CONCURRENCY)
_client = httpx.AsyncClient(base_url=UPSTREAM, timeout=httpx.Timeout(UPSTREAM_TIMEOUT))
_running = 0
_waiting = 0


@app.get("/healthz")
async def healthz():
    return {"ok": True, "upstream": UPSTREAM, "max_concurrency": MAX_CONCURRENCY,
            "running": _running, "waiting": _waiting}


def _fwd_headers(request: Request) -> dict:
    drop = {"host", "content-length", "connection", "transfer-encoding"}
    return {k: v for k, v in request.headers.items() if k.lower() not in drop}


@app.get("/{path:path}")
async def proxy_get(path: str, request: Request):
    # cheap reads (e.g. /v1/models) pass straight through, no queue
    try:
        r = await _client.get(f"/{path}", headers=_fwd_headers(request), params=dict(request.query_params))
    except Exception as e:
        return JSONResponse({"error": f"upstream unreachable: {e}"}, status_code=502)
    return Response(content=r.content, status_code=r.status_code,
                    media_type=r.headers.get("content-type", "application/json"))


@app.post("/{path:path}")
async def proxy_post(path: str, request: Request):
    """Queue inference behind the semaphore. The semaphore is held for the FULL request (including the
    streamed response), so the model only ever works on MAX_CONCURRENCY requests; the rest wait FIFO."""
    global _running, _waiting
    body = await request.body()
    headers = _fwd_headers(request)

    _waiting += 1
    await _sem.acquire()
    _waiting -= 1
    _running += 1
    try:
        req = _client.build_request("POST", f"/{path}", content=body, headers=headers)
        r = await _client.send(req, stream=True)
    except Exception as e:
        _running -= 1
        _sem.release()
        return JSONResponse({"error": f"upstream error: {e}"}, status_code=502)

    async def stream_and_release():
        global _running
        try:
            async for chunk in r.aiter_raw():
                yield chunk
        finally:
            await r.aclose()
            _running -= 1
            _sem.release()

    return StreamingResponse(stream_and_release(), status_code=r.status_code,
                             media_type=r.headers.get("content-type", "application/json"))


if __name__ == "__main__":
    print(f"[turnstile] :{PORT} -> {UPSTREAM}  (max_concurrency={MAX_CONCURRENCY})", flush=True)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
