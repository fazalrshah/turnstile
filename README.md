# 🚦 Turnstile — a concurrency queue for a single local LLM

Running multiple agents against **one** local model server (MLX, Ollama, vLLM, llama.cpp)? The moment a
few of them fire at once, they fight over the same GPU, each slows down, and the later ones **time out.**

Turnstile sits in front and lets requests through **one (or N) at a time**, FIFO. Everyone else waits in
an orderly line with their connection held open — so they **wait their turn instead of failing.** It's an
OpenAI-compatible passthrough (streaming supported) and exposes `/healthz` so you can watch the queue.

```
  agent A ┐
  agent B ┼──► Turnstile (:8080) ──► your local LLM    (1 at a time → no GPU thrash)
  agent C ┘        FIFO queue
```

## 🛑 Read this before you install it (the honest part)

**You might not need Turnstile.** Most local servers already serialize requests internally, so
concurrent-agent timeouts are usually a **client-side timeout** problem — the client gives up *before its
turn comes up*, not because the model failed. The agent never knows it's queued; it just sees a slow HTTP
call.

**Try this first:** raise your client/agent's request timeout (e.g. to 10–15 min). Often that alone fixes
it, with zero extra moving parts.

**Reach for Turnstile when you specifically need:**
- a hard **concurrency cap** (protect a memory-tight GPU from running 2 big requests at once),
- **FIFO fairness** (no request gets starved),
- **backpressure + observability** (`/healthz` shows `running` / `waiting`).

We'd rather tell you to skip our tool than sell you complexity you don't need.

## Quickstart

```bash
pip install -r requirements.txt
UPSTREAM=http://localhost:11434 MAX_CONCURRENCY=1 python turnstile.py
# point your clients at http://localhost:8080 instead of the model directly
```

Watch the queue:
```bash
watch -n1 'curl -s localhost:8080/healthz'
# {"running":1,"waiting":4,...}  ← 1 at the model, 4 waiting their turn
```

## Configuration (env)

| Var | Default | Notes |
|---|---|---|
| `UPSTREAM` | `http://localhost:11434` | your local LLM (OpenAI-compatible) |
| `MAX_CONCURRENCY` | `1` | requests allowed at the model at once (start at 1, raise if the GPU has headroom) |
| `UPSTREAM_TIMEOUT` | `600` | seconds Turnstile waits on the model |
| `PORT` | `8080` | Turnstile's port |

## The timeout chain (important)

```
client timeout  ≥  queue wait + inference     ← must be the most generous
   Turnstile UPSTREAM_TIMEOUT  ≥  inference
```
The client's clock starts when it calls Turnstile, so it sees the **whole** wait (queue + inference) —
keep it the largest, or you're back to timeouts. A too-short client timeout also triggers **retries**,
which pile *duplicate* requests into the queue and make things worse.

## Hard-won lesson

The subtle bug: a proxy that holds the connection open doesn't stop timeouts by itself, because the
**client** doesn't know it's queued — it just sees a slow response. The real fix is always a client
timeout long enough to cover the wait. Turnstile adds the *cap + fairness + visibility* on top of that.

## Built by

Built by **[KodeKing](https://www.kodeking.net)** · author **[Fazal Shah](https://www.fazalshah.com)**.
We build local, private, multi-agent AI systems for teams who can't send their data to the cloud.
Issues and PRs welcome.

## License

MIT — see [LICENSE](LICENSE).
