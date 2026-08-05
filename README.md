# JProxy — Cloudflare Worker

A port of the FastAPI JProxy app to a Cloudflare Worker. No Python runtime,
no cold starts on a sleeping container, no build command to misconfigure —
Workers run at the edge and don't sleep.

## Deploy

1. Install Wrangler (Cloudflare's CLI) if you don't have it:
   ```
   npm install -g wrangler
   ```

2. Log in:
   ```
   wrangler login
   ```

3. From this folder, deploy:
   ```
   wrangler deploy
   ```

That's it — no `requirements.txt`, no build command, no dependencies to
install. Wrangler pushes `worker.js` straight to Cloudflare's edge network.

Your worker will be live at:
```
https://jproxy.<your-subdomain>.workers.dev
```

## Usage (same as before)

Point JanitorAI's Proxy URL at:
```
https://jproxy.<your-subdomain>.workers.dev/proxy?url=https://integrate.api.nvidia.com/v1
```

Optional query params, same as the original:
- `&reasoning=force` or `&reasoning=true` — force reasoning mode on
- `&reasoning=false` — force reasoning mode off
- `&reasoning_visibility=true` — wrap reasoning output in `<think>` tags

## What changed vs. the FastAPI version

- Rewritten in JavaScript to run on Cloudflare's Workers runtime (Python/
  FastAPI/uvicorn can't run there — Workers don't support ASGI servers).
- Same retry-on-transient-error behavior (429/500/502/503/504/529 and a
  few text markers), same backoff schedule (1.5s × attempt, 5 attempts).
- Same SSE transform for `reasoning_content` → `<think>` tag wrapping.
- `/proxy/blank` test endpoint carried over as-is.
- The root page (`/`) is now a minimal built-in HTML page instead of
  reading `index.html` from disk — Workers don't have a filesystem. If you
  want your original documentation page back, paste its HTML into the
  `ROOT_HTML` constant in `worker.js`.

## Notes

- No `requirements.txt` / pip install step — Workers have no build step
  for a single-file JS worker like this.
- Free tier: 100,000 requests/day, no sleep-on-idle, no cold starts.
