// JProxy — Cloudflare Worker port of the original FastAPI app.
// Proxies OpenAI-compatible chat completion requests so JanitorAI (or any
// browser client) can call APIs that would otherwise fail on CORS, with
// retry-on-transient-error logic and optional <think> tag injection for
// reasoning_content.

const JANITOR_ORIGIN = "https://janitorai.com";

const TRANSIENT_STATUS = new Set([429, 500, 502, 503, 504, 529]);
const TRANSIENT_MARKERS = [
  "resourceexhausted",
  "overloaded",
  "temporarily overloaded",
  "too many requests",
];

function corsHeaders(extra = {}) {
  return {
    "Access-Control-Allow-Origin": JANITOR_ORIGIN,
    "Access-Control-Allow-Credentials": "true",
    ...extra,
  };
}

function preflightHeaders() {
  return corsHeaders({
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
    "Access-Control-Max-Age": "600",
  });
}

function isTransient(status, bodyText) {
  if (TRANSIENT_STATUS.has(status)) return true;
  const msg = (bodyText || "").toLowerCase();
  if (TRANSIENT_MARKERS.some((m) => msg.includes(m))) return true;
  for (const code of TRANSIENT_STATUS) {
    if (msg.includes(`"code":${code}`) || msg.includes(`'code': ${code}`)) {
      return true;
    }
  }
  return false;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Reasoning is now forced on regardless of the "reasoning" query param,
// and top_p is pinned to 0.95.
function extraBody(reasoning) {
  if (reasoning === "false") return { chat_template_kwargs: { thinking: true } };
  return { chat_template_kwargs: { thinking: true } };
}

// Calls the upstream OpenAI-compatible endpoint with retry on transient
// errors. Resolves to the successful fetch Response (still has an unread
// streaming body) or throws {status, detail}.
async function callUpstreamWithRetry(targetUrl, key, payload, maxRetries = 5, baseDelay = 1500) {
  let lastStatus = 502;
  let lastDetail = "Unknown upstream error";

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    let response;
    try {
      response = await fetch(targetUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
    } catch (networkErr) {
      lastStatus = 502;
      lastDetail = String(networkErr);
      await sleep(baseDelay * (attempt + 1));
      continue;
    }

    if (response.ok) {
      return response;
    }

    const bodyText = await response.text();
    lastStatus = response.status;
    lastDetail = bodyText;

    if (isTransient(response.status, bodyText)) {
      await sleep(baseDelay * (attempt + 1));
      continue;
    }

    throw { status: response.status, detail: bodyText };
  }

  throw { status: lastStatus, detail: lastDetail };
}

// Transforms an upstream SSE stream: when reasoning_visibility=true, wraps
// reasoning_content deltas in <think>...</think> the same way the Python
// version did, by rewriting each chunk's "content" field.
function buildTransformStream(reasoningVisibility) {
  let buffer = "";
  let isInReasoning = false;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();

  return new TransformStream({
    transform(chunk, controller) {
      buffer += decoder.decode(chunk, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop(); // keep the last partial line for next time

      for (const line of lines) {
        if (!line.startsWith("data: ")) continue;
        const payload = line.slice(6).trim();

        if (payload === "[DONE]") {
          controller.enqueue(encoder.encode("data: [DONE]\n\n"));
          continue;
        }

        let obj;
        try {
          obj = JSON.parse(payload);
        } catch {
          // Not JSON, pass through untouched.
          controller.enqueue(encoder.encode(line + "\n\n"));
          continue;
        }

        const choice = obj.choices && obj.choices[0];
        if (!choice) continue;

        if (reasoningVisibility === "true") {
          const reasoningContent = choice.delta && choice.delta.reasoning_content;
          if (reasoningContent != null) {
            if (!isInReasoning) {
              isInReasoning = true;
              controller.enqueue(
                encoder.encode('data: {"choices":[{"delta":{"content":" <think>"}}]}\n\n')
              );
            }
            choice.delta.content = reasoningContent;
          } else if (isInReasoning) {
            isInReasoning = false;
            controller.enqueue(
              encoder.encode('data: {"choices":[{"delta":{"content":" </think>"}}]}\n\n')
            );
          }
        }

        controller.enqueue(encoder.encode("data: " + JSON.stringify(obj) + "\n\n"));
      }
    },
    flush(controller) {
      controller.enqueue(encoder.encode("data: [DONE]\n\n"));
    },
  });
}

function sseErrorStream(message) {
  const encoder = new TextEncoder();
  const safe = message.replace(/"/g, "'").replace(/\n/g, " ");
  const body =
    `data: {"choices":[{"delta":{"content":"⚠️ Proxy error: ${safe}"}}]}\n\n` +
    `data: [DONE]\n\n`;
  return new Response(encoder.encode(body), {
    status: 200,
    headers: { "Content-Type": "text/event-stream", ...corsHeaders() },
  });
}

async function handleProxy(request) {
  const reqUrl = new URL(request.url);
  const targetBase = reqUrl.searchParams.get("url");
  const reasoning = reqUrl.searchParams.get("reasoning") || "";
  const reasoningVisibility = reqUrl.searchParams.get("reasoning_visibility") || "";

  if (!targetBase) {
    return new Response(JSON.stringify({ detail: "Missing 'url' query parameter" }), {
      status: 400,
      headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }

  const authHeader = request.headers.get("Authorization") || "";
  const key = authHeader.replace("Bearer ", "");

  let data;
  try {
    data = await request.json();
  } catch {
    return new Response(JSON.stringify({ detail: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }

  const payload = {
    model: data.model,
    messages: data.messages,
    temperature: data.temperature,
    top_p: 0.95,
    stream: data.stream,
    ...(data.tools ? { tools: data.tools } : {}),
    ...(data.tool_choice ? { tool_choice: data.tool_choice } : {}),
    ...extraBody(reasoning),
  };

  let cleanBase = targetBase.replace(/\/+$/, "");
  // Always normalize down to the .../v1 base, no matter what the caller
  // appended after it (e.g. /chat/completions, trailing slashes, etc).
  const v1Match = cleanBase.match(/^(.*\/v1)(\/.*)?$/);
  if (v1Match) {
    cleanBase = v1Match[1];
  }
  const targetUrl = `${cleanBase}/chat/completions`;

  let upstream;
  try {
    upstream = await callUpstreamWithRetry(targetUrl, key, payload);
  } catch (err) {
    const detail = err && err.detail ? err.detail : String(err);
    if (data.stream) {
      return sseErrorStream(detail);
    }
    return new Response(JSON.stringify({ detail }), {
      status: (err && err.status) || 502,
      headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }

  if (!data.stream) {
    // Non-streaming: just pass the JSON straight through.
    const text = await upstream.text();
    return new Response(text, {
      status: upstream.status,
      headers: { "Content-Type": "application/json", ...corsHeaders() },
    });
  }

  const transformed = upstream.body.pipeThrough(buildTransformStream(reasoningVisibility));

  return new Response(transformed, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      ...corsHeaders(),
    },
  });
}

async function handleBlank(request) {
  const reqUrl = new URL(request.url);
  const text = reqUrl.searchParams.get("text") || "placeholder";
  const encoder = new TextEncoder();
  const body =
    `data: {"choices":[{"delta":{"content":"${text}"}}]}\n\n` + `data: [DONE]\n\n`;
  return new Response(encoder.encode(body), {
    status: 200,
    headers: { "Content-Type": "text/event-stream", ...corsHeaders() },
  });
}

const ROOT_HTML = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>JProxy</title></head>
<body>
<h1>JProxy</h1>
<p>Proxy server for JanitorAI to bypass CORS on OpenAI-compatible APIs.</p>
<p>POST to <code>/proxy?url=&lt;api base url&gt;</code> with an
<code>Authorization: Bearer &lt;key&gt;</code> header and an OpenAI-style
chat completions JSON body.</p>
</body>
</html>`;

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const { pathname } = url;
    const method = request.method;

    if (pathname === "/" && method === "GET") {
      return new Response(ROOT_HTML, {
        headers: { "Content-Type": "text/html; charset=utf-8", "Access-Control-Allow-Origin": "*" },
      });
    }

    if (pathname === "/proxy") {
      if (method === "GET") {
        return Response.redirect(url.origin + "/", 302);
      }
      if (method === "OPTIONS") {
        return new Response(null, { headers: preflightHeaders() });
      }
      if (method === "POST") {
        return handleProxy(request);
      }
    }

    if (pathname === "/proxy/blank") {
      if (method === "OPTIONS") {
        return new Response(null, { headers: preflightHeaders() });
      }
      if (method === "POST") {
        return handleBlank(request);
      }
    }

    return new Response("Not found", { status: 404, headers: corsHeaders() });
  },
};
