#!/usr/bin/env python3
"""Draft X posts with an LLM and queue them in Buffer. Stdlib only.

Required env:  BUFFER_KEY
Optional env:  BUFFER_CHANNEL_ID (found automatically when the account has one
               X channel), LLM_URL, LLM_KEY, LLM_MODEL (any OpenAI-compatible
               endpoint; defaults to a local Ollama), POSTS_PER_RUN, BRIEF_FILE,
               HISTORY_FILE, DRY_RUN=1
"""
import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

BUFFER_URL = os.environ.get("BUFFER_URL", "https://api.buffer.com")
LLM_URL = os.environ.get("LLM_URL", "http://localhost:11434/v1/chat/completions")
LLM_KEY = os.environ.get("LLM_KEY", "ollama")
LLM_MODEL = os.environ.get("LLM_MODEL", "llama3.1")
COUNT = int(os.environ.get("POSTS_PER_RUN", "2"))
BRIEF = pathlib.Path(os.environ.get("BRIEF_FILE", "brief.txt"))
HISTORY = pathlib.Path(os.environ.get("HISTORY_FILE", "history.jsonl"))
DRY_RUN = os.environ.get("DRY_RUN") == "1"
MAX_LEN = 280


def post_json(url, key, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "User-Agent": "xqueue/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        retry = e.headers.get("Retry-After")
        hint = f" (retry after {retry}s)" if retry else ""
        sys.exit(f"HTTP {e.code} from {url}{hint}: {e.read().decode(errors='replace')[:300]}")


def load_history():
    if not HISTORY.exists():
        return []
    return [json.loads(line)["text"] for line in HISTORY.read_text(encoding="utf-8").splitlines() if line.strip()]


def draft(history):
    recent = "\n".join(f"- {t}" for t in history[-30:]) or "(none yet)"
    system = (
        f"{BRIEF.read_text(encoding='utf-8').strip()}\n\n"
        f"Write {COUNT} new posts for X. Each must be under {MAX_LEN - 10} characters, "
        "stand alone, and contain no links. Do not repeat or closely paraphrase these recent posts:\n"
        f"{recent}\n\n"
        "Reply with only a JSON array of strings."
    )
    out = post_json(LLM_URL, LLM_KEY, {
        "model": LLM_MODEL,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": "Write the posts now."}],
    })
    raw = out["choices"][0]["message"]["content"]
    start, end = raw.find("["), raw.rfind("]")
    if start == -1 or end == -1:
        sys.exit(f"LLM did not return a JSON array: {raw[:300]}")
    return [str(p).strip() for p in json.loads(raw[start:end + 1])]


def buffer(query):
    out = post_json(BUFFER_URL, os.environ["BUFFER_KEY"], {"query": query})
    if out.get("errors"):
        sys.exit(f"Buffer error: {out['errors'][0].get('message', out['errors'])}")
    return out.get("data") or {}


def find_channel():
    """Use BUFFER_CHANNEL_ID if set; otherwise find the account's single X channel."""
    channel = os.environ.get("BUFFER_CHANNEL_ID", "").strip()
    if channel:
        return channel
    found = []
    for org in buffer("{ account { organizations { id } } }")["account"]["organizations"]:
        q = "{ channels(input: { organizationId: %s }) { id service name } }" % json.dumps(org["id"])
        found += buffer(q)["channels"]
    listing = "\n".join(f"  {c['id']}  {c['service']}  {c['name']}" for c in found) or "  (none)"
    x = [c for c in found if str(c["service"]).lower() in ("twitter", "x")]
    if len(x) != 1:
        sys.exit(f"Expected exactly one X channel, found {len(x)}. Set BUFFER_CHANNEL_ID to one of:\n{listing}")
    print(f"Using X channel {x[0]['id']} ({x[0]['name']})")
    return x[0]["id"]


def queue(text, channel):
    # json.dumps gives a valid GraphQL string literal; ensure_ascii=False keeps Arabic and emoji intact.
    literal = json.dumps(text, ensure_ascii=False)
    query = (
        "mutation { createPost(input: { text: %s, channelId: %s, "
        "schedulingType: automatic, mode: addToQueue }) { "
        "... on PostActionSuccess { post { id } } "
        "... on MutationError { message } } }"
    ) % (literal, json.dumps(channel))
    out = post_json(BUFFER_URL, os.environ["BUFFER_KEY"], {"query": query})
    if out.get("errors"):
        return None, out["errors"][0].get("message", "unknown GraphQL error")
    result = (out.get("data") or {}).get("createPost") or {}
    if result.get("post"):
        return result["post"]["id"], None
    return None, result.get("message", "unknown error")


def main():
    channel = find_channel()  # read-only, so it also runs in a dry run to check the Buffer key
    if not BRIEF.exists():
        sys.exit(f"Missing {BRIEF}: write a few lines on topic, audience, tone and language.")
    history = load_history()
    seen = {t.casefold() for t in history}
    queued = 0
    for text in draft(history):
        if not text or len(text) > MAX_LEN or text.casefold() in seen:
            print(f"SKIP  {text[:60]!r}")
            continue
        seen.add(text.casefold())
        if DRY_RUN:
            print(f"DRY   {text}")
            continue
        post_id, err = queue(text, channel)
        if err:
            # Most likely the free plan's queue cap; stop and try again next run.
            sys.exit(f"Buffer rejected the post: {err}")
        with HISTORY.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"id": post_id, "text": text}, ensure_ascii=False) + "\n")
        queued += 1
        print(f"QUEUED {post_id}  {text[:60]}")
    print(f"done: {queued} queued")


if __name__ == "__main__":
    main()
