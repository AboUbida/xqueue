# xqueue

Automated Arabic news account on X (@OPwrbn). A stdlib-only Python script reads
source accounts, rewrites their news in Arabic with an LLM, and hands the posts to
Buffer, which publishes to X. Runs on GitHub Actions in `AboUbida/xqueue`.

## Files
- `xqueue.py` — the whole pipeline. Two modes:
  - queue mode (default): tops the Buffer queue up to `QUEUE_TARGET` (6) posts.
  - live mode (`LIVE=1`): publishes a rewrite immediately when a source account
    posts news; one per run, ≥20 min apart, ≤12/day.
- `brief.txt` — the voice and rules given to the model (Arabic only, news only,
  neutral on clubs, label rumours as reports, skip politics/religion/crime).
- `xsources.txt` — X accounts to read (`@handle | category`).
- `sources.txt` — optional RSS feeds (same `Name | URL | category` format). Removed
  for now; restore with `git checkout 84b74a8~1 -- sources.txt`.
- `cards.py`, `assets/` — drawn headline cards (Pillow + libraqm). Used first by
  default (own visuals count as original content on X); the source's photo is the
  fallback. `OWN_CARDS=0` reverses that. Needs `MEDIA_REPO`/`MEDIA_TOKEN` to host
  the PNGs; without them posts use the source's photo, as before.
- `history.jsonl` — every post queued (text, source link, image, time); dedupes
  across runs. Committed by the workflow. Deleting a post in Buffer does not
  remove it here, which is intended.
- `xstate.json` — last post id seen per X account; committed by the workflow.
- `stats.json` — impressions/engagement per own post, written by stats mode
  (`STATS=1`, workflow `xqueue-stats.yml`, weekly); prints averages by source,
  category, image kind, live/queue and hour.
- `.github/workflows/xqueue.yml` — queue mode, cron 09:17/13:17/17:17/21:17 Riyadh.
- `.github/workflows/xqueue-live.yml` — live mode, cron every 10 min.
- `.github/workflows/xqueue-stats.yml` — stats mode, Mondays.

## Services and secrets (GitHub repo secrets / variables)
- `BUFFER_KEY` (secret) — Buffer GraphQL API, https://api.buffer.com. Free plan:
  10 queued posts max, 250 API requests/day. X channel id 6aca793d6a5c39ccb67443d1
  is found automatically.
- `LLM_KEY` (secret) — Gemini API via its OpenAI-compatible endpoint; model
  `gemini-3.8-flash`. Any OpenAI-compatible endpoint works via `LLM_URL`/`LLM_MODEL`.
  (GitHub Models was retired 2026-07-30; do not use it.)
- `X_BEARER_TOKEN` (secret) — X API app-only token, pay-per-use (~$0.005/read).
  Reads only posts newer than `xstate.json`, excluding replies and retweets.
- `REVIEW=1` (var) — save posts as Buffer drafts for a person to edit and approve
  instead of queuing/publishing. Drafts count toward `QUEUE_TARGET`.
- Optional vars: `QUEUE_TARGET`, `OWN_CARDS`, `STATS_DAYS`, `STATS_MAX`, `LIVE_MAX_PER_DAY`, `LIVE_GAP_MINUTES`,
  `ARTICLE_IMAGES=0` (no photos), `CREDIT_PREFIX` (adds a source line).

## Monetization context
X replaced Creator Revenue Sharing with Original Content Rewards (2026-09-08):
Premium, 500 verified followers, 500K verified home-timeline impressions in 90
days, and it excludes automated, aggregated and copied content. Hence the push
toward review mode, own cards and value-adding posts rather than plain rewrites.

## Known issues
- GitHub's cron scheduler has never fired for this repo (manual runs work). The
  workaround is an external timer (cron-job.org) POSTing to the
  `actions/workflows/<file>/dispatches` API with a fine-grained PAT
  (Actions: read/write), body `{"ref":"main","inputs":{"dry_run":"false"}}`.
- Private repo Actions quota is 2,000 min/month; every-10-min runs exceed it.
  Make the repo public or run less often.
- Photos are the publishers'/accounts' own images (copyright risk accepted by owner).
- Buffer may refuse some image hosts; the script falls back to a card, then text.

## Working here
- Test locally with a dry run: `DRY_RUN=1 python3 xqueue.py` (needs the env vars).
- Trigger a real run: `gh workflow run xqueue -R AboUbida/xqueue -f dry_run=false`.
- Keep the script stdlib-only; Pillow is optional and only for cards.
- Never put keys in files; they live in GitHub secrets only.
