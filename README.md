# gta-police-poller

Polls Waze police reports across the entire GTA (Oakville–Oshawa,
lake shore to the York Region north edge) every 15 minutes via the Waze
RT protocol (the mobile app's protocol — no API key, no cost) and pushes
each snapshot to the live map at
https://gta-police-worker.filip-pauzenberger.workers.dev

Runs on GitHub Actions (`.github/workflows/poll.yml`). The anonymous
Waze account state is carried between runs in the Actions cache; the
map's ingest key is the `POLL_KEY` repository secret. Latest poll
status: [STATUS.md](STATUS.md).
<!-- schedule registration nudge 2026-11-09 -->
