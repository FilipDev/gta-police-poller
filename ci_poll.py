#!/usr/bin/env python3
"""CI poller: one Waze GTA police poll via the RT protocol, pushed to the
live map's ingest endpoint. Runs on GitHub Actions every 15 minutes.

The anonymous Waze account state lives in data/rt_state.json, carried
between runs by the Actions cache (see .github/workflows/poll.yml); on a
cold start waze_rt registers a fresh account. The ingest key comes from
the POLL_KEY repository secret.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

WORKER_URL = "https://gta-police-worker.filip-pauzenberger.workers.dev"
GTA = {"bottom": 43.40, "left": -79.95, "top": 44.15, "right": -78.75}
COLS, ROWS = 7, 6
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0 Safari/537.36")


def tiles(bbox, cols=COLS, rows=ROWS):
    lat_step = (bbox["top"] - bbox["bottom"]) / rows
    lon_step = (bbox["right"] - bbox["left"]) / cols
    out = []
    for r in range(rows):
        for c in range(cols):
            out.append({
                "bottom": round(bbox["bottom"] + r * lat_step, 6),
                "left": round(bbox["left"] + c * lon_step, 6),
                "top": round(bbox["bottom"] + (r + 1) * lat_step, 6),
                "right": round(bbox["left"] + (c + 1) * lon_step, 6),
            })
    return out


def to_records(alerts):
    records, seen = [], set()
    for a in alerts:
        if a.get("type") != "POLICE":
            continue
        loc = a.get("location") or {}
        lat, lon = loc.get("y"), loc.get("x")
        uuid = str(a.get("uuid") or "")
        if lat is None or lon is None or not uuid or uuid in seen:
            continue
        seen.add(uuid)
        pub = a.get("pubMillis")
        records.append({
            "uuid": uuid,
            "subtype": a.get("subtype") or "",
            "lat": lat, "lon": lon,
            "street": a.get("street") or "",
            "city": a.get("city") or "",
            "thumbs_up": a.get("nThumbsUp"),
            "reported_ts": (datetime.fromtimestamp(pub / 1000, tz=timezone.utc)
                            .isoformat().replace("+00:00", "Z") if pub else None),
        })
    return records


def post_ingest(records, removed_count, key):
    payload = json.dumps({"records": records,
                          "removed_signals": removed_count}).encode()
    req = urllib.request.Request(
        f"{WORKER_URL}/__ingest?key={key}", data=payload,
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def map_meta():
    req = urllib.request.Request(f"{WORKER_URL}/api/alerts",
                                 headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return (json.loads(resp.read()) or {}).get("meta") or {}


def main() -> int:
    key = os.environ.get("POLL_KEY", "").strip()
    if not key:
        print("ci_poll: POLL_KEY secret is not set", file=sys.stderr)
        return 1
    import waze_rt
    alerts_raw = removed = None
    last_err = None
    for attempt in range(2):  # Waze answers transient 504 "Retry" at times
        try:
            alerts_raw, removed = waze_rt.fetch_snapshot(tiles(GTA))
            break
        except Exception as e:
            last_err = e
            if attempt == 0:
                import time
                time.sleep(10)
    if alerts_raw is None:
        print(f"ci_poll: Waze poll failed twice: {last_err}", file=sys.stderr)
        return 1
    records = to_records(alerts_raw)
    now = datetime.now(timezone.utc)
    result = None
    try:
        result = post_ingest(records, len(removed), key)
    except Exception as e:
        # The ingest connection sometimes drops after the merge has already
        # landed server-side; verify against the live map before failing.
        print(f"ci_poll: ingest POST error ({e}); verifying via /api/alerts",
              file=sys.stderr)
        import time
        time.sleep(5)
        meta = map_meta()
        last = meta.get("last_poll") or ""
        try:
            last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
            fresh = abs((now - last_dt).total_seconds()) < 240
        except Exception:
            fresh = False
        if fresh and meta.get("police_in_snapshot") == len(records):
            result = {"ok": True, "verified_via": "api_alerts", **meta}
    if not result or not result.get("ok"):
        print(f"ci_poll: ingest failed: {result}", file=sys.stderr)
        return 1
    total = result.get("total_unique")
    with open(os.path.join(HERE, "STATUS.md"), "w") as f:
        f.write("# GTA police poller\n\n"
                f"Last poll: {now.strftime('%Y-%m-%d %H:%M UTC')} — "
                f"{len(records)} police in snapshot, "
                f"{total} total unique reports on the live map.\n\n"
                "Live map: https://gta-police-worker.filip-pauzenberger.workers.dev\n")
    print(json.dumps({"ok": True, "police_in_snapshot": len(records),
                      "new_unique": result.get("new_unique"),
                      "total_unique": total,
                      "removed_signals": len(removed)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
