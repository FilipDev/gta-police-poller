#!/usr/bin/env python3
"""
Waze RT-protocol client (workaround for the blocked live-map georss feed).

Faithful Python port of the fetch path of SABRE Plus (nicglazkov/
highway-radar-sabre-plus, itself ported from wzsabre 2.2), which is the
community implementation of the protocol the Waze mobile app itself uses:

  - Host (North America): rt-xlb-am.waze.com
  - register: POST /rtserver/distrib/static   (ProtoBase64 lines: ClientInfo + Register)
      -> protobuf Batch{ Element{ register_successful{ username, password } } }
      Waze itself issues a fresh *anonymous* account; no personal credentials.
  - login:    POST /rtserver/distrib/login    (ClientInfo + LoginRequest + ReportAdsSettings)
      -> Batch{ Element{ login_response{ login_success{ server_session_id, secret_key } } } }
  - command:  POST /rtserver/distrib/command  (plain-text lines: SeeMe / SetMood /
      Location / MapDisplayed), uid header = base64(UID{ id, secret_key })
      -> Batch{ Element{ add_alert_action{ realtime_alert{...} } } }

Requests that are protobuf are framed as "ProtoBase64," + base64(Batch bytes),
one Element per line, newline-joined — exactly as the app/SABRE send them.
Responses are protobuf Batch messages, decoded here with a small generic
wire-format parser (field numbers from waze.proto, bundled with SABRE Plus).

Session behaviour mirrors the reference implementation:
  - one anonymous account, registered once and persisted (data/rt_state.json),
    because Waze caps anonymous registrations per day (reference cap: 10/day);
  - a fresh login per poll (sessions idle out after ~100 s anyway);
  - within one login, the server sends each alert ONCE, so a poll = handshake
    + one MapDisplayed query per tile, merged — that merged set is the full
    current snapshot for the area.

No third-party API key, no browser, no reCAPTCHA surface: these are the app's
own RT servers, reached the way the app reaches them.
"""
import base64, json, random, time, urllib.request, urllib.error, uuid as uuidlib
from http.cookiejar import CookieJar
from pathlib import Path

BASE = Path(__file__).resolve().parent
STATE_PATH = BASE / "data" / "rt_state.json"

RT_HOST = "rt-xlb-am.waze.com"          # region "na"
PATH_LOGIN = "/rtserver/distrib/login"
PATH_STATIC = "/rtserver/distrib/static"
PATH_COMMAND = "/rtserver/distrib/command"
PROTOCOL_VERSION = 234
APP_VERSION = "5.17.1.0"

DEVICE_POOL = [
    ("samsung", "SM-S928B", "16-SDK36", 1440, 3088),
    ("samsung", "SM-A546B", "15-SDK35", 1080, 2340),
    ("Google", "Pixel 9 Pro", "16-SDK36", 1280, 2856),
    ("Google", "Pixel 8", "15-SDK35", 1080, 2400),
    ("OnePlus", "CPH2451", "15-SDK35", 1240, 2772),
    ("Xiaomi", "2201117TG", "14-SDK34", 1220, 2712),
    ("motorola", "moto g84", "15-SDK35", 1080, 2400),
    ("Nothing", "A065", "15-SDK35", 1080, 2412),
]

ALERT_TYPES = {0: "UNKNOWN", 1: "CHIT_CHAT", 2: "POLICE", 3: "ACCIDENT", 4: "JAM",
               5: "TRAFFIC_INFO", 6: "HAZARD", 7: "MISC", 8: "CONSTRUCTION", 9: "PARKING",
               10: "DYNAMIC", 11: "CAMERA", 13: "ROAD_CLOSED", 14: "SYSTEM_ROAD_CLOSED",
               15: "UNKNOWN_ALERT", 16: "SOS", 17: "CRASH_PRONE", 19: "TURN_CLOSED",
               100: "NEW_BAD_WEATHER", 101: "NEW_LANE_CLOSED", 103: "PERMANENT_HAZARD",
               104: "PERSONAL_SAFETY"}
ALERT_SUBTYPES = {0: "", 201: "POLICE_VISIBLE", 202: "POLICE_HIDING",
                  203: "POLICE_WITH_MOBILE_CAMERA", 301: "ACCIDENT_MINOR", 302: "ACCIDENT_MAJOR"}

# ── protobuf wire primitives ────────────────────────────────────────────────

def _varint(n):
    n &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def _tag(field, wire):
    return _varint((field << 3) | wire)

def f_var(field, value):   # int32/int64/enum/bool
    return _tag(field, 0) + _varint(value)

def f_bytes(field, data):  # embedded message
    if isinstance(data, str):
        data = data.encode("utf-8")
    return _tag(field, 2) + _varint(len(data)) + data

def f_str(field, s):
    return f_bytes(field, s) if s else b""

def parse_fields(buf):
    """Generic wire parse: {field_number: [(wire_type, value), ...]}.
    Varints -> int, length-delimited -> bytes, 32/64-bit -> raw bytes."""
    fields, i, n = {}, 0, len(buf)
    while i < n:
        tag, i = _read_varint(buf, i)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            val, i = _read_varint(buf, i)
        elif wire == 2:
            ln, i = _read_varint(buf, i)
            val, i = buf[i:i + ln], i + ln
        elif wire == 5:
            val, i = buf[i:i + 4], i + 4
        elif wire == 1:
            val, i = buf[i:i + 8], i + 8
        else:
            break  # unknown wire type: stop, keep what we have
        fields.setdefault(field, []).append((wire, val))
    return fields

def _read_varint(buf, i):
    shift, result = 0, 0
    while True:
        b = buf[i]; i += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, i
        shift += 7

def get_msg(fields, field):
    v = fields.get(field)
    return parse_fields(v[0][1]) if v and v[0][0] == 2 else {}

def get_var(fields, field, default=0):
    v = fields.get(field)
    return v[0][1] if v and v[0][0] == 0 else default

def get_str(fields, field, default=""):
    v = fields.get(field)
    return v[0][1].decode("utf-8", "replace") if v and v[0][0] == 2 else default

def get_all_msg(fields, field):
    return [parse_fields(v) for (w, v) in fields.get(field, []) if w == 2]

def to_int32(v):
    v &= 0xFFFFFFFF
    return v - 0x100000000 if v >= 0x80000000 else v

# ── request message builders (field numbers from waze.proto) ────────────────

def coordinate(lon, lat):
    return f_var(101, int(round(lon * 1_000_000))) + f_var(102, int(round(lat * 1_000_000)))

def client_info(device, lon, lat):
    # Position jitter up to +/-500 m, as the official client does.
    lon += ((random.random() - 0.5) * 1000.0) / (111320.0 * __import__("math").cos(__import__("math").radians(lat)))
    lat += ((random.random() - 0.5) * 1000.0) / 110574.0
    display = f_var(1, 2) + f_var(2, device["screen_w"]) + f_var(3, device["screen_h"])
    ci = b""
    ci += f_var(1, PROTOCOL_VERSION)
    ci += f_str(3, APP_VERSION)
    ci += f_bytes(4, coordinate(lon, lat))
    ci += f_str(5, device["manufacturer"])
    ci += f_str(6, device["model"])
    ci += f_str(11, device["os_version"])
    ci += f_str(16, "en")
    ci += f_str(17, device["installation_id"])
    ci += f_var(18, 50)      # ANDROID_DEVICE
    ci += f_var(19, 1)       # AppType WAZE
    ci += f_bytes(24, display)
    ci += f_str(25, "en")
    ci += f_str(26, str(uuidlib.uuid4()))
    ci += f_var(28, int(time.time() * 1000))
    ci += f_var(31, 5)       # AppFlavor ALPHA (matches reference implementation)
    return ci

def batch_of(element):
    return f_bytes(1001, element)

def proto_line(element):
    return "ProtoBase64," + base64.b64encode(batch_of(element)).decode()

def client_info_line(device, lon, lat):
    return proto_line(f_bytes(2184, client_info(device, lon, lat)))

def register_line():
    return proto_line(f_bytes(2219, b""))          # Register: empty

def login_line(username, password):
    cred = f_str(1, username) + f_str(2, password)
    lr = f_bytes(1, cred) + f_var(3, 0)            # LoginReason NORMAL
    return proto_line(f_bytes(2744, lr))

def ads_line():
    return proto_line(f_bytes(2108, b""))          # ReportAdsSettings: empty

def uid_header(session_id, secret_key):
    uid = f_var(1, session_id) + f_str(2, secret_key)
    return base64.b64encode(uid).decode()

# ── plain-text command lines ────────────────────────────────────────────────

def f6(v):
    return f"{v:.6f}"

def map_displayed(lon_min, lat_min, lon_max, lat_max):
    mid_lon, mid_lat = (lon_min + lon_max) / 2, (lat_min + lat_max) / 2
    box = (f"{f6(lon_min)},{f6(lat_max)},{f6(lon_max)},{f6(lat_max)},"
           f"{f6(lon_max)},{f6(lat_min)},{f6(lon_min)},{f6(lat_min)}")
    return f"MapDisplayed,{box},{f6(mid_lon)},{f6(mid_lat)},67186,{box}"

def handshake(lon, lat):
    box = (lon - 0.018, lat - 0.015, lon + 0.018, lat + 0.015)
    return "\n".join(["SeeMe,1,2,T,T,T,1,-1,1,7", "SetMood,1",
                      f"Location,{lon},{lat}", map_displayed(*box)])

# ── response interpretation ─────────────────────────────────────────────────

class RtError(Exception):
    pass

class AccountRejected(RtError):
    pass

def check_errors(elements):
    for el in elements:
        err = get_msg(el, 2003)
        if err:
            code = get_var(err, 10101)
            desc = get_str(err, 10102)
            low = desc.lower()
            if any(k in low for k in ("relogin", "unknown userid", "secretkey missing", "secret key missing")):
                raise RtError(f"session expired: {desc}")
            if 400 <= code < 500:
                raise AccountRejected(f"server error {code}: {desc}")
            if code >= 500:
                raise RtError(f"server error {code}: {desc}")
        le = get_msg(el, 2224)
        if le:
            et = get_var(le, 2)
            if et in (2, 0):   # INTERNAL_ISSUES / UNKNOWN: transient, keep account
                raise RtError(f"login error (transient): {et}")
            raise AccountRejected(f"login error: {et}")

def parse_batch(data):
    return get_all_msg(parse_fields(data), 1001)

def parse_alerts(elements):
    out = []
    for el in elements:
        aaa = get_msg(el, 2708)
        if not aaa:
            continue
        ra = get_msg(aaa, 1)
        if not ra:
            continue
        info = get_msg(ra, 2)
        if not info:
            continue
        pos = get_msg(info, 3)
        if not pos:
            continue
        lon = to_int32(get_var(pos, 101)) / 1_000_000.0
        lat = to_int32(get_var(pos, 102)) / 1_000_000.0
        atype = ALERT_TYPES.get(get_var(info, 1), "UNKNOWN")
        subtype = ALERT_SUBTYPES.get(get_var(info, 2), "")
        street = city = ""
        thumbs = None
        report_time = 0
        ri = get_msg(ra, 3)
        if ri:
            report_time = get_var(ri, 4)
            t = get_var(ri, 9)
            thumbs = t if t > 0 else None
            addr = get_msg(ri, 8)
            if addr:
                street = get_str(addr, 2)
                city = get_str(addr, 3)
        out.append({
            "uuid": get_str(ra, 6) or str(get_var(ra, 1)),
            "type": atype,
            "subtype": subtype,
            "location": {"x": lon, "y": lat},
            "street": street,
            "city": city,
            "nThumbsUp": thumbs,
            "pubMillis": report_time * 1000 if report_time > 0 else None,
        })
    return out

def parse_removed(elements):
    out = []
    for el in elements:
        oc = get_str(el, 2001).strip()
        if oc.startswith("RmAlert,"):
            out.append(oc[len("RmAlert,"):].strip())
    return out

# ── session ─────────────────────────────────────────────────────────────────

class WazeRtSession:
    def __init__(self):
        self.cj = CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.seq = 1
        self.state = self._load_state()
        self.session_id = 0
        self.secret_key = ""

    def _load_state(self):
        if STATE_PATH.exists():
            try:
                return json.loads(STATE_PATH.read_text())
            except Exception:
                pass
        man, model, osv, w, h = random.choice(DEVICE_POOL)
        return {"device": {"manufacturer": man, "model": model, "os_version": osv,
                           "screen_w": w, "screen_h": h,
                           "installation_id": str(uuidlib.uuid4())},
                "credentials": None, "registrations": []}

    def _save_state(self):
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state))
        tmp.replace(STATE_PATH)
        try:
            STATE_PATH.chmod(0o600)
        except Exception:
            pass

    def _next_seq(self):
        s = self.seq
        self.seq += 1
        return str(s)

    def _post(self, path, body_bytes, headers, timeout=30):
        # "Connection: close" matters here: without it, responses through this
        # VM's egress proxy intermittently never terminate (observed 2026-11-08:
        # register/login POSTs hanging past 120 s, then succeeding in <1 s with
        # the header set). Retry timeouts up to 3 times, as the reference
        # OkHttp wrapper does for transient failures.
        last = None
        for attempt in range(1, 4):
            req = urllib.request.Request(f"https://{RT_HOST}{path}", data=body_bytes, method="POST")
            req.add_header("Content-Type", "binary/octet-stream")
            req.add_header("Connection", "close")
            for k, v in headers.items():
                req.add_header(k, v)
            try:
                with self.opener.open(req, timeout=timeout) as r:
                    return r.status, r.read()
            except urllib.error.HTTPError as e:
                return e.code, e.read()
            except Exception as e:
                last = e
                if attempt < 3:
                    time.sleep(0.4 * attempt)
        raise last

    def register(self, lon, lat):
        # Reference-implementation guard: at most 10 anonymous registrations / 24 h.
        now = time.time()
        regs = [t for t in self.state.get("registrations", []) if now - t < 86400]
        if len(regs) >= 10:
            raise RtError("anonymous registration cap reached for today (10/24h)")
        body = (client_info_line(self.state["device"], lon, lat) + "\n" + register_line()).encode()
        code, data = self._post(PATH_STATIC, body, {
            "User-Agent": APP_VERSION, "x-waze-network-version": "3",
            "sequence-number": self._next_seq()})
        if code >= 400:
            raise RtError(f"register HTTP {code}")
        if not data:
            raise RtError("empty register response")
        elements = parse_batch(data)
        check_errors(elements)
        for el in elements:
            rs = get_msg(el, 2220)
            if rs:
                user, pw = get_str(rs, 1), get_str(rs, 2)
                if user and pw:
                    self.state["credentials"] = {"username": user, "password": pw}
                    regs.append(now)
                    self.state["registrations"] = regs
                    self._save_state()
                    return
        raise RtError("register: no RegisterSuccessful in response")

    def login(self, lon, lat):
        creds = self.state.get("credentials")
        if not creds:
            raise RtError("login before register")
        self.cj.clear()
        body = ("\n".join([client_info_line(self.state["device"], lon, lat),
                           login_line(creds["username"], creds["password"]),
                           ads_line()])).encode()
        code, data = self._post(PATH_LOGIN, body, {
            "User-Agent": f"waze/{APP_VERSION}", "cache-control": "no-cache",
            "sequence-number": self._next_seq(), "x-waze-network-version": "3",
            "x-waze-wait-timeout": "8500"})
        if 400 <= code < 500:
            raise AccountRejected(f"login HTTP {code}")
        if code >= 500:
            raise RtError(f"login HTTP {code}")
        if not data:
            raise RtError("empty login response")
        elements = parse_batch(data)
        check_errors(elements)
        for el in elements:
            lr = get_msg(el, 2745)
            if not lr:
                continue
            lerr = get_msg(lr, 2)
            if lerr:
                et = get_var(lerr, 2)
                if et in (2, 0):
                    raise RtError(f"login error (transient): {et}")
                raise AccountRejected(f"login error: {et}")
            ls = get_msg(lr, 1)
            if ls:
                sid = get_var(ls, 1)
                secret = get_str(ls, 3)
                if sid and secret:
                    self.session_id, self.secret_key = sid, secret
                    self.seq = 2
                    return
        raise AccountRejected("login: no LoginSuccess in response")

    def command(self, payload, timeout=30):
        if not self.session_id:
            raise RtError("command before login")
        code, data = self._post(PATH_COMMAND, payload.encode(), {
            "User-Agent": APP_VERSION, "cache-control": "no-cache",
            "sequence-number": self._next_seq(), "x-waze-network-version": "3",
            "x-waze-wait-timeout": "10500",
            "uid": uid_header(self.session_id, self.secret_key)}, timeout=timeout)
        if 400 <= code < 500:
            self.session_id = 0
            raise RtError(f"command HTTP {code} (session invalidated)")
        if code >= 500:
            raise RtError(f"command HTTP {code}")
        if not data:
            raise RtError("empty command response")
        elements = parse_batch(data)
        check_errors(elements)
        return elements

    def ensure_ready(self, lon, lat):
        if not self.state.get("credentials"):
            self.register(lon, lat)
        try:
            self.login(lon, lat)
        except AccountRejected:
            # Anonymous account was purged server-side: register a fresh one, once.
            self.state["credentials"] = None
            self._save_state()
            self.register(lon, lat)
            self.login(lon, lat)

# ── area fetch ──────────────────────────────────────────────────────────────

def fetch_snapshot(tiles, centre_lon=-79.35, centre_lat=43.78):
    """One poll: login, handshake, then one MapDisplayed per tile.
    Returns (alerts, removed_uuids) merged across the whole snapshot —
    within a session the server sends each alert once, so the merge is the
    complete current picture. `tiles` are dicts with bottom/top/left/right."""
    s = WazeRtSession()
    s.ensure_ready(centre_lon, centre_lat)
    alerts, removed = {}, []
    elements = s.command(handshake(centre_lon, centre_lat))
    for a in parse_alerts(elements):
        alerts[a["uuid"]] = a
    removed += parse_removed(elements)
    for t in tiles:
        elements = s.command(map_displayed(t["left"], t["bottom"], t["right"], t["top"]))
        for a in parse_alerts(elements):
            alerts[a["uuid"]] = a
        removed += parse_removed(elements)
    return list(alerts.values()), removed

if __name__ == "__main__":
    # Smoke test: one downtown tile.
    tiles = [{"bottom": 43.63, "top": 43.68, "left": -79.42, "right": -79.35}]
    alerts, removed = fetch_snapshot(tiles)
    police = [a for a in alerts if a["type"] == "POLICE"]
    print(json.dumps({"snapshot_alerts": len(alerts), "police": len(police),
                      "removed_signals": len(removed)}))
    for a in police[:10]:
        print(a["subtype"] or "POLICE", a["location"], a["street"], a["city"])
