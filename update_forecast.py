"""
update_forecast.py
==================
Standalone Forecast-Update für GitHub Actions.

Läuft im ~/SaltyLakeVienna/forecast Repo in der GitHub-Cloud.
Kein externes Modul-Import nötig — alles in einer Datei.

Holt:
  - Open-Meteo (4 Modelle): best_match, ecmwf_ifs04, icon_seamless, gfs_seamless
  - ZAMG AROME (2.5 km, Alpenraum)

Aggregiert per Median → schreibt forecast.json im aktuellen Verzeichnis.
"""

import ssl
import json
import math
import urllib.request
from datetime import datetime, timezone
from statistics import median

# ============================================================
# Config: Neue Donau Wien
# ============================================================
LATITUDE  = 48.2183
LONGITUDE = 16.4279

MIN_BEGINNER     = 6
MAX_BEGINNER     = 15
MAX_INTERMEDIATE = 25

DIR_SCORE = {
    'NW':  1.0, 'WNW': 0.85, 'NNW': 0.85,
    'SO':  1.0, 'SSO': 0.85, 'OSO': 0.85,
    'W':   0.5, 'N':   0.5, 'S':   0.5, 'O':   0.5,
    'WSW': 0.3, 'SW':  0.2, 'SSW': 0.3,
    'NO':  0.3, 'NNO': 0.3, 'ONO': 0.3,
}

COMPASS = ['N','NNO','NO','ONO','O','OSO','SO','SSO',
           'S','SSW','SW','WSW','W','WNW','NW','NNW']


def deg_to_compass(deg):
    return COMPASS[round(deg / 22.5) % 16]


def uv_to_speed_dir(u, v):
    speed = math.sqrt(u * u + v * v)
    deg = (math.degrees(math.atan2(-u, -v))) % 360
    return speed, deg


def ms_to_kn(ms):
    return ms * 1.94384


def circular_median(degs):
    if not degs:
        return 0.0
    sins = [math.sin(math.radians(d)) for d in degs]
    coss = [math.cos(math.radians(d)) for d in degs]
    deg = math.degrees(math.atan2(median(sins), median(coss)))
    return deg if deg >= 0 else deg + 360


def rate_conditions(kn, compass):
    if kn is None:
        return "keine Daten"
    if kn < MIN_BEGINNER:
        return "ZU SCHWACH"
    if kn > MAX_INTERMEDIATE:
        return "ZU STARK"
    dir_score = DIR_SCORE.get(compass, 0.3)
    if dir_score >= 0.95 and MIN_BEGINNER <= kn <= MAX_BEGINNER:
        return "PERFEKT"
    if dir_score >= 0.8 and MIN_BEGINNER <= kn <= MAX_INTERMEDIATE:
        return "GUT"
    if dir_score >= 0.5:
        return "OK"
    return "BEDINGT"


# ============================================================
# Open-Meteo
# ============================================================
def fetch_open_meteo(model):
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={LATITUDE}&longitude={LONGITUDE}"
        f"&hourly=wind_speed_10m,wind_direction_10m,wind_gusts_10m,temperature_2m"
        f"&wind_speed_unit=kn&timezone=Europe%2FVienna&forecast_days=14"
        f"&models={model}"
    )
    ctx = ssl.create_default_context()
    with urllib.request.urlopen(url, context=ctx, timeout=30) as r:
        raw = json.loads(r.read())

    h = raw["hourly"]
    result = []
    for i, ts in enumerate(h["time"]):
        kn = h["wind_speed_10m"][i]
        deg = h["wind_direction_10m"][i]
        gust = h["wind_gusts_10m"][i]
        temp = h["temperature_2m"][i]
        if kn is None or deg is None:
            continue
        result.append({
            "time": ts,
            "wind_kn": round(kn, 1),
            "wind_deg": round(deg, 1),
            "wind_compass": deg_to_compass(deg),
            "wind_gust_kn": round(gust, 1) if gust is not None else None,
            "temp_c": round(temp, 1) if temp is not None else None,
            "source": f"open-meteo-{model}",
        })
    return result


# ============================================================
# ZAMG AROME
# ============================================================
def fetch_zamg():
    PARAMS = ["u10m", "v10m", "t2m"]
    params_str = "&".join([f"parameters={p}" for p in PARAMS])
    url = (
        f"https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/nwp-v1-1h-2500m"
        f"?{params_str}"
        f"&lat_lon={LATITUDE},{LONGITUDE}"
        f"&output_format=geojson"
    )
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        raw = json.loads(r.read())

    timestamps = raw["timestamps"]
    props = raw["features"][0]["properties"]["parameters"]

    u_data = props["u10m"]["data"]
    v_data = props["v10m"]["data"]
    t_data = props["t2m"]["data"]

    result = []
    for i, ts in enumerate(timestamps):
        u, v, t = u_data[i], v_data[i], t_data[i]
        if u is None or v is None:
            continue
        speed_ms, deg = uv_to_speed_dir(u, v)
        result.append({
            "time": ts,
            "wind_kn": round(ms_to_kn(speed_ms), 1),
            "wind_deg": round(deg, 1),
            "wind_compass": deg_to_compass(deg),
            "wind_gust_kn": None,
            "temp_c": round(t, 1) if t is not None else None,
            "source": "zamg-arome",
        })
    return result


# ============================================================
# Aggregator
# ============================================================
def get_aggregated_forecast():
    all_data = {}
    sources_ok = []

    for model in ["best_match", "ecmwf_ifs04", "icon_seamless", "gfs_seamless"]:
        try:
            data = fetch_open_meteo(model)
            for row in data:
                ts = row["time"][:13]
                all_data.setdefault(ts, []).append(row)
            print(f"  open-meteo-{model}: {len(data)} Stunden")
            if data:
                sources_ok.append(f"open-meteo-{model}")
        except Exception as e:
            print(f"  open-meteo-{model} fehlgeschlagen: {e}")

    try:
        zamg = fetch_zamg()
        for row in zamg:
            ts = row["time"][:13]
            all_data.setdefault(ts, []).append(row)
        print(f"  zamg-arome: {len(zamg)} Stunden")
        if zamg:
            sources_ok.append("zamg-arome")
    except Exception as e:
        print(f"  zamg-arome fehlgeschlagen: {e}")

    aggregated = []
    for ts in sorted(all_data.keys()):
        rows = all_data[ts]
        winds = [r["wind_kn"] for r in rows if r.get("wind_kn") is not None]
        degs  = [r["wind_deg"] for r in rows if r.get("wind_deg") is not None]
        gusts = [r["wind_gust_kn"] for r in rows if r.get("wind_gust_kn") is not None]
        temps = [r["temp_c"] for r in rows if r.get("temp_c") is not None]

        if not winds or not degs:
            continue

        wind_kn = round(median(winds), 1)
        deg = round(circular_median(degs), 0)
        compass = deg_to_compass(deg)
        spread = round(max(winds) - min(winds), 1) if len(winds) > 1 else 0.0

        aggregated.append({
            "time": ts + ":00",
            "wind_kn": wind_kn,
            "wind_deg": deg,
            "wind_compass": compass,
            "wind_gust_kn": round(median(gusts), 1) if gusts else None,
            "temp_c": round(median(temps), 1) if temps else None,
            "rating": rate_conditions(wind_kn, compass),
            "sources": [r["source"] for r in rows],
            "model_count": len(rows),
            "spread_kn": spread,
        })

    return aggregated, sources_ok


def aggregate_daily(hourly):
    by_day = {}
    for h in hourly:
        day = h["time"][:10]
        by_day.setdefault(day, []).append(h)

    result = []
    for day in sorted(by_day.keys()):
        hours = by_day[day]

        def slot_hours(s, e):
            return [h for h in hours if s <= int(h["time"][11:13]) < e]

        def summarize(slot):
            if not slot:
                return None
            winds = [h["wind_kn"] for h in slot]
            degs = [h["wind_deg"] for h in slot]
            deg_avg = circular_median(degs)
            compass = deg_to_compass(deg_avg)
            kn = round(median(winds), 1)
            return {
                "wind_kn": kn,
                "wind_compass": compass,
                "wind_deg": round(deg_avg, 0),
                "rating": rate_conditions(kn, compass),
                "spread_kn": round(max(winds) - min(winds), 1) if len(winds) > 1 else 0.0,
            }

        all_winds = [h["wind_kn"] for h in hours]
        all_temps = [h["temp_c"] for h in hours if h["temp_c"] is not None]

        result.append({
            "date": day,
            "wind_kn_avg": round(median(all_winds), 1),
            "wind_kn_max": round(max(all_winds), 1),
            "temp_max": round(max(all_temps), 1) if all_temps else None,
            "temp_min": round(min(all_temps), 1) if all_temps else None,
            "morning_10_12":   summarize(slot_hours(10, 12)),
            "afternoon_14_16": summarize(slot_hours(14, 16)),
        })
    return result


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    started = datetime.now(timezone.utc)
    print(f"🌬️  Forecast-Update: {started.isoformat(timespec='seconds')}")

    hourly, sources = get_aggregated_forecast()
    if not hourly:
        print("❌ Keine Daten, Abbruch.")
        exit(1)

    daily = aggregate_daily(hourly)

    output = {
        "generated_at": started.isoformat(timespec="seconds"),
        "generated_at_unix": int(started.timestamp()),
        "location": {
            "name": "Neue Donau Wien",
            "lat": LATITUDE,
            "lon": LONGITUDE,
        },
        "spot_info": {
            "best_directions": ["NW", "SO"],
            "beginner_range_kn": [6, 15],
            "max_intermediate_kn": 25,
        },
        "hourly": hourly,
        "daily": daily,
        "meta": {
            "hourly_count": len(hourly),
            "daily_count": len(daily),
            "sources": sources,
            "runner": "github-actions",
        },
    }

    with open("forecast.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"✅ forecast.json geschrieben: {len(hourly)}h, {len(daily)} Tage, {len(sources)} Modelle OK")
