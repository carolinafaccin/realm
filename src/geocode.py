import sys
import json
import time
import datetime
import pandas as pd
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

ROOT = Path(__file__).resolve().parent.parent
CACHE_FILE = ROOT / "data/.geocache.json"
RATE_LIMIT = 1.1  # Nominatim policy: max 1 request/second


def load_cache():
    if CACHE_FILE.exists():
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


def geocode_one(geolocator, address, cache, retries=3):
    if address in cache:
        return cache[address]

    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT)
            location = geolocator.geocode(address, timeout=10)
            if location:
                result = [location.latitude, location.longitude]
                cache[address] = result
                return result
            break
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"         retrying ({attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)

    cache[address] = None
    return None


def main(parquet_path):
    src = Path(parquet_path)
    if not src.exists():
        print(f"[!] File not found: {src}")
        return

    out_dir = ROOT / "data/geocode"
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / src.name

    start = datetime.datetime.now()
    df = pd.read_parquet(src)
    cache = load_cache()

    df["_addr"] = (
        df["endereco"].fillna("") + ", " +
        df["bairro"].fillna("") + ", " +
        df["cidade"].fillna("") + ", RS, Brazil"
    )

    unique_addrs = df["_addr"].unique()
    to_geocode = [a for a in unique_addrs if a not in cache]
    cached_count = len(unique_addrs) - len(to_geocode)

    print(f"\n  Source     {src}")
    print(f"  Dest       {dest}")
    print(f"  Rows       {len(df)}")
    print(f"  Addresses  {len(unique_addrs)} unique  ({cached_count} cached, {len(to_geocode)} new)")

    if to_geocode:
        print()
        geolocator = Nominatim(user_agent="poa-analise")
        ok = fail = 0

        for i, address in enumerate(to_geocode, 1):
            pct = i / len(to_geocode) * 100
            eta_s = (len(to_geocode) - i) * RATE_LIMIT
            eta = f"~{int(eta_s)}s left" if i < len(to_geocode) else "done"
            result = geocode_one(geolocator, address, cache)

            if result:
                ok += 1
                status = "✓"
            else:
                fail += 1
                status = "✗"

            print(f"  [{i:>3}/{len(to_geocode)}  {pct:>5.1f}%  {eta:<12}]  {status}  {address}")

            if i % 100 == 0:
                save_cache(cache)

        save_cache(cache)
        print(f"\n  Geocoded   {ok} OK  |  {fail} failed")
    else:
        print("  All addresses already in cache — skipping geocoding")

    df["latitude"] = df["_addr"].map(lambda a: (cache.get(a) or [None, None])[0])
    df["longitude"] = df["_addr"].map(lambda a: (cache.get(a) or [None, None])[1])

    df = df.drop(columns=["_addr"])
    df.to_parquet(dest, index=False)

    geocoded = int(df["latitude"].notna().sum())
    elapsed = (datetime.datetime.now() - start).seconds
    print(f"\n  Saved      {dest}")
    print(f"  Geocoded   {geocoded}/{len(df)} rows with coordinates  ({geocoded/len(df)*100:.1f}%)")
    print(f"  Elapsed    {elapsed}s\n")
    return dest


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        target = sys.argv[1]
    else:
        parquets = sorted((ROOT / "data/scrape").glob("*.parquet"))
        if not parquets:
            print("[!] No parquet files found in data/scrape/")
            sys.exit(1)
        target = parquets[-1]
        print(f"[*] Using most recent: {target}")

    main(str(target))
