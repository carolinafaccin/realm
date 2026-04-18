import sys
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point


def main(parquet_path):
    path = Path(parquet_path)
    if not path.exists():
        print(f"[!] File not found: {path}")
        return

    df = pd.read_parquet(path)

    before = len(df)
    df = df[df['latitude'].notna() & df['longitude'].notna()].copy()
    dropped = before - len(df)

    if df.empty:
        print("[!] No rows with coordinates — run geocode.py first")
        return

    df['geometry'] = df.apply(lambda r: Point(r['longitude'], r['latitude']), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    gdf = gdf.drop(columns=['latitude', 'longitude'])

    out_path = path.with_suffix('.gpkg')
    gdf.to_file(out_path, driver='GPKG')

    print(f"  Saved  {out_path}  ({len(gdf)} features, {dropped} rows skipped — no coordinates)")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        target = sys.argv[1]
    else:
        parquets = sorted(p for p in Path("data/georeferenced").glob("*.parquet"))
        if not parquets:
            print("[!] No parquet files found in data/")
            sys.exit(1)
        target = parquets[-1]
        print(f"[*] Using most recent: {target}")

    main(str(target))
