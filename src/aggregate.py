import sys
import pandas as pd
from pathlib import Path


def main(parquet_path):
    src = Path(parquet_path)
    if not src.exists():
        print(f"[!] File not found: {src}")
        return

    out_dir = Path("data/h3")
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / src.name

    df = pd.read_parquet(src)

    before = len(df)
    df = df[
        (df["transacao"] == "venda") &
        df["h3_id"].notna() &
        (df["area"] > 0) &
        (df["preco"] > 0)
    ].copy()
    dropped = before - len(df)

    if df.empty:
        print("[!] No usable venda rows with area, price, and H3 cell")
        return

    df["preco_m2"] = df["preco"] / df["area"]

    agg = (
        df.groupby("h3_id")
        .agg(
            contagem=("id", "count"),
            preco_m2_mediano=("preco_m2", "median"),
            preco_m2_medio=("preco_m2", "mean"),
            preco_mediano=("preco", "median"),
            area_mediana=("area", "median"),
        )
        .reset_index()
    )

    agg.to_parquet(dest, index=False)

    print(f"  Source     {src}  ({before} rows, {dropped} excluded)")
    print(f"  Saved      {dest}  ({len(agg)} H3 cells)")
    return dest


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        target = sys.argv[1]
    else:
        parquets = sorted(Path("data/georeferenced").glob("*.parquet"))
        if not parquets:
            print("[!] No parquet files found in data/georeferenced/")
            sys.exit(1)
        target = parquets[-1]
        print(f"[*] Using most recent: {target}")

    main(str(target))
