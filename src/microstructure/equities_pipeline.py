from __future__ import annotations
import pathlib
import pandas as pd

from .metrics import (
    prepare_session,
    per_minute_dollar_volume,
    order_counts_per_minute,
    ohlc_per_minute,
    vwap_per_minute,
    build_l2_by_bucket,
    depth_near_touch,
    price_impact_by_minute,
    build_mid_and_px_series,
    log_returns,
    realized_variance,
    acf_np,
)
from .plots import line_series, bar_minute

def run_equity_day(
    csv_path: str,
    symbol: str,
    session_date: str,
    out_dir: str,
    price_in_nanos: bool = True,
) -> None:
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    raw = pd.read_csv(csv_path)
    mbo = prepare_session(raw, symbol=symbol, session_date=session_date, price_in_nanos=price_in_nanos)
    trades = mbo[mbo["action"] == "T"].copy()

    dv = per_minute_dollar_volume(trades)
    bar_minute(dv, f"{symbol} dollar volume per minute", "dollar volume per minute", out / f"{symbol.lower()}_dv_min.png")

    counts = order_counts_per_minute(mbo)
    counts.to_csv(out / f"{symbol.lower()}_order_counts.csv", index=True)

    ohlc = ohlc_per_minute(trades)
    ohlc.to_csv(out / f"{symbol.lower()}_ohlc_min.csv")
    vwap = vwap_per_minute(trades)
    line_series(vwap, f"{symbol} vwap per minute", "vwap", out / f"{symbol.lower()}_vwap_min.png")

    l2_min = build_l2_by_bucket(mbo, bucket="minute")
    l2_sec = build_l2_by_bucket(mbo, bucket="second")
    l2_min[["spread"]].reset_index().groupby("ts").last()["spread"].to_csv(out / f"{symbol.lower()}_spread_min.csv")

    depth_touch = depth_near_touch(l2_min, multiple=2.0)
    depth_touch.to_csv(out / f"{symbol.lower()}_depth_near_touch.csv", index=False)

    impact = price_impact_by_minute(trades, l2_sec, horizon_seconds=5)
    impact.to_csv(out / f"{symbol.lower()}_impact.csv", index=False)
    line_series(impact.set_index("minute")[["beta_5s"]].squeeze(), f"{symbol} five second price impact", "beta", out / f"{symbol.lower()}_impact.png")

    series = build_mid_and_px_series(l2_sec, trades)
    line_series(series["mid_1s"], f"{symbol} one second midquote", "mid", out / f"{symbol.lower()}_mid_1s.png")
    line_series(series["mid_1m"], f"{symbol} one minute midquote", "mid", out / f"{symbol.lower()}_mid_1m.png")
    line_series(series["px_1s"].dropna(), f"{symbol} one second transaction price", "price", out / f"{symbol.lower()}_px_1s.png")
    line_series(series["px_1m"].dropna(), f"{symbol} one minute transaction price", "price", out / f"{symbol.lower()}_px_1m.png")

    r_mid_1m = log_returns(series["mid_1m"])
    r_px_1m = log_returns(series["px_1m"])
    rv_mid = realized_variance(r_mid_1m)
    rv_px = realized_variance(r_px_1m)
    with open(out / f"{symbol.lower()}_variance.txt", "w") as f:
        f.write(f"midquote RV one minute  {rv_mid}\n")
        f.write(f"transaction RV one minute  {rv_px}\n")

    ac_mid = acf_np(r_mid_1m, nlags=20)
    ac_px = acf_np(r_px_1m, nlags=20)
    pd.DataFrame({"lag": list(range(1, 21)), "acf_mid": ac_mid, "acf_px": ac_px}).to_csv(out / f"{symbol.lower()}_acf.csv", index=False)
