from __future__ import annotations
from typing import Dict, Iterable, Optional, Tuple, List
import pathlib
import numpy as np
import pandas as pd

from .metrics import log_returns, realized_variance, acf_np
from .plots import line_series

def _take_cols(df: pd.DataFrame, pair: str, fields: Iterable[str], prefixes: Iterable[str]) -> Optional[pd.DataFrame]:
    cols: Dict[str, str] = {}
    for f in fields:
        for pref in prefixes:
            c = f"{pref}{pair}.{f}"
            if c in df.columns:
                cols[f] = c
                break
    base = ["Time"] + list(cols.values())
    if not set(base).issubset(df.columns):
        return None
    out = df[base].copy()
    out.columns = ["timestamp"] + list(cols.keys())
    out["pair"] = pair
    return out

def build_fx_panels(order_csv: str, trade_csv: str, pairs: List[str]) -> Dict[str, pd.DataFrame]:
    orders_raw = pd.read_csv(order_csv)
    trades_raw = pd.read_csv(trade_csv)

    order_fields = ["DELETED_TIME", "NUM_PARTCP", "BUY_SELL_FLAG", "TICK_STATUS", "RECORD_TYPE", "PRICE", "SIZE", "OMDSEQ"]
    trade_fields = ["PRICE", "SIZE", "BUY_SELL_FLAG"]

    orders = []
    trades = []
    for p in pairs:
        o = _take_cols(orders_raw, p, order_fields, prefixes=("EBS_BOOK::",))
        t = _take_cols(trades_raw, p, trade_fields, prefixes=("EBS_TRADE::", "EBS_BOOK::"))
        if o is not None:
            orders.append(o)
        if t is not None:
            trades.append(t)
    orders = pd.concat(orders, ignore_index=True)
    trades = pd.concat(trades, ignore_index=True)

    orders["timestamp"] = pd.to_datetime(orders["timestamp"], errors="coerce")
    trades["timestamp"] = pd.to_datetime(trades["timestamp"], errors="coerce")
    orders = orders.dropna(subset=["timestamp"])
    trades = trades.dropna(subset=["timestamp"])

    start_time = pd.to_datetime("09:30:00").time()
    end_time = pd.to_datetime("16:00:00").time()
    orders = orders[(orders["timestamp"].dt.time >= start_time) & (orders["timestamp"].dt.time <= end_time)]
    trades = trades[(trades["timestamp"].dt.time >= start_time) & (trades["timestamp"].dt.time <= end_time)]

    orders["price"] = pd.to_numeric(orders["PRICE"], errors="coerce")
    orders["size"] = pd.to_numeric(orders["SIZE"], errors="coerce")
    orders["side"] = orders["BUY_SELL_FLAG"].astype("Int64")
    trades["price"] = pd.to_numeric(trades.get("PRICE"), errors="coerce")
    trades["size"] = pd.to_numeric(trades.get("SIZE"), errors="coerce")
    if "BUY_SELL_FLAG" in trades:
        trades["side"] = trades["BUY_SELL_FLAG"].astype("Int64")

    orders["second"] = orders["timestamp"].dt.floor("S")
    orders["minute"] = orders["timestamp"].dt.floor("T")
    trades["second"] = trades["timestamp"].dt.floor("S")
    trades["minute"] = trades["timestamp"].dt.floor("T")

    quotes = orders.pivot_table(index="timestamp", columns=["pair", "side"], values="price", aggfunc="last").sort_index()
    sizes = orders.pivot_table(index="timestamp", columns=["pair", "side"], values="size", aggfunc="last").sort_index()

    out = {"orders": orders, "trades": trades, "quotes": quotes, "sizes": sizes}
    return out

def fx_summary_and_figures(panels: Dict[str, pd.DataFrame], out_dir: str, pairs: Iterable[str]) -> None:
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    quotes = panels["quotes"]
    sizes = panels["sizes"]
    trades = panels["trades"]

    mid_1s = {}
    mid_1m = {}
    px_1s = {}
    px_1m = {}
    for p in pairs:
        have = ((p, 0) in quotes.columns) and ((p, 1) in quotes.columns)
        if not have:
            continue
        mid = ((quotes[(p, 0)] + quotes[(p, 1)]) * 0.5).rename("mid")
        s1 = mid.resample("1S").last().ffill()
        sT = mid.resample("1T").last().ffill()
        mid_1s[p] = s1
        mid_1m[p] = sT

        tx1 = trades.loc[trades["pair"] == p].sort_values("timestamp").groupby("second")["price"].last()
        txT = trades.loc[trades["pair"] == p].sort_values("timestamp").groupby("minute")["price"].last()
        px_1s[p] = tx1
        px_1m[p] = txT

        line_series(s1, f"{p} one second midquote", "mid", out / f"{p.replace('/', '')}_mid_1s.png")
        line_series(sT, f"{p} one minute midquote", "mid", out / f"{p.replace('/', '')}_mid_1m.png")
        line_series(tx1.dropna(), f"{p} one second transaction price", "price", out / f"{p.replace('/', '')}_px_1s.png")
        line_series(txT.dropna(), f"{p} one minute transaction price", "price", out / f"{p.replace('/', '')}_px_1m.png")

    rows = []
    for p in pairs:
        if p in mid_1m and p in px_1m:
            r_mid = log_returns(mid_1m[p])
            r_px = log_returns(px_1m[p])
            rv_mid = realized_variance(r_mid)
            rv_px = realized_variance(r_px)
            ac_mid = acf_np(r_mid, nlags=10)
            ac_px = acf_np(r_px, nlags=10)
            rows.append({"pair": p, "rv_mid_1m": rv_mid, "rv_tx_1m": rv_px, "acf1_mid": ac_mid[0], "acf1_tx": ac_px[0]})
    pd.DataFrame(rows).to_csv(out / "fx_variance_acf.csv", index=False)

    need = ["EUR/USD", "USD/JPY", "EUR/JPY"]
    if all(p in mid_1s for p in need):
        idx = sorted(set(mid_1s["EUR/USD"].dropna().index) & set(mid_1s["USD/JPY"].dropna().index) & set(mid_1s["EUR/JPY"].dropna().index))
        eu = mid_1s["EUR/USD"].loc[idx]
        uj = mid_1s["USD/JPY"].loc[idx]
        ej = mid_1s["EUR/JPY"].loc[idx]
        gap = np.log(ej) - (np.log(eu) + np.log(uj))
        tau = 1e-4
        flags = gap.abs() > tau

        cap_proxy = None
        try:
            sd = panels["sizes"]
            sd_sec = (
                sd.stack([0, 1]).reset_index()
                .rename(columns={"level_1": "pair", "level_2": "side", 0: "size"})
                .pivot_table(index="timestamp", columns="pair", values="size", aggfunc="last")
                .reindex(idx)
            )
            cap_proxy = sd_sec[need].min(axis=1)
        except Exception:
            pass

        runs = []
        cur = 0
        for v in flags.values:
            cur = cur + 1 if v else (runs.append(cur) or 0)
        if cur > 0:
            runs.append(cur)

        summary = {
            "seconds_total": int(len(idx)),
            "seconds_flagged": int(flags.sum()),
            "freq_pct": float(100.0 * flags.sum() / len(idx)) if len(idx) else np.nan,
            "avg_duration_sec": float(np.mean(runs)) if runs else 0.0,
            "max_duration_sec": int(np.max(runs)) if runs else 0,
            "capacity_proxy_median": float(np.nanmedian(cap_proxy)) if cap_proxy is not None else np.nan,
        }
        pd.Series(summary).to_csv(out / "fx_triangular_summary.csv")
