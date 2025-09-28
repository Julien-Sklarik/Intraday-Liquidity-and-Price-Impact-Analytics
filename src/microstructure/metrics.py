from __future__ import annotations
import math
from typing import Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

# ---------- generic utilities

def prepare_session(
    mbo: pd.DataFrame,
    symbol: str,
    session_date: str,
    tz: str = "America/New_York",
    price_in_nanos: bool = True,
) -> pd.DataFrame:
    df = mbo[mbo["symbol"] == symbol].copy()
    df["ts"] = pd.to_datetime(df["ts_event"], unit="ns", utc=True)
    df["ts_et"] = df["ts"].dt.tz_convert(tz)

    d = pd.to_datetime(session_date).date()
    sod = pd.Timestamp(d, tz=tz) + pd.Timedelta(hours=9, minutes=30)
    eod = pd.Timestamp(d, tz=tz) + pd.Timedelta(hours=16)

    df = df[(df["ts_et"] >= sod) & (df["ts_et"] < eod)].sort_values(["ts_et", "sequence"])
    if price_in_nanos:
        df["px"] = df["price"] / 1_000_000_000.0
    else:
        df["px"] = df["price"]
    df["minute"] = df["ts_et"].dt.floor("min")
    df["second"] = df["ts_et"].dt.floor("s")
    return df

def per_minute_dollar_volume(trades: pd.DataFrame) -> pd.Series:
    return (
        trades.assign(dollar=lambda t: t["px"] * t["size"])
        .groupby("minute")["dollar"]
        .sum()
        .rename("dollar_volume")
    )

def order_counts_per_minute(mbo: pd.DataFrame) -> pd.DataFrame:
    g = lambda d, n: d.groupby("minute").size().rename(n)
    n_trades = g(mbo[mbo["action"] == "T"], "n_trades")
    n_add = g(mbo[mbo["action"] == "A"], "n_orders_add")
    n_new = g(mbo[mbo["action"].isin(["A", "F", "R"])], "n_orders_new")
    n_cancel = g(mbo[mbo["action"] == "C"], "n_orders_cancel")
    n_replace = g(mbo[mbo["action"] == "R"], "n_orders_replace")
    return pd.concat([n_trades, n_new, n_add, n_cancel, n_replace], axis=1).fillna(0)

def ohlc_per_minute(trades: pd.DataFrame) -> pd.DataFrame:
    return trades.set_index("ts")["px"].resample("min").ohlc()

def vwap_per_minute(trades: pd.DataFrame) -> pd.Series:
    g = (
        trades.assign(px_sz=lambda d: d["px"] * d["size"])
        .groupby(pd.Grouper(key="ts", freq="min"))[["px_sz", "size"]]
        .sum()
    )
    return (g["px_sz"] / g["size"]).rename("vwap")

# ---------- book rebuild and snapshots

def _best(levels: Dict[float, int], is_bid: bool) -> Tuple[Optional[float], int]:
    keys = [p for p, q in levels.items() if q > 0]
    if not keys:
        return None, 0
    p = max(keys) if is_bid else min(keys)
    return p, int(levels[p])

def build_l2_by_bucket(mbo: pd.DataFrame, bucket: str = "minute") -> pd.DataFrame:
    assert bucket in {"minute", "second"}
    orders: Dict[int, Tuple[str, float, int]] = {}
    levels_bid: Dict[float, int] = {}
    levels_ask: Dict[float, int] = {}
    recs = []

    current = mbo[bucket].iloc[0]

    for r in mbo.itertuples(index=False):
        act, oid, side, px, sz = r.action, int(r.order_id), r.side, float(r.px), int(r.size)

        if getattr(r, bucket) > current:
            bb, bq = _best(levels_bid, True)
            ba, aq = _best(levels_ask, False)
            spread = float(np.subtract(ba, bb)) if bb is not None and ba is not None else np.nan
            if np.isfinite(spread):
                for p, q in levels_ask.items():
                    if q > 0:
                        recs.append((getattr(r, bucket), "A", p, q, bb, ba, bq, aq, spread))
                for p, q in levels_bid.items():
                    if q > 0:
                        recs.append((getattr(r, bucket), "B", p, q, bb, ba, bq, aq, spread))
            current = getattr(r, bucket)

        if act in ("A", "F"):
            if oid in orders:
                s_old, p_old, q_old = orders.pop(oid)
                if s_old == "B":
                    levels_bid[p_old] = levels_bid.get(p_old, 0) - q_old
                else:
                    levels_ask[p_old] = levels_ask.get(p_old, 0) - q_old
            orders[oid] = (side, px, sz)
            if side == "B":
                levels_bid[px] = levels_bid.get(px, 0) + sz
            else:
                levels_ask[px] = levels_ask.get(px, 0) + sz

        elif act == "C":
            if oid in orders:
                s_old, p_old, q_old = orders[oid]
                dq = int(min(sz, q_old))
                q_new = q_old - dq
                if s_old == "B":
                    levels_bid[p_old] = levels_bid.get(p_old, 0) - dq
                else:
                    levels_ask[p_old] = levels_ask.get(p_old, 0) - dq
                if q_new > 0:
                    orders[oid] = (s_old, p_old, q_new)
                else:
                    orders.pop(oid, None)

        elif act == "R":
            if oid in orders:
                s_old, p_old, q_old = orders[oid]
                if s_old == "B":
                    levels_bid[p_old] = levels_bid.get(p_old, 0) - q_old
                else:
                    levels_ask[p_old] = levels_ask.get(p_old, 0) - q_old
                orders[oid] = (side, px, sz)
                if side == "B":
                    levels_bid[px] = levels_bid.get(px, 0) + sz
                else:
                    levels_ask[px] = levels_ask.get(px, 0) + sz

    l2 = pd.DataFrame(
        recs,
        columns=[
            "ts", "side", "price", "depth",
            "best_bid", "best_ask", "best_bid_depth", "best_ask_depth", "spread",
        ],
    )
    l2["mid_price"] = (l2["best_bid"] + l2["best_ask"]) * 0.5
    l2["rel_spread"] = l2["spread"] / l2["mid_price"]
    return l2.set_index("ts").sort_index()

def depth_near_touch(l2: pd.DataFrame, multiple: float = 2.0) -> pd.DataFrame:
    avg_spread = l2["spread"].mean()
    near = l2[
        ((l2["side"] == "A") & (l2["price"] < l2["mid_price"] + multiple * avg_spread))
        | ((l2["side"] == "B") & (l2["price"] > l2["mid_price"] - multiple * avg_spread))
    ]
    depth = near.groupby(["ts", "side"], observed=True)["depth"].sum()
    ts_idx = depth.index.get_level_values(0)
    tz = getattr(ts_idx[0], "tz", None)
    all_ts = pd.date_range(ts_idx.min().floor("min"), ts_idx.max().ceil("min"), freq="min", tz=tz)
    full_idx = pd.MultiIndex.from_product([all_ts, ["A", "B"]], names=["ts", "side"])
    return depth.reindex(full_idx, fill_value=0).reset_index()

# ---------- impact and simple stats

def price_impact_by_minute(trades: pd.DataFrame, l2_sec: pd.DataFrame, horizon_seconds: int = 5) -> pd.DataFrame:
    tr = trades.loc[trades["side"].isin(["A", "B"]), ["ts", "side"]].copy()
    tr = tr.sort_values("ts").assign(sign=lambda x: x["side"].map({"B": 1, "A": -1}).astype(int))

    mids = (
        l2_sec[["mid_price"]]
        .reset_index()
        .drop_duplicates("ts", keep="last")
        .sort_values("ts")
    )
    tr = (
        pd.merge_asof(tr, mids, on="ts", direction="backward")
        .rename(columns={"mid_price": "mid0"})
    )
    tr["ts_h"] = tr["ts"] + pd.Timedelta(seconds=horizon_seconds)
    mids_h = mids.rename(columns={"ts": "ts_h", "mid_price": "mid_h"})
    tr = pd.merge_asof(tr.sort_values("ts_h"), mids_h, on="ts_h", direction="backward").dropna(subset=["mid0", "mid_h"])

    tr["log_r"] = np.log(tr["mid_h"] / tr["mid0"])
    tr["minute"] = tr["ts"].dt.floor("min")

    def _ols(g: pd.DataFrame) -> pd.Series:
        x = g["sign"].astype(float).to_numpy()
        y = g["log_r"].astype(float).to_numpy()
        if x.size < 2 or np.var(x) == 0.0:
            return pd.Series({"alpha": np.nan, "beta": np.nan, "n": x.size})
        X = np.column_stack([np.ones_like(x), x])
        alpha, beta = np.linalg.lstsq(X, y, rcond=None)[0]
        return pd.Series({"alpha": alpha, "beta": beta, "n": x.size})

    out = tr.groupby("minute", observed=True).apply(_ols).reset_index()
    return out.rename(columns={"beta": f"beta_{horizon_seconds}s"})

def build_mid_and_px_series(l2: pd.DataFrame, trades: pd.DataFrame) -> Dict[str, pd.Series]:
    mid_1s = l2[["mid_price"]].groupby("ts").last()["mid_price"]
    mid_1m = mid_1s.resample("1min", label="left", closed="left").last().ffill()

    px_1s = trades.set_index("ts")["px"].resample("1s", label="left", closed="left").last()
    px_1m = trades.set_index("ts")["px"].resample("1min", label="left", closed="left").last()

    return {"mid_1s": mid_1s, "mid_1m": mid_1m, "px_1s": px_1s, "px_1m": px_1m}

def log_returns(series: pd.Series) -> pd.Series:
    s = series.dropna()
    return np.log(s).diff().dropna()

def realized_variance(returns: pd.Series) -> float:
    r = returns.dropna().astype(float)
    return float((r * r).sum())

def acf_np(x: Iterable[float], nlags: int = 10) -> np.ndarray:
    x = np.asarray(list(pd.Series(x).dropna()), dtype=float)
    x = x - np.nanmean(x)
    denom = np.nansum(x * x)
    out = []
    for k in range(1, nlags + 1):
        num = np.nansum(x[k:] * x[:-k])
        out.append(num / denom if denom > 0 else np.nan)
    return np.array(out)
