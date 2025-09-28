from __future__ import annotations
from typing import Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def _style(ax, title: str, ylabel: str):
    ax.set_title(title, loc="left")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.25)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    loc = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc))
    return ax

def line_series(s: pd.Series, title: str, ylabel: str, savepath: Optional[str] = None):
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(s.index, s.values)
    _style(ax, title, ylabel)
    fig.tight_layout()
    if savepath:
        fig.savefig(savepath, dpi=300)
    plt.close(fig)

def bar_minute(s: pd.Series, title: str, ylabel: str, savepath: Optional[str] = None):
    s = s.copy()
    s.index = pd.to_datetime(s.index)
    fig, ax = plt.subplots(figsize=(12, 4))
    step = np.median(np.diff(mdates.date2num(s.index.values))) if len(s) > 1 else 1.0 / 1440.0
    ax.bar(s.index, s.values, width=step * 0.85, align="center")
    _style(ax, title, ylabel)
    fig.tight_layout()
    if savepath:
        fig.savefig(savepath, dpi=300)
    plt.close(fig)
