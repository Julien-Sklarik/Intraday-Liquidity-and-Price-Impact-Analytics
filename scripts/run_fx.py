from __future__ import annotations
import pathlib
from microstructure.fx_pipeline import build_fx_panels, fx_summary_and_figures

def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    data_root = root / "data" / "fx"
    out_dir = root / "figures" / "fx"

    pairs = ["EUR/USD", "EUR/JPY", "USD/JPY"]

    panels = build_fx_panels(
        order_csv=str(data_root / "orders.csv"),
        trade_csv=str(data_root / "trades.csv"),
        pairs=pairs,
    )
    fx_summary_and_figures(panels, str(out_dir), pairs)

if __name__ == "__main__":
    main()
