from __future__ import annotations
import pathlib
from microstructure.equities_pipeline import run_equity_day

def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    data_root = root / "data"
    figs_root = root / "figures" / "equities"
    figs_root.mkdir(parents=True, exist_ok=True)

    run_equity_day(
        csv_path=str(data_root / "msft" / "mbo.csv"),
        symbol="MSFT",
        session_date="2025-07-22",
        out_dir=str(figs_root / "msft"),
        price_in_nanos=True,
    )

    run_equity_day(
        csv_path=str(data_root / "qubt" / "mbo.csv"),
        symbol="QUBT",
        session_date="2025-07-30",
        out_dir=str(figs_root / "qubt"),
        price_in_nanos=False,
    )

if __name__ == "__main__":
    main()
