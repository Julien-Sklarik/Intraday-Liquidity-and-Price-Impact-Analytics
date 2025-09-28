# Intraday Liquidity and Price Impact Analytics

I turned my UC Berkeley microstructure work into a clean research project that a hiring manager can clone and run. 
I analyze one day in US equities and one day in major FX pairs. I rebuild top of book liquidity, compute spreads and depth, 
estimate five second price impact, and report realized variance and auto correlation. 
I kept the code small, readable, and production friendly.

## What I built

1. Equities day study for MSFT and QUBT  
   I read full event streams, build minute and second panels, reconstruct the level two view at the end of each time bucket, 
   compute spread and depth near the touch, and estimate five second price impact with a rolling cross sectional regression inside each minute.

2. FX one day study for EUR USD, EUR JPY, and USD JPY  
   I assemble one second and one minute midquote and transaction series from EBS style tables, compute realized variance and auto correlation, 
   and flag one second triangular gaps as a simple arbitrage diagnostic with a capacity proxy based on best depth.

## Why this matters for trading and quant roles

1. I show comfort with limit order books and event data at realistic scale  
2. I separate data, logic, and presentation so the work is easy to extend  
3. I reproduce known stylized facts such as the U shape in activity and the negative lag one in midquote returns

## Repo layout

intraday_microstructure_analytics/
  README.md
  requirements.txt
  .gitignore
  src/
    microstructure/
      __init__.py
      metrics.py
      plots.py
      fx_pipeline.py
      equities_pipeline.py
  scripts/
    run_equities.py
    run_fx.py
  data/
    README_data.md
  figures/
    equities/
    fx/
  notebooks/
    equities_intraday.ipynb
    fx_one_day.ipynb

## Quick start

1. Create a fresh virtual environment  
   python -m venv .venv  
   source .venv/bin/activate  on macOS and Linux  
   .venv\Scripts\activate  on Windows

2. Install packages  
   pip install -r requirements.txt

3. Place inputs under data as described below, then run  
   python scripts/run_equities.py  
   python scripts/run_fx.py

## Data inputs

1. Equities  
   Place one csv per symbol  
   data/msft/mbo.csv  
   data/qubt/mbo.csv  
   Required columns  
   ts_event in nanoseconds UTC  
   symbol  
   action with values T A C R F  
   side with values A B for ask and bid and T rows carry the aggressor side in A or B  
   price in nano dollars or in dollars  
   size  
   order_id  
   sequence  
   If your headers differ slightly you can adjust the loader in scripts

2. FX  
   Two csv files exported from your vendor  
   data/fx/orders.csv  
   data/fx/trades.csv  
   The tables contain repeated blocks for each pair with EBS style field names. The fx pipeline reads these blocks and builds a tidy panel.

## Reproduce my figures

1. Equities outputs land in figures/equities  
   dollar volume per minute png  
   vwap per minute png  
   five second price impact png  
   minute and second midquote and transaction series png  
   csv files for OHLC, order counts, spread per minute, depth near the touch, auto correlation, and realized variance

2. FX outputs land in figures/fx  
   one second and one minute midquote and transaction series png  
   csv files for variance, auto correlation, and triangular gap summary

## Notes

I wrote this as my own project and kept it self contained. All figures and tables regenerate from scripts. 
No classroom templates and no course language appear in the repo. If you want me to connect it to your internal data schema I can adapt the loaders quickly.
