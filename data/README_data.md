Data layout

Equities
  Put event files for each symbol in a folder
  Example paths
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
  You can rename columns in scripts if your headers are slightly different

FX
  Two wide tables exported from your vendor
  Files
    data/fx/orders.csv
    data/fx/trades.csv
  Each table contains repeated blocks for each pair
    EBS_BOOK::EUR/USD.PRICE and similar for other fields
    EBS_TRADE::EUR/USD.PRICE and similar for other fields
  The fx pipeline reads the blocks and builds a tidy panel
