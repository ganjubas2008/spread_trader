
# How to run the project:

**Preliminary commands:**

```bash
pip install -r requirements.txt
chmod +x run.sh preprocess.sh
```

**Data preprocessing:**

```bash
./preprocess.sh
```

**Backtesting:**

```bash
./backtest.sh
```
Note that you need to preprocess data before running backtest.
______

# Project Description:

This repository provides a complete backtesting framework for evaluating a trading strategy, covering everything from raw data preprocessing to calculating various performance metrics.

### Architecture:
1) **Preprocessing**:  
   - `./preprocess.sh` takes the path to raw market data and converts it into a more lightweight Parquet (`.pqt`) format. The processed market snapshots are stored in the `data/preprocessed_data/pqt` directory.  
   - The script then extracts all market actions (e.g., placing, modifying, or canceling orders) from the order book data. These actions are stored in the `data/preprocessed_data/actions` directory.

2) **Running the Backtest**:  
   - The `ActionStream` class processes market actions in chunks from `data/preprocessed_data/actions` and yields them one-by-one, simulating a real-time market data stream.  
   - Each time a market action occurs, the `SpreadTrader` class evaluates whether a trading opportunity is present.  
   - If a valid opportunity is detected, `SpreadTrader` executes a trade, taking into account current market liquidity and updating the `Portfolio` accordingly.  
   - After a predefined timestamp (e.g., `16:00` each day), `SpreadTrader` begins unwinding open positions to close exposures.

### Strategy Essentials:
As detailed in the research section of the project, the **Order Book Imbalance (OBI)** metric is used as a key trading signal.

$$\text{OBI} := \frac{Volume_{bid} - Volume_{ask}}{Volume_{bid} + Volume_{ask}}$$

A trade is executed when:

$$\text{OBI}_A - \text{OBI}_B > \delta_{const} \quad \text{or} \quad \text{OBI}_A - \text{OBI}_B < -\delta_{const}$$  

where **A** and **B** are different financial instruments (e.g., Spot and Perpetual Futures).  

However, this static approach is suboptimal. Future versions should improve upon it by dynamically adjusting **$δ$** instead of relying on a fixed threshold **$δ_{const}$**.
