import os
import sys
import warnings
from datetime import datetime, timedelta

import akshare as ak
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ==================== 配置参数 ====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

# 交易规则：仅周一到周四 10:00 执行
TRADE_WEEKDAYS = {0, 1, 2, 3}
TRADE_HOUR = 10

# 风控参数
LOW_BUY_RATIO = 0.998
HARD_STOP = 0.985
MAX_ACCEPTABLE_MARKET_DROP = -0.8

# 手续费与净利润目标（A股普通估算）
BUY_FEE_RATE = 0.0003
SELL_FEE_RATE = 0.0003
SELL_TAX_RATE = 0.0005
ROUND_TRIP_FEE_RATE = BUY_FEE_RATE + SELL_FEE_RATE + SELL_TAX_RATE
NET_PROFIT_TARGET_MIN = 250
NET_PROFIT_TARGET_MAX = 350

# 筛选参数：B 平衡型
MIN_PRICE = 5
MAX_PRICE = 50
MIN_PCT = 0.6
MAX_PCT = 4.2
MIN_AMOUNT = 1.2e8
MIN_LB = 1.15
MAX_LB = 2.4
MIN_TURNOVER = 1.5
MAX_TURNOVER = 12.0
MIN_AMPLITUDE = 1.2
MAX_AMPLITUDE = 7.0
MAX_OPEN_PCT = 2.8

# 回测参数
BACKTEST_LOOKBACK_DAYS = 120
BACKTEST_MIN_SIGNALS = 3

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()
current_hour = now.hour


# ==================== 工具函数 ====================
def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        requests.post(
            "http://www.pushplus.plus/send",
            json={
                "token": PUSHPLUS_TOKEN,
                "title": title,
                "content": content,
                "template": "markdown",
            },
            timeout=10,
        )


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def get_col(df: pd.DataFrame, col: str, default=np.nan):
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df), index=df.index)


def calc_open_pct(row: pd.Series) -> float:
    prev_close = safe_float(row.get("昨收"), 0.0)
    open_price = safe_float(row.get("今开"), 0.0)
    if prev_close <= 0 or open_price <= 0:
        return np.nan
    return (open_price / prev_close - 1) * 100


def get_next_trade_day_text(base_dt: datetime) -> str:
    candidate = base_dt + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def market_is_weak(market_pct: float) -> bool:
    return market_pct <= MAX_ACCEPTABLE_MARKET_DROP


def calc_net_profit(sell_price: float, buy_price: float, capital: float) -> float:
    if buy_price <= 0 or sell_price <= 0 or capital <= 0:
        return 0.0
    shares = capital / buy_price
    gross_profit = (sell_price - buy_price) * shares
    fees = capital * BUY_FEE_RATE + (shares * sell_price) * (SELL_FEE_RATE + SELL_TAX_RATE)
    return gross_profit - fees


def calc_target_sell_price(buy_price: float, capital: float, net_profit_target: float) -> float:
    if buy_price <= 0 or capital <= 0:
        return 0.0
    target_ratio = 1 + ROUND_TRIP_FEE_RATE + (net_profit_target / capital)
    return round(buy_price * target_ratio, 2)


def calc_stop_loss_amount(buy_price: float, stop_price: float, capital: float) -> float:
    return round(calc_net_profit(stop_price, buy_price, capital), 2)


# ==================== 历史隔日统计 ====================
def evaluate_stock_history(symbol: str) -> dict:
    start_date = (now - timedelta(days=BACKTEST_LOOKBACK_DAYS + 40)).strftime("%Y%m%d")
    end_date = today

    try:
        hist = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
    except Exception:
        return {
            "signals": 0,
            "win_rate": 0.0,
            "avg_next_close": 0.0,
            "avg_next_high": 0.0,
            "avg_worst_drawdown": 0.0,
            "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0,
            "history_score": -999,
        }

    if hist is None or hist.empty:
        return {
            "signals": 0,
            "win_rate": 0.0,
            "avg_next_close": 0.0,
            "avg_next_high": 0.0,
            "avg_worst_drawdown": 0.0,
            "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0,
            "history_score": -999,
        }

    hist = hist.rename(
        columns={
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "涨跌幅": "pct",
            "成交额": "amount",
            "换手率": "turnover",
            "振幅": "amplitude",
        }
    ).copy()

    required = ["open", "close", "high", "low", "pct", "amount"]
    for col in required:
        if col not in hist.columns:
            return {
                "signals": 0,
                "win_rate": 0.0,
                "avg_next_close": 0.0,
                "avg_next_high": 0.0,
                "avg_worst_drawdown": 0.0,
                "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0,
                "history_score": -999,
            }

    hist = hist.sort_values("date").tail(BACKTEST_LOOKBACK_DAYS).reset_index(drop=True)
    hist["open_pct"] = (hist["open"] / hist["close"].shift(1) - 1) * 100

    signals = []
    for i in range(1, len(hist) - 1):
        row = hist.iloc[i]
        next_row = hist.iloc[i + 1]

        if not (
            MIN_PCT <= safe_float(row["pct"]) <= MAX_PCT
            and safe_float(row["amount"]) >= MIN_AMOUNT
            and MIN_TURNOVER <= safe_float(row.get("turnover"), 0.0) <= MAX_TURNOVER
            and MIN_AMPLITUDE <= safe_float(row.get("amplitude"), 0.0) <= MAX_AMPLITUDE
            and safe_float(row["close"]) >= MIN_PRICE
            and safe_float(row["close"]) <= MAX_PRICE
            and safe_float(row.get("open_pct"), 999.0) <= MAX_OPEN_PCT
        ):
            continue

        signal_close = safe_float(row["close"])
        if signal_close <= 0:
            continue

        next_open = safe_float(next_row["open"])
        next_close = safe_float(next_row["close"])
        next_high = safe_float(next_row["high"])
        next_low = safe_float(next_row["low"])
        if min(next_open, next_close, next_high, next_low) <= 0:
            continue

        next_close_ret = (next_close / signal_close - 1) * 100
        next_high_ret = (next_high / signal_close - 1) * 100
        next_low_ret = (next_low / signal_close - 1) * 100
        win = 1 if next_close_ret > 0 else 0

        buy_ref = signal_close * LOW_BUY_RATIO
        target_250_sell = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
        target_350_sell = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)
        target_250_hit = 1 if next_high >= target_250_sell else 0
        target_350_hit = 1 if next_high >= target_350_sell else 0

        signals.append(
            {
                "next_close_ret": next_close_ret,
                "next_high_ret": next_high_ret,
                "next_low_ret": next_low_ret,
                "win": win,
                "target_250_hit": target_250_hit,
                "target_350_hit": target_350_hit,
            }
        )

    if not signals:
        return {
            "signals": 0,
            "win_rate": 0.0,
            "avg_next_close": 0.0,
            "avg_next_high": 0.0,
            "avg_worst_drawdown": 0.0,
            "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0,
            "history_score": -999,
        }

    signals_df = pd.DataFrame(signals)
    signal_count = int(len(signals_df))
    win_rate = float(signals_df["win"].mean() * 100)
    avg_next_close = float(signals_df["next_close_ret"].mean())
    avg_next_high = float(signals_df["next_high_ret"].mean())
    avg_worst_drawdown = float(signals_df["next_low_ret"].mean())
    target_250_hit_rate = float(signals_df["target_250_hit"].mean() * 100)
    target_350_hit_rate = float(signals_df["target_350_hit"].mean() * 100)

    sample_penalty = min(signal_count, 12) / 12
    history_score = (
        win_rate * 0.30
        + target_250_hit_rate * 0.35
        + target_350_hit_rate * 0.20
        + avg_next_close * 10.0
        + avg_next_high * 6.0
        + avg_worst_drawdown * 2.0
    ) * sample_penalty

    return {
        "signals": signal_count,
        "win_rate": win_rate,
        "avg_next_close": avg_next_close,
        "avg_next_high": avg_next_high,
        "avg_worst_drawdown": avg_worst_drawdown,
        "target_250_hit_rate": target_250_hit_rate,
        "target_350_hit_rate": target_350_hit_rate,
        "history_score": history_score,
    }


# ==================== 交易时间控制 ====================
if week_num not in TRADE_WEEKDAYS:
    msg = (
        f"## {today} 非交易执行日\n\n"
        "当前规则仅在周一到周四 10:00 选股并买入，次日卖出；周五到周日不新开仓。\n\n"
        "脚本只做概率筛选，不保证盈利。"
    )
    push("非交易执行日提醒", msg)
    print("当前不是周一到周四，停止执行")
    sys.exit(0)

if current_hour != TRADE_HOUR:
    msg = (
        f"## {today} 非执行时段\n\n"
        "当前时间不是 10:00，按规则不执行选股与买入。\n\n"
        "请在周一到周四 10:00 运行脚本。"
    )
    push("非执行时段提醒", msg)
    print("当前不是 10:00，停止执行")
    sys.exit(0)

# ==================== 获取实时市场数据 ====================
raw_df = ak.stock_zh_a_spot_em()

try:
    sh_row = raw_df[raw_df["名称"] == "上证指数"]
    market_pct = float(sh_row["涨跌幅"].iloc[0]) if not sh_row.empty else 0.0
except Exception:
    market_pct = 0.0

if market_is_weak(market_pct):
    content = (
        f"## {today} 10:00 市场过滤\n\n"
        f"当前上证指数涨跌幅：{market_pct:.2f}%\n\n"
        "市场偏弱，今日执行空仓，避免硬做。\n\n"
        "说明：这是风险控制，不代表后续一定下跌。"
    )
    push("今日空仓｜市场偏弱", content)
    print("市场偏弱，今日空仓")
    sys.exit(0)

df = raw_df.rename(
    columns={
        "代码": "code",
        "名称": "name",
        "最新价": "price",
        "涨跌幅": "pct",
        "成交额": "amount",
        "量比": "lb",
        "换手率": "turnover",
        "振幅": "amplitude",
        "今开": "open",
        "昨收": "prev_close",
    }
).copy()

df["open_pct"] = raw_df.apply(calc_open_pct, axis=1)
df["turnover"] = get_col(df, "turnover", np.nan)
df["amplitude"] = get_col(df, "amplitude", np.nan)
df["open_pct"] = get_col(df, "open_pct", np.nan)

# ==================== 过滤高危标的 ====================
ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[
    (df["code"].astype(str).str.startswith("60"))
    | (df["code"].astype(str).str.startswith("00"))
]

# ==================== B 平衡型实时筛选 ====================
filtered = df[
    (df["price"] >= MIN_PRICE)
    & (df["price"] <= MAX_PRICE)
    & (df["pct"] >= MIN_PCT)
    & (df["pct"] <= MAX_PCT)
    & (df["amount"] >= MIN_AMOUNT)
    & (df["lb"] >= MIN_LB)
    & (df["lb"] <= MAX_LB)
    & (df["turnover"] >= MIN_TURNOVER)
    & (df["turnover"] <= MAX_TURNOVER)
    & (df["amplitude"] >= MIN_AMPLITUDE)
    & (df["amplitude"] <= MAX_AMPLITUDE)
    & (df["open_pct"] <= MAX_OPEN_PCT)
].copy()

stock = None
candidate_table = None

if filtered.empty:
    content = (
        f"## {today} 10:00 实时选股\n\n"
        f"大盘涨跌：{market_pct:.2f}%\n\n"
        "今日没有同时满足实时条件的标的，按规则空仓。\n\n"
        "空仓也是策略的一部分。"
    )
    push("今日无高质量标的", content)
else:
    filtered["realtime_score"] = (
        filtered["pct"] * 1.4
        + filtered["lb"] * 2.5
        + (filtered["amount"] / 1e8) * 0.9
        + filtered["turnover"] * 0.7
        - filtered["amplitude"] * 0.35
        - filtered["open_pct"].fillna(0) * 0.5
    )

    candidates = filtered.sort_values("realtime_score", ascending=False).head(8).copy()

    history_rows = []
    for _, row in candidates.iterrows():
        hist_stat = evaluate_stock_history(str(row["code"]))
        history_rows.append(hist_stat)

    history_df = pd.DataFrame(history_rows, index=candidates.index)
    candidates = pd.concat([candidates, history_df], axis=1)
    candidates = candidates[candidates["signals"] >= BACKTEST_MIN_SIGNALS].copy()

    if candidates.empty:
        content = (
            f"## {today} 10:00 实时选股\n\n"
            f"大盘涨跌：{market_pct:.2f}%\n\n"
            "有实时强势股，但历史隔日样本不足，今日放弃交易。\n\n"
            "说明：这是为了减少低样本误判。"
        )
        push("今日放弃交易｜样本不足", content)
        print("样本不足，今日空仓")
        sys.exit(0)

    candidates["final_score"] = (
        candidates["realtime_score"] * 0.55
        + candidates["history_score"] * 0.45
    )
    candidates = candidates.sort_values("final_score", ascending=False).reset_index(drop=True)

    stock = candidates.iloc[0]
    p = safe_float(stock["price"])
    buy_ref = round(p * LOW_BUY_RATIO, 2)
    stop = round(p * HARD_STOP, 2)
    next_sell_day = get_next_trade_day_text(now)

    target_sell_min = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
    target_sell_max = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)
    net_profit_min = round(calc_net_profit(target_sell_min, buy_ref, FIX_AMOUNT), 2)
    net_profit_max = round(calc_net_profit(target_sell_max, buy_ref, FIX_AMOUNT), 2)
    net_stop_loss = calc_stop_loss_amount(buy_ref, stop, FIX_AMOUNT)

    candidate_table = candidates.head(3)[
        [
            "code",
            "name",
            "price",
            "pct",
            "signals",
            "win_rate",
            "target_250_hit_rate",
            "target_350_hit_rate",
            "avg_next_close",
            "avg_next_high",
            "final_score",
        ]
    ].copy()

    csv_file = "recommendation_history.csv"
    new_row = pd.DataFrame(
        [
            {
                "date": today,
                "code": stock["code"],
                "name": stock["name"],
                "price": p,
                "buy_ref": buy_ref,
                "target_sell_min": target_sell_min,
                "target_sell_max": target_sell_max,
                "pct": safe_float(stock["pct"]),
                "win_rate": safe_float(stock["win_rate"]),
                "target_250_hit_rate": safe_float(stock["target_250_hit_rate"]),
                "target_350_hit_rate": safe_float(stock["target_350_hit_rate"]),
                "avg_next_close": safe_float(stock["avg_next_close"]),
                "avg_next_high": safe_float(stock["avg_next_high"]),
                "final_score": safe_float(stock["final_score"]),
            }
        ]
    )

    if os.path.exists(csv_file):
        history_df = pd.read_csv(csv_file)
        already_logged = (
            (history_df["date"].astype(str) == str(today))
            & (history_df["code"].astype(str) == str(stock["code"]))
        ).any()
        if not already_logged:
            new_row.to_csv(csv_file, mode="a", header=False, index=False, encoding="utf-8-sig")
        count = len(pd.read_csv(csv_file))
    else:
        new_row.to_csv(csv_file, mode="w", header=True, index=False, encoding="utf-8-sig")
        count = 1

    top3_lines = []
    for _, row in candidate_table.iterrows():
        top3_lines.append(
            f"- {row['name']}({row['code']})｜现涨 {safe_float(row['pct']):.2f}%｜"
            f"历史胜率 {safe_float(row['win_rate']):.1f}%｜"
            f"到手250命中 {safe_float(row['target_250_hit_rate']):.1f}%｜"
            f"到手350命中 {safe_float(row['target_350_hit_rate']):.1f}%"
        )
    top3_text = "\n".join(top3_lines)

    content = f"""
## {today} 10:00 A股实时推荐（平衡型回测版）
**大盘当前涨跌**：{market_pct:.2f}%
**说明**：使用 10:00 左右最新A股实时数据 + 候选股近 {BACKTEST_LOOKBACK_DAYS} 天历史隔日表现综合评分，选出当前规则下更优先的标的。

### 今日优先标的
- **股票**：{stock['name']}({stock['code']})
- **现价**：{p:.2f}
- **当前涨幅**：{safe_float(stock['pct']):.2f}%
- **计划买入参考**：{buy_ref}
- **止盈价格区间**：{target_sell_min} ~ {target_sell_max}
- **止损价格**：{stop}
- **计划卖出日**：{next_sell_day}

### 止盈按到手净利润计算
- **计划买入金额**：{FIX_AMOUNT} 元
- **手续费估算总费率**：{ROUND_TRIP_FEE_RATE * 100:.2f}%
- **目标到手净利润区间**：{NET_PROFIT_TARGET_MIN} ~ {NET_PROFIT_TARGET_MAX} 元
- **对应止盈后预计到手**：{net_profit_min} ~ {net_profit_max} 元
- **触发止损预计亏损**：{net_stop_loss} 元

### 历史隔日表现参考
- 历史触发样本：{int(stock['signals'])} 次
- 次日收盘上涨胜率：{safe_float(stock['win_rate']):.1f}%
- 到手净利250元命中率：{safe_float(stock['target_250_hit_rate']):.1f}%
- 到手净利350元命中率：{safe_float(stock['target_350_hit_rate']):.1f}%
- 次日平均收盘收益：{safe_float(stock['avg_next_close']):.2f}%
- 次日平均最高收益：{safe_float(stock['avg_next_high']):.2f}%
- 次日平均最大回撤：{safe_float(stock['avg_worst_drawdown']):.2f}%

### 今日前3候选
{top3_text}

### 执行纪律
1. 仅在周一到周四 10:00 运行并参考结果
2. 若盘中快速拉高并明显偏离买入参考，可放弃，不强追
3. 次日优先执行卖出，不恋战
4. 当日没有高质量标的时允许空仓
5. 历史回测只提供概率参考，不代表未来必然赚钱

累计推荐记录：{count} 条
"""
    push("10点实时推荐｜平衡型回测版", content)

print(f"运行完成｜今日标的：{stock['name'] if stock is not None else '无'}")
