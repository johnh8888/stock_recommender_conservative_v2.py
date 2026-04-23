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

# 测试模式：默认开启（本地运行可直接出结果），正式环境可通过环境变量 TEST_MODE=0 关闭
TEST_MODE = os.getenv("TEST_MODE", "1") == "1"   # 默认为 True，即忽略时间限制

TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

# 交易规则：仅周一到周四 10:00 执行；测试模式可忽略时间限制
TRADE_WEEKDAYS = {0, 1, 2, 3}
TRADE_HOUR = 10

# 风控参数
LOW_BUY_RATIO = 0.999
HARD_STOP = 0.988
MAX_ACCEPTABLE_MARKET_DROP = -0.4   # 微量放宽大盘容忍度

# 手续费与净利润目标（A股普通估算）
BUY_FEE_RATE = 0.0003
SELL_FEE_RATE = 0.0003
SELL_TAX_RATE = 0.0005
ROUND_TRIP_FEE_RATE = BUY_FEE_RATE + SELL_FEE_RATE + SELL_TAX_RATE
NET_PROFIT_TARGET_MIN = 250
NET_PROFIT_TARGET_MAX = 350

# 筛选条件（微量放宽版）
MIN_PRICE = 8
MAX_PRICE = 30
MIN_PCT = 1.2
MAX_PCT = 3.8
MIN_AMOUNT = 2.0e8
MIN_LB = 1.2
MAX_LB = 2.0
MIN_TURNOVER = 2.5
MAX_TURNOVER = 9.0
MIN_AMPLITUDE = 1.8
MAX_AMPLITUDE = 6.5
MAX_OPEN_PCT = 2.0
MIN_SCORE_THRESHOLD = 8.0
TOP_N_CANDIDATES = 5
BACKTEST_LOOKBACK_DAYS = 150
BACKTEST_MIN_SIGNALS = 4

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()
current_hour = now.hour


def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        requests.post(
            "http://www.pushplus.plus/send",
            json={"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "markdown"},
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
    return df[col] if col in df.columns else pd.Series([default] * len(df), index=df.index)


def calc_open_pct(row: pd.Series) -> float:
    prev_close = safe_float(row.get("昨收"), 0.0)
    open_price = safe_float(row.get("今开"), 0.0)
    if prev_close <= 0 or open_price <= 0:
        return np.nan
    return (open_price / prev_close - 1) * 100


def get_next_trade_day_text(base_dt: datetime) -> str:
    """基于真实交易日历计算下一交易日"""
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        if trade_cal is not None and not trade_cal.empty:
            trade_dates = sorted(trade_cal["trade_date"].astype(str).tolist())
            base_str = base_dt.strftime("%Y-%m-%d")
            for d in trade_dates:
                if d > base_str:
                    return d.replace("-", "")
        # 降级方案：简单跳过周末
        candidate = base_dt + timedelta(days=1)
        while candidate.weekday() >= 5:
            candidate += timedelta(days=1)
        return candidate.strftime("%Y%m%d")
    except Exception:
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


def evaluate_stock_history(symbol: str) -> dict:
    """回测买入价使用当日开盘价（更贴近10:00实际执行）"""
    start_date = (now - timedelta(days=BACKTEST_LOOKBACK_DAYS + 40)).strftime("%Y%m%d")
    end_date = today
    try:
        hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception:
        return {
            "signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
            "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999
        }

    if hist is None or hist.empty:
        return {
            "signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
            "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999
        }

    hist = hist.rename(columns={
        "日期": "date", "开盘": "open", "收盘": "close", "最高": "high",
        "最低": "low", "涨跌幅": "pct", "成交额": "amount", "换手率": "turnover", "振幅": "amplitude"
    }).copy()
    hist = hist.sort_values("date").tail(BACKTEST_LOOKBACK_DAYS).reset_index(drop=True)
    hist["open_pct"] = (hist["open"] / hist["close"].shift(1) - 1) * 100

    signals = []
    for i in range(1, len(hist) - 1):
        row = hist.iloc[i]
        next_row = hist.iloc[i + 1]
        if not (
            MIN_PCT <= safe_float(row["pct"]) <= MAX_PCT and
            safe_float(row["amount"]) >= MIN_AMOUNT and
            MIN_TURNOVER <= safe_float(row.get("turnover"), 0.0) <= MAX_TURNOVER and
            MIN_AMPLITUDE <= safe_float(row.get("amplitude"), 0.0) <= MAX_AMPLITUDE and
            MIN_PRICE <= safe_float(row["close"]) <= MAX_PRICE and
            safe_float(row.get("open_pct"), 999.0) <= MAX_OPEN_PCT
        ):
            continue

        buy_price = safe_float(row["open"])
        next_high = safe_float(next_row["high"])
        next_close = safe_float(next_row["close"])
        next_low = safe_float(next_row["low"])
        if min(buy_price, next_high, next_close, next_low) <= 0:
            continue

        buy_ref = buy_price * LOW_BUY_RATIO
        target_250_sell = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
        target_350_sell = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)

        signals.append({
            "win": 1 if next_close > buy_price else 0,
            "target_250_hit": 1 if next_high >= target_250_sell else 0,
            "target_350_hit": 1 if next_high >= target_350_sell else 0,
            "next_close_ret": (next_close / buy_price - 1) * 100,
            "next_high_ret": (next_high / buy_price - 1) * 100,
            "next_low_ret": (next_low / buy_price - 1) * 100,
        })

    if not signals:
        return {
            "signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
            "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
            "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999
        }

    s = pd.DataFrame(signals)
    signal_count = int(len(s))
    win_rate = float(s["win"].mean() * 100)
    target_250_hit_rate = float(s["target_250_hit"].mean() * 100)
    target_350_hit_rate = float(s["target_350_hit"].mean() * 100)
    avg_next_close = float(s["next_close_ret"].mean())
    avg_next_high = float(s["next_high_ret"].mean())
    avg_worst_drawdown = float(s["next_low_ret"].mean())

    sample_penalty = min(signal_count, 15) / 15
    history_score = (
        win_rate * 0.22 +
        target_250_hit_rate * 0.38 +
        target_350_hit_rate * 0.22 +
        avg_next_close * 10.0 +
        avg_next_high * 5.0 +
        avg_worst_drawdown * 2.0
    ) * sample_penalty

    return {
        "signals": signal_count,
        "win_rate": win_rate,
        "target_250_hit_rate": target_250_hit_rate,
        "target_350_hit_rate": target_350_hit_rate,
        "avg_next_close": avg_next_close,
        "avg_next_high": avg_next_high,
        "avg_worst_drawdown": avg_worst_drawdown,
        "history_score": history_score,
    }


# ==================== 执行控制 ====================
# 测试模式默认开启，正式环境请设置环境变量 TEST_MODE=0
if not TEST_MODE:
    if week_num not in TRADE_WEEKDAYS or current_hour != TRADE_HOUR:
        print("当前非周一到周四 10:00，脚本未执行")
        sys.exit(0)

print("=== 开始运行选股脚本（测试模式已开启） ===" if TEST_MODE else "=== 正式模式运行中 ===")

raw_df = ak.stock_zh_a_spot_em()
try:
    sh_row = raw_df[raw_df["名称"] == "上证指数"]
    market_pct = float(sh_row["涨跌幅"].iloc[0]) if not sh_row.empty else 0.0
except Exception:
    market_pct = 0.0

if market_is_weak(market_pct):
    print("市场偏弱，今日空仓")
    sys.exit(0)

df = raw_df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct",
    "成交额": "amount", "量比": "lb", "换手率": "turnover", "振幅": "amplitude",
    "今开": "open", "昨收": "prev_close"
}).copy()
df["open_pct"] = raw_df.apply(calc_open_pct, axis=1)
df["turnover"] = get_col(df, "turnover", np.nan)
df["amplitude"] = get_col(df, "amplitude", np.nan)
df["open_pct"] = get_col(df, "open_pct", np.nan)

ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].astype(str).str.startswith("60")) | (df["code"].astype(str).str.startswith("00"))]

filtered = df[
    (df["price"] >= MIN_PRICE) & (df["price"] <= MAX_PRICE) &
    (df["pct"] >= MIN_PCT) & (df["pct"] <= MAX_PCT) &
    (df["amount"] >= MIN_AMOUNT) &
    (df["lb"] >= MIN_LB) & (df["lb"] <= MAX_LB) &
    (df["turnover"] >= MIN_TURNOVER) & (df["turnover"] <= MAX_TURNOVER) &
    (df["amplitude"] >= MIN_AMPLITUDE) & (df["amplitude"] <= MAX_AMPLITUDE) &
    (df["open_pct"] <= MAX_OPEN_PCT)
].copy()

if filtered.empty:
    print("今日无高质量标的，空仓")
    sys.exit(0)

filtered["realtime_score"] = (
    filtered["pct"] * 1.35 +
    filtered["lb"] * 2.2 +
    (filtered["amount"] / 1e8) * 0.8 +
    filtered["turnover"] * 0.75 -
    filtered["amplitude"] * 0.45 -
    filtered["open_pct"].fillna(0) * 0.6
)

candidates = filtered.sort_values("realtime_score", ascending=False).head(TOP_N_CANDIDATES).copy()
if candidates.empty:
    print("今日无候选，空仓")
    sys.exit(0)

history_rows = [evaluate_stock_history(str(row["code"])) for _, row in candidates.iterrows()]
if not history_rows:
    print("历史样本不足，空仓")
    sys.exit(0)

candidates = pd.concat([candidates.reset_index(drop=True), pd.DataFrame(history_rows)], axis=1)
candidates = candidates[candidates["signals"] >= BACKTEST_MIN_SIGNALS].copy()
if candidates.empty:
    print("历史样本不足，空仓")
    sys.exit(0)

candidates["final_score"] = candidates["realtime_score"] * 0.28 + candidates["history_score"] * 0.72
candidates = candidates[candidates["final_score"] >= MIN_SCORE_THRESHOLD].sort_values("final_score", ascending=False).reset_index(drop=True)

if candidates.empty:
    print("评分不足，空仓")
    sys.exit(0)

stock = candidates.iloc[0]
p = safe_float(stock["price"])
buy_ref = round(p * LOW_BUY_RATIO, 2)
stop = round(p * HARD_STOP, 2)
next_sell_day = get_next_trade_day_text(now)

target_sell_min = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
target_sell_max = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)
net_profit_min = round(calc_net_profit(target_sell_min, buy_ref, FIX_AMOUNT), 2)
net_profit_max = round(calc_net_profit(target_sell_max, buy_ref, FIX_AMOUNT), 2)
net_stop_loss = round(calc_net_profit(stop, buy_ref, FIX_AMOUNT), 2)

best = candidates.head(3)[["code", "name", "pct", "signals", "win_rate", "target_250_hit_rate", "target_350_hit_rate", "final_score"]].copy()
lines = []
for _, row in best.iterrows():
    lines.append(
        f"- {row['name']}({row['code']})｜现涨 {safe_float(row['pct']):.2f}%｜"
        f"胜率 {safe_float(row['win_rate']):.1f}%｜"
        f"250命中 {safe_float(row['target_250_hit_rate']):.1f}%｜"
        f"350命中 {safe_float(row['target_350_hit_rate']):.1f}%"
    )

content = f"""
## {today} 10:00 最强赚钱候选
- 股票：{stock['name']}({stock['code']})
- 现价：{p:.2f}
- 计划买入参考：{buy_ref}
- 止盈区间（净利{NET_PROFIT_TARGET_MIN}~{NET_PROFIT_TARGET_MAX}元）：{target_sell_min} ~ {target_sell_max}
- 止损价格：{stop}
- 次日卖出日：{next_sell_day}
- 到手净利预计：{net_profit_min} ~ {net_profit_max}
- 止损预计亏损：{net_stop_loss}
- 历史样本：{int(stock['signals'])}
- 次日收盘上涨胜率：{safe_float(stock['win_rate']):.1f}%
- 到手250元命中率：{safe_float(stock['target_250_hit_rate']):.1f}%
- 到手350元命中率：{safe_float(stock['target_350_hit_rate']):.1f}%

### 前3候选
{os.linesep.join(lines)}
""".strip()

push("10点最强候选", content)
print(f"今日推荐：{stock['name']}({stock['code']})")
