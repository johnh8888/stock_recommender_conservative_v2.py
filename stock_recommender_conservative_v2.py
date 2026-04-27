import csv
import os
import sys
import time
import warnings
from datetime import datetime, timedelta

import akshare as ak
import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ==================== 配置参数 ====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
TEST_MODE = os.getenv("TEST_MODE", "1") == "1"

TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

# 交易时间规则（测试模式可忽略）
MORNING_START, MORNING_END = 10, 10.67   # 10:00-10:40
AFTERNOON_START, AFTERNOON_END = 14.67, 14.92  # 14:40-14:55
TRADE_WEEKDAYS = {0, 1, 2, 3}

# 风控参数
LOW_BUY_RATIO = 0.997
HARD_STOP_RATIO = -0.02
MAX_ACCEPTABLE_MARKET_DROP = -0.35

# 手续费
BUY_FEE_RATE = 0.0003
SELL_FEE_RATE = 0.0003
SELL_TAX_RATE = 0.0005
ROUND_TRIP_FEE_RATE = BUY_FEE_RATE + SELL_FEE_RATE + SELL_TAX_RATE
NET_PROFIT_TARGET_MIN = 250
NET_PROFIT_TARGET_MAX = 350

# 早盘筛选条件
MIN_PRICE, MAX_PRICE = 8, 30
MIN_PCT, MAX_PCT = 1.2, 5.5
MIN_AMOUNT = 1.8e8
MIN_LB, MAX_LB = 1.0, 2.5
MIN_TURNOVER, MAX_TURNOVER = 2.5, 9.0
MIN_AMPLITUDE, MAX_AMPLITUDE = 1.8, 7.0
MAX_OPEN_PCT = 2.0

# 尾盘特殊条件
EOD_MAX_PCT = 1.8
EOD_MIN_PCT = -0.3
EOD_MAX_TURNOVER = 4.5
EOD_MIN_LB, EOD_MAX_LB = 0.9, 1.5
EOD_MAX_AMPLITUDE = 3.5

# 技术与评分
MIN_SCORE_THRESHOLD = 7.5
TOP_N_CANDIDATES = 5
BACKTEST_LOOKBACK_DAYS = 180
BACKTEST_MIN_SIGNALS = 4
MIN_CONSECUTIVE_UP = 3

# 大盘过滤 & 基本面排雷
MA20_FILTER = True
FUNDAMENTAL_CHECK = True

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()
current_hour = now.hour + now.minute / 60.0


def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        try:
            requests.post(
                "http://www.pushplus.plus/send",
                json={"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "markdown"},
                timeout=10,
            )
        except Exception:
            pass


def safe_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def get_col(df, col, default=np.nan):
    return df[col] if col in df.columns else pd.Series([default] * len(df), index=df.index)


def calc_open_pct(row):
    prev_close = safe_float(row.get("昨收"), 0.0)
    open_price = safe_float(row.get("今开"), 0.0)
    if prev_close <= 0 or open_price <= 0:
        return np.nan
    return (open_price / prev_close - 1) * 100


def get_next_trade_day_text(base_dt: datetime) -> str:
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        if trade_cal is not None and not trade_cal.empty:
            trade_dates = sorted(trade_cal["trade_date"].astype(str).tolist())
            base_str = base_dt.strftime("%Y-%m-%d")
            for d in trade_dates:
                if d > base_str:
                    return d.replace("-", "")
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


def get_market_ma20_safe():
    """返回大略安全判断(close, ma20, safe)"""
    try:
        index_df = ak.stock_zh_index_daily(symbol="sh000001")
        index_df = index_df.sort_values("date").tail(30)
        close = float(index_df["close"].iloc[-1])
        ma20 = float(index_df["close"].rolling(20).mean().iloc[-1])
        ma20_prev = float(index_df["close"].rolling(20).mean().iloc[-2])
        safe = (close > ma20) and (ma20 > ma20_prev)
        return close, ma20, safe
    except Exception:
        return 0, 0, True


def get_sector_rank_map():
    """返回{板块名称: 涨跌幅}"""
    try:
        sector_df = ak.stock_board_industry_name_em()
        return dict(zip(sector_df["板块名称"], sector_df["涨跌幅"]))
    except Exception:
        return {}


def has_consecutive_mild_up(code: str, days_needed=MIN_CONSECUTIVE_UP):
    """最近days_needed天连续小阳，且20日内无大跌"""
    try:
        end = (now - timedelta(days=1)).strftime("%Y%m%d")
        start = (now - timedelta(days=30)).strftime("%Y%m%d")
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
        if hist is None or hist.empty or len(hist) < days_needed:
            return False
        recent = hist.tail(days_needed + 5)
        pct_col = get_col(recent, "涨跌幅")
        tail_pct = pct_col.tail(days_needed)
        if tail_pct.isna().any():
            return False
        if not tail_pct.between(0.5, 4.5).all():
            return False
        check = pct_col.tail(20)
        if (check < -5).any():
            return False
        return True
    except Exception:
        return False


def has_safe_fundamentals(code: str):
    """归属母公司净利润 > 0"""
    try:
        info = ak.stock_individual_info_em(symbol=code)
        if info is None or info.empty:
            return True
        info_dict = dict(zip(info["item"], info["value"]))
        net_profit = safe_float(info_dict.get("归属母公司股东的净利润", 0), 0)
        return net_profit > 0
    except Exception:
        return True


def evaluate_stock_history(symbol: str) -> dict:
    """回测买入价采用收盘价*低吸比率模拟实盘挂单"""
    start_date = (now - timedelta(days=BACKTEST_LOOKBACK_DAYS + 40)).strftime("%Y%m%d")
    end_date = today
    try:
        hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}

    if hist is None or hist.empty:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}

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
            MIN_TURNOVER <= safe_float(row.get("turnover"), 0) <= MAX_TURNOVER and
            MIN_AMPLITUDE <= safe_float(row.get("amplitude"), 0) <= MAX_AMPLITUDE and
            MIN_PRICE <= safe_float(row["close"]) <= MAX_PRICE and
            safe_float(row.get("open_pct"), 999) <= MAX_OPEN_PCT
        ):
            continue

        buy_price_sim = safe_float(row["close"]) * LOW_BUY_RATIO
        next_high = safe_float(next_row["high"])
        next_close = safe_float(next_row["close"])
        next_low = safe_float(next_row["low"])
        if min(buy_price_sim, next_high, next_close, next_low) <= 0:
            continue

        target_250 = calc_target_sell_price(buy_price_sim, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
        target_350 = calc_target_sell_price(buy_price_sim, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)

        signals.append({
            "win": 1 if next_close > buy_price_sim else 0,
            "target_250_hit": 1 if next_high >= target_250 else 0,
            "target_350_hit": 1 if next_high >= target_350 else 0,
            "next_close_ret": (next_close / buy_price_sim - 1) * 100,
            "next_high_ret": (next_high / buy_price_sim - 1) * 100,
            "next_low_ret": (next_low / buy_price_sim - 1) * 100,
        })

    if not signals:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}

    s = pd.DataFrame(signals)
    signal_count = len(s)
    win_rate = float(s["win"].mean() * 100)
    hit_250 = float(s["target_250_hit"].mean() * 100)
    hit_350 = float(s["target_350_hit"].mean() * 100)
    avg_close = float(s["next_close_ret"].mean())
    avg_high = float(s["next_high_ret"].mean())
    avg_low = float(s["next_low_ret"].mean())

    sample_penalty = min(signal_count, 15) / 15
    history_score = (
        win_rate * 0.22 +
        hit_250 * 0.38 +
        hit_350 * 0.22 +
        avg_close * 9.0 +
        avg_high * 4.5 +
        avg_low * 2.0
    ) * sample_penalty

    return {
        "signals": signal_count, "win_rate": win_rate,
        "target_250_hit_rate": hit_250, "target_350_hit_rate": hit_350,
        "avg_next_close": avg_close, "avg_next_high": avg_high,
        "avg_worst_drawdown": avg_low, "history_score": history_score
    }


# ==================== 主流程 ====================
if not TEST_MODE:
    in_morning = (week_num in TRADE_WEEKDAYS) and (MORNING_START <= current_hour < MORNING_END)
    in_afternoon = (week_num in TRADE_WEEKDAYS) and (AFTERNOON_START <= current_hour < AFTERNOON_END)
    if not (in_morning or in_afternoon):
        print("非允许交易时段，退出")
        sys.exit(0)
else:
    in_morning = True
    in_afternoon = False

# 大盘安全过滤
if MA20_FILTER:
    _, _, ma_safe = get_market_ma20_safe()
    if not ma_safe and not TEST_MODE:
        print("大盘不在20日线上方或均线未向上，暂停开仓")
        sys.exit(0)
    if not ma_safe and TEST_MODE:
        print("⚠️ 测试模式：大盘未满足安全条件，但继续执行")

# ---------- 获取全A实时行情（带重试） ----------
max_retries = 3
raw_df = None
for attempt in range(1, max_retries + 1):
    try:
        raw_df = ak.stock_zh_a_spot_em()
        if raw_df is not None and not raw_df.empty:
            break
        print(f"第 {attempt} 次获取行情为空，重试...")
    except Exception as e:
        print(f"获取行情失败，第 {attempt} 次重试... ({e})")
        time.sleep(5)
else:
    print("多次尝试后仍无法获取行情，退出")
    sys.exit(0)

# 大盘涨跌幅
try:
    sh_row = raw_df[raw_df["名称"] == "上证指数"]
    market_pct = float(sh_row["涨跌幅"].iloc[0]) if not sh_row.empty else 0.0
except Exception:
    market_pct = 0.0

if market_is_weak(market_pct):
    print(f"市场跌幅{market_pct:.2f}%过深，空仓")
    sys.exit(0)

# 数据清洗
df = raw_df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct",
    "成交额": "amount", "量比": "lb", "换手率": "turnover", "振幅": "amplitude",
    "今开": "open", "昨收": "prev_close"
}).copy()
df["open_pct"] = raw_df.apply(calc_open_pct, axis=1)
for col_name in ["turnover", "amplitude", "open_pct"]:
    df[col_name] = get_col(df, col_name, np.nan)

# 排除ST/新股等
ban_pattern = r"(^ST|^\*ST|退市|^N|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].astype(str).str.startswith(("60", "00")))]

# 行业板块效应
sector_map = get_sector_rank_map()
if sector_map:
    sector_pcts = sorted(sector_map.values(), reverse=True)
    cutoff_idx = int(len(sector_pcts) * 0.4)
    cutoff_pct = sector_pcts[cutoff_idx] if len(sector_pcts) > 0 else -100
    # 尝试匹配行业列
    sector_col = None
    for col_name in ["行业", "所属行业"]:
        if col_name in raw_df.columns:
            sector_col = col_name
            break
    if sector_col:
        df["sector"] = raw_df[sector_col]
        df["sector_pct"] = df["sector"].map(sector_map)
        df = df[df["sector_pct"].notna() & (df["sector_pct"] >= cutoff_pct)]

# 早盘筛选
if in_morning:
    filtered = df[
        (df["price"] >= MIN_PRICE) & (df["price"] <= MAX_PRICE) &
        (df["pct"] >= MIN_PCT) & (df["pct"] <= MAX_PCT) &
        (df["amount"] >= MIN_AMOUNT) &
        (df["lb"] >= MIN_LB) & (df["lb"] <= MAX_LB) &
        (df["turnover"] >= MIN_TURNOVER) & (df["turnover"] <= MAX_TURNOVER) &
        (df["amplitude"] >= MIN_AMPLITUDE) & (df["amplitude"] <= MAX_AMPLITUDE) &
        (df["open_pct"] <= MAX_OPEN_PCT)
    ].copy()
    if MIN_CONSECUTIVE_UP > 0:
        filtered = filtered[filtered["code"].apply(has_consecutive_mild_up)]
    print(f"早盘初步筛选出 {len(filtered)} 只")
else:
    filtered = pd.DataFrame()

# 尾盘补充
if (filtered.empty and not in_morning) or in_afternoon:
    print("切换到尾盘防御模式...")
    filtered_eod = df[
        (df["price"] >= MIN_PRICE) & (df["price"] <= MAX_PRICE) &
        (df["pct"] >= EOD_MIN_PCT) & (df["pct"] <= EOD_MAX_PCT) &
        (df["amount"] >= MIN_AMOUNT) &
        (df["lb"] >= EOD_MIN_LB) & (df["lb"] <= EOD_MAX_LB) &
        (df["turnover"] <= EOD_MAX_TURNOVER) &
        (df["amplitude"] <= EOD_MAX_AMPLITUDE)
    ].copy()
    if MIN_CONSECUTIVE_UP > 0:
        filtered_eod = filtered_eod[filtered_eod["code"].apply(has_consecutive_mild_up)]
    filtered = filtered_eod
    print(f"尾盘初步筛选出 {len(filtered)} 只")

if filtered.empty:
    print("今日无标的，空仓")
    sys.exit(0)

# 实时评分
filtered["realtime_score"] = (
    filtered["pct"] * 1.3 +
    filtered["lb"] * 2.0 +
    (filtered["amount"] / 1e8) * 0.7 +
    filtered["turnover"] * 0.7 -
    filtered["amplitude"] * 0.4 -
    filtered["open_pct"].fillna(0) * 0.5
)

candidates = filtered.sort_values("realtime_score", ascending=False).head(TOP_N_CANDIDATES).copy()
if candidates.empty:
    print("无候选")
    sys.exit(0)

# 历史回测 + 基本面排雷
history_rows = []
valid_idx = []
for idx, row in candidates.iterrows():
    code = str(row["code"])
    if FUNDAMENTAL_CHECK and not has_safe_fundamentals(code):
        continue
    hist_res = evaluate_stock_history(code)
    history_rows.append(hist_res)
    valid_idx.append(idx)

if not valid_idx:
    print("基本面或历史样本不足，空仓")
    sys.exit(0)

candidates = candidates.loc[valid_idx].reset_index(drop=True)
candidates = pd.concat([candidates, pd.DataFrame(history_rows)], axis=1)
candidates = candidates[candidates["signals"] >= BACKTEST_MIN_SIGNALS].copy()
if candidates.empty:
    print("历史样本不足，空仓")
    sys.exit(0)

candidates["final_score"] = candidates["realtime_score"] * 0.28 + candidates["history_score"] * 0.72
candidates = candidates[candidates["final_score"] >= MIN_SCORE_THRESHOLD]
candidates = candidates.sort_values("final_score", ascending=False).reset_index(drop=True)

if candidates.empty:
    print("评分不足，空仓")
    sys.exit(0)

stock = candidates.iloc[0]
p = safe_float(stock["price"])
buy_ref = round(p * LOW_BUY_RATIO, 2)
stop = round(buy_ref * (1 + HARD_STOP_RATIO), 2)
next_sell_day = get_next_trade_day_text(now)

target_sell_min = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
target_sell_max = calc_target_sell_price(buy_ref, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)
net_profit_min = round(calc_net_profit(target_sell_min, buy_ref, FIX_AMOUNT), 2)
net_profit_max = round(calc_net_profit(target_sell_max, buy_ref, FIX_AMOUNT), 2)
net_stop_loss = round(calc_net_profit(stop, buy_ref, FIX_AMOUNT), 2)

best = candidates.head(3)[["code", "name", "pct", "signals", "win_rate",
                           "target_250_hit_rate", "target_350_hit_rate", "final_score"]].copy()
lines = []
for _, row in best.iterrows():
    lines.append(
        f"- {row['name']}({row['code']})｜现涨 {safe_float(row['pct']):.2f}%｜"
        f"胜率 {safe_float(row['win_rate']):.1f}%｜"
        f"250命中 {safe_float(row['target_250_hit_rate']):.1f}%｜"
        f"350命中 {safe_float(row['target_350_hit_rate']):.1f}%"
    )

content = f"""
## {today} 低吸稳赢候选
- 股票：{stock['name']}({stock['code']})
- 现价：{p:.2f}
- 计划低吸买入参考：{buy_ref} （折扣{LOW_BUY_RATIO*100:.1f}%）
- 止盈区间：{target_sell_min} ~ {target_sell_max}
- 硬止损价格：{stop} （成本-2%）
- 卖出窗口：{next_sell_day} 起
- 预估净利：{net_profit_min} ~ {net_profit_max}
- 止损预估亏损：{net_stop_loss}
- 历史样本：{int(stock['signals'])}
- 次日收盘胜率：{safe_float(stock['win_rate']):.1f}%
- 目标250命中率：{safe_float(stock['target_250_hit_rate']):.1f}%
- 目标350命中率：{safe_float(stock['target_350_hit_rate']):.1f}%

### 前3候选
{os.linesep.join(lines)}
""".strip()

push("低吸稳赢候选", content)
print(f"今日推荐：{stock['name']}({stock['code']})")

# ========== 记录日志 ==========
log_file = "trade_log.csv"
log_row = {
    "date": today,
    "time_window": "morning" if in_morning else "afternoon",
    "code": str(stock["code"]),
    "name": str(stock["name"]),
    "price_at_signal": p,
    "buy_ref": buy_ref,
    "stop": stop,
    "target_min": target_sell_min,
    "target_max": target_sell_max,
    "net_profit_min": net_profit_min,
    "net_profit_max": net_profit_max,
    "signals": int(stock["signals"]),
    "win_rate": round(safe_float(stock["win_rate"]), 2),
    "hit_250": round(safe_float(stock["target_250_hit_rate"]), 2),
    "hit_350": round(safe_float(stock["target_350_hit_rate"]), 2),
    "final_score": round(safe_float(stock["final_score"]), 2),
    "market_pct": market_pct,
    "weekday": week_num
}
file_exists = os.path.isfile(log_file)
with open(log_file, "a", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=list(log_row.keys()))
    if not file_exists:
        writer.writeheader()
    writer.writerow(log_row)
print(f"交易日志已写入：{log_file}")
