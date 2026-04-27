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

# 交易时间规则
MORNING_START, MORNING_END = 10, 10.67
AFTERNOON_START, AFTERNOON_END = 14.67, 14.92
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

# 尾盘条件
EOD_MAX_PCT = 1.8
EOD_MIN_PCT = -0.3
EOD_MAX_TURNOVER = 4.5
EOD_MIN_LB, EOD_MAX_LB = 0.9, 1.5
EOD_MAX_AMPLITUDE = 3.5

MIN_SCORE_THRESHOLD = 7.5
TOP_N_CANDIDATES = 5
BACKTEST_LOOKBACK_DAYS = 180
BACKTEST_MIN_SIGNALS = 4
MIN_CONSECUTIVE_UP = 3

MA20_FILTER = True
FUNDAMENTAL_CHECK = True

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()
current_hour = now.hour + now.minute / 60.0


# ---------- 工具函数 ----------
def push(title, content):
    if PUSHPLUS_TOKEN:
        try:
            requests.post("http://www.pushplus.plus/send",
                          json={"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "markdown"},
                          timeout=10)
        except:
            pass


def safe_float(value, default=0.0):
    try:
        if pd.isna(value): return default
        return float(value)
    except:
        return default


def get_col(df, col, default=np.nan):
    return df[col] if col in df.columns else pd.Series([default] * len(df), index=df.index)


def calc_open_pct(row):
    prev = safe_float(row.get("prev_close", row.get("昨收")), 0.0)
    opn = safe_float(row.get("open", row.get("今开")), 0.0)
    if prev <= 0 or opn <= 0: return np.nan
    return (opn / prev - 1) * 100


def get_next_trade_day_text(base_dt):
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        if trade_cal is not None and not trade_cal.empty:
            dates = sorted(trade_cal["trade_date"].astype(str).tolist())
            base_str = base_dt.strftime("%Y-%m-%d")
            for d in dates:
                if d > base_str: return d.replace("-", "")
    except:
        pass
    candidate = base_dt + timedelta(days=1)
    while candidate.weekday() >= 5: candidate += timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def market_is_weak(market_pct):
    return market_pct <= MAX_ACCEPTABLE_MARKET_DROP


def calc_net_profit(sell_price, buy_price, capital):
    if buy_price <= 0 or sell_price <= 0 or capital <= 0: return 0.0
    shares = capital / buy_price
    gross = (sell_price - buy_price) * shares
    fees = capital * BUY_FEE_RATE + (shares * sell_price) * (SELL_FEE_RATE + SELL_TAX_RATE)
    return gross - fees


def calc_target_sell_price(buy_price, capital, net_profit_target):
    if buy_price <= 0 or capital <= 0: return 0.0
    return round(buy_price * (1 + ROUND_TRIP_FEE_RATE + net_profit_target / capital), 2)


def get_market_ma20_safe():
    try:
        index_df = ak.stock_zh_index_daily(symbol="sh000001")
        index_df = index_df.sort_values("date").tail(30)
        close = float(index_df["close"].iloc[-1])
        ma20 = float(index_df["close"].rolling(20).mean().iloc[-1])
        ma20_prev = float(index_df["close"].rolling(20).mean().iloc[-2])
        return close, ma20, (close > ma20 and ma20 > ma20_prev)
    except:
        return 0, 0, True


def get_sector_rank_map():
    try:
        sector_df = ak.stock_board_industry_name_em()
        return dict(zip(sector_df["板块名称"], sector_df["涨跌幅"]))
    except:
        return {}


def has_consecutive_mild_up(code, days=MIN_CONSECUTIVE_UP):
    try:
        end = (now - timedelta(days=1)).strftime("%Y%m%d")
        start = (now - timedelta(days=30)).strftime("%Y%m%d")
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
        if hist is None or hist.empty or len(hist) < days: return False
        recent = hist.tail(days + 5)
        pct_col = get_col(recent, "涨跌幅")
        tail = pct_col.tail(days)
        if tail.isna().any(): return False
        if not tail.between(0.5, 4.5).all(): return False
        if (pct_col.tail(20) < -5).any(): return False
        return True
    except:
        return False


def has_safe_fundamentals(code):
    try:
        info = ak.stock_individual_info_em(symbol=code)
        if info is None or info.empty: return True
        info_dict = dict(zip(info["item"], info["value"]))
        return safe_float(info_dict.get("归属母公司股东的净利润", 0)) > 0
    except:
        return True


def evaluate_stock_history(symbol):
    start_date = (now - timedelta(days=BACKTEST_LOOKBACK_DAYS + 40)).strftime("%Y%m%d")
    end_date = today
    try:
        hist = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    except:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}
    if hist is None or hist.empty:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}

    hist = hist.rename(columns={"日期": "date", "开盘": "open", "收盘": "close", "最高": "high",
                                "最低": "low", "涨跌幅": "pct", "成交额": "amount",
                                "换手率": "turnover", "振幅": "amplitude"}).copy()
    hist = hist.sort_values("date").tail(BACKTEST_LOOKBACK_DAYS).reset_index(drop=True)
    hist["open_pct"] = (hist["open"] / hist["close"].shift(1) - 1) * 100

    signals = []
    for i in range(1, len(hist) - 1):
        row = hist.iloc[i]
        nxt = hist.iloc[i + 1]
        if not (MIN_PCT <= safe_float(row["pct"]) <= MAX_PCT and
                safe_float(row["amount"]) >= MIN_AMOUNT and
                MIN_TURNOVER <= safe_float(row.get("turnover"), 0) <= MAX_TURNOVER and
                MIN_AMPLITUDE <= safe_float(row.get("amplitude"), 0) <= MAX_AMPLITUDE and
                MIN_PRICE <= safe_float(row["close"]) <= MAX_PRICE and
                safe_float(row.get("open_pct"), 999) <= MAX_OPEN_PCT):
            continue

        buy_sim = safe_float(row["close"]) * LOW_BUY_RATIO
        next_high, next_close, next_low = safe_float(nxt["high"]), safe_float(nxt["close"]), safe_float(nxt["low"])
        if min(buy_sim, next_high, next_close, next_low) <= 0:
            continue

        t250 = calc_target_sell_price(buy_sim, FIX_AMOUNT, NET_PROFIT_TARGET_MIN)
        t350 = calc_target_sell_price(buy_sim, FIX_AMOUNT, NET_PROFIT_TARGET_MAX)
        signals.append({
            "win": 1 if next_close > buy_sim else 0,
            "target_250_hit": 1 if next_high >= t250 else 0,
            "target_350_hit": 1 if next_high >= t350 else 0,
            "next_close_ret": (next_close / buy_sim - 1) * 100,
            "next_high_ret": (next_high / buy_sim - 1) * 100,
            "next_low_ret": (next_low / buy_sim - 1) * 100,
        })

    if not signals:
        return {"signals": 0, "win_rate": 0.0, "target_250_hit_rate": 0.0,
                "target_350_hit_rate": 0.0, "avg_next_close": 0.0,
                "avg_next_high": 0.0, "avg_worst_drawdown": 0.0, "history_score": -999}

    s = pd.DataFrame(signals)
    n_sig = len(s)
    win_r = float(s["win"].mean() * 100)
    hit250 = float(s["target_250_hit"].mean() * 100)
    hit350 = float(s["target_350_hit"].mean() * 100)
    avg_c = float(s["next_close_ret"].mean())
    avg_h = float(s["next_high_ret"].mean())
    avg_l = float(s["next_low_ret"].mean())

    penalty = min(n_sig, 15) / 15
    score = (win_r * 0.22 + hit250 * 0.38 + hit350 * 0.22 + avg_c * 9.0 + avg_h * 4.5 + avg_l * 2.0) * penalty

    return {"signals": n_sig, "win_rate": win_r, "target_250_hit_rate": hit250,
            "target_350_hit_rate": hit350, "avg_next_close": avg_c,
            "avg_next_high": avg_h, "avg_worst_drawdown": avg_l, "history_score": score}


# ==================== 行情获取（动态列映射，双源容错） ====================
def fetch_spot_data():
    """
    优先东方财富，失败后使用新浪（动态识别列名）
    返回标准化DataFrame，包含：code, name, price, pct, amount, lb, turnover, amplitude, open, prev_close
    """
    # 尝试东方财富
    for attempt in range(1, 3):
        try:
            print(f"东方财富行情，第{attempt}次...")
            raw = ak.stock_zh_a_spot_em()
            if raw is not None and not raw.empty:
                # 东方财富标准列名：代码、名称、最新价、涨跌幅、成交额、量比、换手率、振幅、今开、昨收
                standard = pd.DataFrame()
                standard["code"] = raw["代码"]
                standard["name"] = raw["名称"]
                standard["price"] = raw["最新价"].astype(float)
                standard["pct"] = raw["涨跌幅"].astype(float)
                standard["amount"] = raw["成交额"].astype(float)
                standard["lb"] = raw["量比"].astype(float)
                standard["turnover"] = raw["换手率"].astype(float)
                standard["amplitude"] = raw["振幅"].astype(float)
                standard["open"] = raw["今开"].astype(float)
                standard["prev_close"] = raw["昨收"].astype(float)
                # 保留行业列（若有）
                for col in ["行业", "所属行业"]:
                    if col in raw.columns:
                        standard[col] = raw[col]
                print("东方财富行情成功")
                return standard
        except Exception as e:
            print(f"失败: {e}")
            time.sleep(3)

    # 切换新浪
    try:
        print("尝试新浪行情...")
        raw = ak.stock_zh_a_spot()
        if raw is None or raw.empty:
            print("新浪返回空")
            return pd.DataFrame()

        # 动态列名映射表（中英文对照）
        name_map = {
            "代码": "code", "名称": "name",
            "最新价": "price", "涨跌幅": "pct", "成交额": "amount",
            "换手率": "turnover", "振幅": "amplitude", "开盘": "open",
            "昨收": "prev_close", "成交量": "volume", "量比": "lb"
        }
        # 先统一列名（只映射存在的列）
        standard = pd.DataFrame()
        for raw_col, target_col in name_map.items():
            if raw_col in raw.columns:
                standard[target_col] = raw[raw_col]
        # 如果缺少关键列，尝试其他名称
        if "pct" not in standard.columns:
            if "涨跌幅" in raw.columns:
                standard["pct"] = raw["涨跌幅"]
        if "lb" not in standard.columns:
            if "量比" in standard.columns:
                pass
            else:
                standard["lb"] = 1.0  # 默认中性值
        if "open" not in standard.columns and "今开" in raw.columns:
            standard["open"] = raw["今开"]
        if "prev_close" not in standard.columns and "昨收" not in standard.columns:
            if "昨收" in raw.columns:
                standard["prev_close"] = raw["昨收"]

        # 确保数据类型
        for col in ["price", "pct", "amount", "lb", "turnover", "amplitude", "open", "prev_close"]:
            if col in standard.columns:
                standard[col] = pd.to_numeric(standard[col], errors="coerce")

        # 补充缺失列
        if "turnover" not in standard.columns:
            standard["turnover"] = 0.0
        if "amplitude" not in standard.columns:
            standard["amplitude"] = 0.0
        if "open" not in standard.columns:
            standard["open"] = standard["price"]
        if "prev_close" not in standard.columns:
            standard["prev_close"] = standard["open"]

        # 保留行业信息
        for col in ["行业", "所属行业"]:
            if col in raw.columns:
                standard[col] = raw[col]

        print("新浪行情成功")
        return standard
    except Exception as e:
        print(f"新浪行情获取失败: {e}")
        return pd.DataFrame()


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

if MA20_FILTER:
    _, _, ma_safe = get_market_ma20_safe()
    if not ma_safe and not TEST_MODE:
        print("大盘不在20日线上方或均线未向上，暂停开仓")
        sys.exit(0)
    if not ma_safe and TEST_MODE:
        print("⚠️ 测试模式：大盘未满足安全条件，继续运行")

# 获取行情
raw_df = fetch_spot_data()
if raw_df.empty:
    print("所有行情接口均不可用，退出")
    sys.exit(0)

# 寻找上证指数行
market_pct = 0.0
name_col = "name"
if name_col in raw_df.columns:
    sh_mask = raw_df[name_col].str.contains("上证指数|上证综合指数", na=False)
    if sh_mask.any():
        market_pct = safe_float(raw_df.loc[sh_mask, "pct"].iloc[0], 0.0)

if market_is_weak(market_pct):
    print(f"市场跌幅{market_pct:.2f}%过深，空仓")
    sys.exit(0)

# 数据清洗
df = raw_df.copy()
df["open_pct"] = df.apply(calc_open_pct, axis=1)
for col_name in ["turnover", "amplitude", "open_pct"]:
    df[col_name] = get_col(df, col_name, np.nan)

# 排除ST/新股
ban_pattern = r"(^ST|^\*ST|退市|^N|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].astype(str).str.startswith(("60", "00")))]

# 板块效应
sector_map = get_sector_rank_map()
if sector_map:
    sector_pcts = sorted(sector_map.values(), reverse=True)
    cutoff_idx = int(len(sector_pcts) * 0.4)
    cutoff_pct = sector_pcts[cutoff_idx] if sector_pcts else -100
    sector_col = None
    for col_name in ["行业", "所属行业"]:
        if col_name in df.columns:
            sector_col = col_name
            break
    if sector_col:
        df["sector_pct"] = df[sector_col].map(sector_map)
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

# 历史回测 + 基本面
history_rows, valid_idx = [], []
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

# 日志记录
log_file = "trade_log.csv"
log_row = {
    "date": today, "time_window": "morning" if in_morning else "afternoon",
    "code": str(stock["code"]), "name": str(stock["name"]),
    "price_at_signal": p, "buy_ref": buy_ref, "stop": stop,
    "target_min": target_sell_min, "target_max": target_sell_max,
    "net_profit_min": net_profit_min, "net_profit_max": net_profit_max,
    "signals": int(stock["signals"]), "win_rate": round(safe_float(stock["win_rate"]), 2),
    "hit_250": round(safe_float(stock["target_250_hit_rate"]), 2),
    "hit_350": round(safe_float(stock["target_350_hit_rate"]), 2),
    "final_score": round(safe_float(stock["final_score"]), 2),
    "market_pct": market_pct, "weekday": week_num
}
file_exists = os.path.isfile(log_file)
with open(log_file, "a", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=list(log_row.keys()))
    if not file_exists: writer.writeheader()
    writer.writerow(log_row)
print(f"交易日志已写入：{log_file}")
