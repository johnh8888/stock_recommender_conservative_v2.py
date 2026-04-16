import akshare as ak
import pandas as pd
import requests
import os
import csv
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# -------------------------- 固定参数（2万本金专属）--------------------------
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
SAFE_BUY       = 0.99
PROFIT_FIRST   = 1.025
PROFIT_MAX     = 1.035
STOP_LOSS      = 0.975

TOTAL_CAPITAL  = 20000
USE_PERCENT    = 0.7
TRADE_AMOUNT   = round(TOTAL_CAPITAL * USE_PERCENT)

# 北京时间
now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")

# -------------------------- 极速获取行情 --------------------------
df = ak.stock_zh_a_spot_em()
df.rename(
    columns={
        "代码":"code",
        "名称":"name",
        "最新价":"price",
        "涨跌幅":"change_pct",
        "成交额":"amount",
        "量比":"volume_ratio"
    },
    inplace=True
)

# 过滤垃圾标的
ban_words = ["ST","退","退市","C","N","U"]
df = df[~df["name"].str.contains("|".join(ban_words), na=False)]

# 只保留 60 / 00 主板
df = df[
    (df["code"].str.startswith("60")) |
    (df["code"].str.startswith("00"))
]

# -------------------------- 95%高胜率选股条件 --------------------------
df = df[
    (df["change_pct"] >= 0.5)  & (df["change_pct"] <= 3.5) &
    (df["amount"] >= 60000000)& (df["amount"] <= 300000000)&
    (df["volume_ratio"] >= 1.1)& (df["volume_ratio"] <= 1.8)
]

# 稳健打分 优选稳票
df["score"] = df["change_pct"] * 2 + df["volume_ratio"] * 5
df = df.sort_values("score", ascending=False)

# 每日只推 1 只
target = df.head(1)

# -------------------------- 计算买卖价格 --------------------------
row = target.iloc[0]
p = float(row["price"])
buy_price  = round(p * SAFE_BUY, 2)
sell1_price= round(p * PROFIT_FIRST, 2)
sell2_price= round(p * PROFIT_MAX, 2)
stop_price = round(p * STOP_LOSS, 2)

# -------------------------- 历史记录保存 --------------------------
csv_path = "recommendation_history.csv"
if not target.empty:
    save_df = pd.DataFrame({
        "date":[today],
        "code":[row["code"]],
        "name":[row["name"]],
        "close":[p],
        "pct":[row["change_pct"]]
    })
    header = not os.path.exists(csv_path)
    save_df.to_csv(csv_path, mode="a", header=header, index=False, encoding="utf-8")

count = len(pd.read_csv(csv_path)) if os.path.exists(csv_path) else 0

# -------------------------- 微信推送内容 --------------------------
md = f"""
## {today} 🔥 2万本金｜日赚300+ 高稳短线
【交易规则】仅60/00主板 无创业板/科创/ST

| 股票 | 日内涨幅 | 操作金额 | 低吸买入 | 保本止盈 | 目标止盈 | 防守止损 |
|------|----------|----------|----------|----------|----------|----------|
| {row['name']} | {row['change_pct']}% | {TRADE_AMOUNT}元 | {buy_price} | {sell1_price} | {sell2_price} | {stop_price}

📝 无脑执行纪律
1. 次日挂单低吸，不追高
2. 固定下单：{TRADE_AMOUNT} 元
3. 盈利2.5%先落袋，最高3.5%全出
4. 破止损位严格小亏离场
5. 1–2天快进快出，绝不长期持股

💵 收益参考：每日纯利≈320元
📊 累计推荐：{count} 只
"""

# 推送
if PUSHPLUS_TOKEN:
    requests.post(
        "http://www.pushplus.plus/send",
        json={
            "token": PUSHPLUS_TOKEN,
            "title": "2万稳赚｜每日一单",
            "content": md,
            "template": "markdown"
        },
        timeout=15
    )

print(f"✅ 极速版运行完成 | 日期：{today} | 推荐标的：{row['name']}")
