import akshare as ak
import pandas as pd
import requests
import os
import sys
import re
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ==================== 配置参数 ====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

LOW_BUY_RATIO  = 0.990
TAKE_PROFIT1   = 1.022
TAKE_PROFIT2   = 1.035
HARD_STOP      = 0.968

now      = datetime.utcnow() + timedelta(hours=8)
today    = now.strftime("%Y%m%d")
week_num = now.weekday()

# ==================== 推送函数封装 ====================
def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        requests.post(
            "http://www.pushplus.plus/send",
            json={"token": PUSHPLUS_TOKEN, "title": title,
                  "content": content, "template": "markdown"},
            timeout=10
        )

# ==================== 周五强制空仓 ====================
if week_num == 4:
    msg = f"## {today} 今日星期五 强制空仓\n\n稳赚铁律：绝不持股过周末\n\n周一再正常低吸套利。"
    push("周五空仓｜规避风险", msg)
    print("周五停止荐股，空仓休息")
    sys.exit(0)

# ==================== 获取市场数据（只请求一次） ====================
raw_df = ak.stock_zh_a_spot_em()

# 提取大盘涨跌（复用已取数据）
try:
    sh_row    = raw_df[raw_df["名称"] == "上证指数"]
    market_pct = float(sh_row["涨跌幅"].iloc[0]) if not sh_row.empty else 0.0
except Exception:
    market_pct = 0.0

df = raw_df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price",
    "涨跌幅": "pct", "成交额": "amount", "量比": "lb"
})

# ==================== 过滤高危标的 ====================
ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].str.startswith("60")) | (df["code"].str.startswith("00"))]

# ==================== 稳健选股 ====================
filtered = df[
    (df["pct"]    >= 0.4) & (df["pct"]    <= 3.0) &
    (df["amount"] >= 6e7)                          &
    (df["lb"]     >= 1.1) & (df["lb"]     <= 1.7) &
    (df["price"]  >  5)
].copy()

stock = None  # 预定义，防止 NameError

if filtered.empty:
    content = f"## {today} A股稳赚筛选\n\n今日无符合条件稳健标的，空仓观望。"
    push("稳健套利｜今日无标的", content)
else:
    # 优化打分：兼顾涨幅、量比、流动性
    filtered["score"] = (
        filtered["pct"] * 1.5 +
        filtered["lb"]  * 3.0 +
        (filtered["amount"] / 1e8) * 0.5
    )
    filtered = filtered.sort_values("score", ascending=False).head(8)
    stock = filtered.iloc[0]
    p     = float(stock["price"])

    buy_ref = round(p * LOW_BUY_RATIO, 2)
    tp1     = round(p * TAKE_PROFIT1,  2)
    tp2     = round(p * TAKE_PROFIT2,  2)
    stop    = round(p * HARD_STOP,     2)

    # ==================== 历史记录（防重复） ====================
    csv_file = "recommendation_history.csv"
    new_row  = pd.DataFrame([{
        "date": today, "code": stock["code"],
        "name": stock["name"], "price": p, "pct": stock["pct"]
    }])

    if os.path.exists(csv_file):
        history_df    = pd.read_csv(csv_file)
        already_logged = ((history_df["date"] == today) &
                          (history_df["code"] == stock["code"])).any()
        if not already_logged:
            new_row.to_csv(csv_file, mode="a", header=False,
                           index=False, encoding="utf-8")
        count = len(pd.read_csv(csv_file))
    else:
        new_row.to_csv(csv_file, mode="w", header=True,
                       index=False, encoding="utf-8")
        count = 1

    content = f"""
## {today} 稳健套利｜固定2.2%~3.5%收益
**大盘今日涨跌**：{market_pct:.2f}%
仅60/00可交易主板｜隔日短线｜周五自动空仓

| 股票 | 今日涨幅 | 操作金额 | 低吸参考价 | 保本止盈 | 目标止盈 | 防守止损 |
|------|----------|----------|------------|----------|----------|----------|
| {stock['name']}({stock['code']}) | {stock['pct']:.2f}% | {FIX_AMOUNT}元 | {buy_ref} | {tp1} | {tp2} | {stop} |

### 必守操作纪律
1. 次日只挂低吸参考价，绝不追高
2. 固定下单 {FIX_AMOUNT} 元，不加仓、不补仓
3. 涨到 +2.2% 先落袋部分利润
4. 强势冲高拿到 +3.5% 全部清仓
5. 跌破止损位严格离场，不扛单
6. 周四持仓，周五上午必须清仓

累计推荐：{count} 只
"""
    push("稳健套利｜2.2%~3.5%", content)

print(f"运行完成｜今日标的：{stock['name'] if stock is not None else '无'}")
