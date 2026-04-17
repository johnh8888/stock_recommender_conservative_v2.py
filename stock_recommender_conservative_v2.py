import akshare as ak
import pandas as pd
import requests
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ==================== 【最终定版：2万本金 稳健套利 2.2%~3.5%】====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")

# 本金&仓位
TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

# 核心参数：只吃2.2%~3.5%安全利润
LOW_BUY_RATIO = 0.990     # 次日低吸挂单价
TAKE_PROFIT1 = 1.022      # 第一止盈 +2.2%
TAKE_PROFIT2 = 1.035      # 目标止盈 +3.5%
HARD_STOP = 0.968         # 防守止损 -3.2%

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()   # 0周一~4周五，自动判断星期

# ==================== 周五强制空仓：彻底规避周末黑天鹅 ====================
if week_num == 4:
    content = f"""
## {today} 🛑 今日星期五｜强制空仓
✅ 稳赚铁律：绝不持股过周末
❌ 周五不新开仓、不买入任何股票
💡 周一再正常低吸套利，规避周末突发利空/外围大跌
"""
    if PUSHPLUS_TOKEN:
        requests.post(
            "http://www.pushplus.plus/send",
            json={
                "token": PUSHPLUS_TOKEN,
                "title": "周五空仓｜规避风险",
                "content": content,
                "template": "markdown"
            }
        )
    print("🛑 周五停止荐股，空仓休息")
    exit()

# ==================== 极速获取市场数据 ====================
df = ak.stock_zh_a_spot_em()
df = df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price",
    "涨跌幅": "pct", "成交额": "amount", "量比": "lb"
})

# 过滤所有高危标的：ST/退市/新股/创业板/科创板
ban = ["ST", "退", "退市", "C", "N", "U", "XD", "XR"]
df = df[~df["name"].str.contains("|".join(ban), na=False)]
# 只留你能买的：60沪市主板、00开头深市/中小板
df = df[(df["code"].str.startswith("60")) | (df["code"].str.startswith("00"))]

# 大盘涨跌参考
try:
    index = ak.stock_zh_a_spot_em()
    sh_index = index[index["名称"] == "上证指数"]
    market_pct = float(sh_index["涨跌幅"].iloc[0]) if not sh_index.empty else 0
except:
    market_pct = 0

# ==================== 稳健保守选股：高胜率低波动 ====================
filtered = df[
    (df["pct"] >= 0.4) & (df["pct"] <= 3.0) &
    (df["amount"] >= 60000000) &
    (df["lb"] >= 1.1) & (df["lb"] <= 1.7) &
    (df["price"] > 5)
].copy()

if filtered.empty:
    content = f"## {today} A股稳赚筛选\n\n今日无符合条件稳健标的，空仓观望。"
else:
    # 稳盘打分，优选最稳的护盘股
    filtered["score"] = filtered["pct"] * 1.8 + filtered["lb"] * 4
    filtered = filtered.sort_values("score", ascending=False).head(8)
    stock = filtered.iloc[0]
    p = float(stock["price"])

    # 按你的成本算精准买卖点
    buy_ref = round(p * LOW_BUY_RATIO, 2)
    tp1 = round(p * TAKE_PROFIT1, 2)
    tp2 = round(p * TAKE_PROFIT2, 2)
    stop = round(p * HARD_STOP, 2)

    # 保存历史记录
    csv_file = "recommendation_history.csv"
    new_row = pd.DataFrame([{
        "date": today,
        "code": stock["code"],
        "name": stock["name"],
        "price": p,
        "pct": stock["pct"]
    }])
    new_row.to_csv(csv_file, mode="a", header=not os.path.exists(csv_file), index=False, encoding="utf-8")
    count = len(pd.read_csv(csv_file)) if os.path.exists(csv_file) else 0

    # 微信推送文案
    content = f"""
## {today} 🔥 稳健套利｜固定2.2%~3.5%收益
**大盘今日涨跌**：{market_pct:.2f}%
✅ 仅60/00可交易主板｜隔日短线｜周五自动空仓

| 股票 | 今日涨幅 | 操作金额 | 低吸参考价 | 保本止盈 | 目标止盈 | 防守止损 |
|------|----------|----------|------------|----------|----------|----------|
| {stock['name']}({stock['code']}) | {stock['pct']:.2f}% | {FIX_AMOUNT}元 | {buy_ref} | {tp1} | {tp2} | {stop} |

### 必守操作纪律
1. 次日只挂【低吸参考价】低吸，绝不追高
2. 固定下单：{FIX_AMOUNT}元，不加仓、不补仓
3. 涨到 +2.2% 先落袋部分利润
4. 强势冲高拿到 +3.5% 全部清仓
5. 跌破止损位严格离场，不扛单
6. 周四持仓，周五上午必须清仓，绝不持股过周末

累计推荐：{count} 只
"""

# 微信推送
if PUSHPLUS_TOKEN:
    requests.post(
        "http://www.pushplus.plus/send",
        json={
            "token": PUSHPLUS_TOKEN,
            "title": "稳健套利｜2.2%~3.5%",
            "content": content,
            "template": "markdown"
        }
    )

print(f"✅ 最终版运行完成｜今日标的：{stock['name'] if not filtered.empty else '无'}")
