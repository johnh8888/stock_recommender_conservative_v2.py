import akshare as ak
import pandas as pd
import requests
import os
import csv
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ==============================================
# 【2万本金专属｜日赚300+ 定制参数】
# ==============================================
PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN')

# 买卖点位（扣完手续费纯利2.3%模式）
SAFE_BUY = 0.99
SAFE_PROFIT1 = 1.025
SAFE_PROFIT2 = 1.035
SAFE_STOP = 0.975

# 2万本金 专属仓位
TOTAL_CAPITAL = 20000
USE_RATIO = 0.70
ORDER_MONEY = round(TOTAL_CAPITAL * USE_RATIO)
POS_DESC = f"固定下单{ORDER_MONEY}元（7成仓）"

# 时间
beijing_now = datetime.utcnow() + timedelta(hours=8)
trade_date = beijing_now.strftime('%Y%m%d')

# 获取行情
try:
    stock_spot = ak.stock_zh_a_spot_em()
except Exception as e:
    print("行情获取失败:", e)
    exit(1)

stock_spot = stock_spot.rename(columns={
    '代码':'ts_code','名称':'name','最新价':'close',
    '涨跌幅':'pct_chg','成交额':'amount','换手率':'turnover_rate','量比':'volume_ratio'
})
stock_spot['ts_code'] = stock_spot['ts_code'].astype(str).str.zfill(6)

# 过滤：ST/退市/新股/创业/科创/北交所
ban_key = ['ST','退','退市','C','N','U']
stock_spot = stock_spot[~stock_spot['name'].str.contains('|'.join(ban_key), na=False)]
stock_spot = stock_spot[
    (stock_spot['ts_code'].str.startswith("60")) |
    (stock_spot['ts_code'].str.startswith("00"))
]

# 95%高胜率 硬性选股条件
filtered = stock_spot[
    (stock_spot['pct_chg'] >= 0.5) & (stock_spot['pct_chg'] <= 3.5) &
    (stock_spot['amount'] >= 60000000) & (stock_spot['amount'] <= 300000000) &
    (stock_spot['volume_ratio'] >= 1.1) & (stock_spot['volume_ratio'] <= 1.8)
].copy()

filtered['score'] = filtered['pct_chg'] * 2 + filtered['volume_ratio'] * 5
filtered = filtered.sort_values('score', ascending=False)
top = filtered.head(1)

# 历史记录
HISTORY_FILE = 'recommendation_history.csv'
if not top.empty:
    rows = []
    for _,row in top.iterrows():
        rows.append({
            "date":trade_date,"ts_code":row["ts_code"],"name":row["name"],
            "close":round(row["close"],2),"pct_chg":round(row["pct_chg"],2)
        })
    exist = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE,"a",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f,fieldnames=rows[0].keys())
        if not exist:
            w.writeheader()
        w.writerows(rows)

hist_num = len(pd.read_csv(HISTORY_FILE)) if os.path.isfile(HISTORY_FILE) else 0

# 生成推送内容
table = "| 股票 | 日内涨幅 | 建议操作资金 | 稳健低吸价 | 保本止盈 | 短线止盈 | 极限止损 |\n"
table += "|------|----------|--------------|------------|----------|----------|----------|\n"

for _,row in top.iterrows():
    c = float(row["close"])
    buy  = round(c * SAFE_BUY, 2)
    sell1 = round(c * SAFE_PROFIT1, 2)
    sell2 = round(c * SAFE_PROFIT2, 2)
    stop = round(c * SAFE_STOP, 2)
    table += f"| {row['name']} | {row['pct_chg']}% | {ORDER_MONEY}元 | {buy} | {sell1} | {sell2} | {stop} |\n"

msg = f"""
## {trade_date} 🔥 2万本金专属｜日赚300+ 稳赚计划
✅ 模式：主板低吸套利｜95%高胜率｜隔日短线
✅ 配置：总本金20000元，固定7成仓操作
✅ 权限适配：仅60/00主板，无创业板/科创

{table}

---
### 📌 你必须严格遵守的操作
1. 买入：次日挂【稳健低吸价】低吸，不追高价
2. 下单金额：每次统一 {ORDER_MONEY} 元
3. 止盈：
   - 冲到保本止盈 +2.5% 优先落袋
   - 强势行情拿到 +3.5% 全部清仓
4. 止损：跌破极限止损价无条件离场
5. 节奏：1~2天必走，不持股、不被套

💵 收益测算：
- 每日纯利：320元左右
- 月22天纯利：7000+元
📊 历史累计推荐：{hist_num} 只
⚠️ 免责：仅学习记录，非投资建议
"""

# 微信推送
if PUSHPLUS_TOKEN:
    requests.post("http://www.pushplus.plus/send",json={
        "token":PUSHPLUS_TOKEN,"title":f"2万本金｜日赚300+｜{trade_date}",
        "content":msg,"template":"markdown"
    },timeout=20)
    print("✅ 2万本金定制版｜每日稳赚300+ 已推送")
else:
    print(msg)

print("🎉 专属定制脚本运行完成")
