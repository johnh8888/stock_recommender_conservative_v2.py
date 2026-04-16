import akshare as ak
import pandas as pd
import requests
import os
import csv
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ==================== 配置 ====================
PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN')

# 【全局稳健参数 可自行微调】
RATIO_FIRST_BUY = 0.98   # 稳健买点
RATIO_ADD_BUY = 0.95     # 加仓点
RATIO_TAKE1 = 1.04       # 小止盈
RATIO_TAKE2 = 1.06       # 目标止盈
RATIO_STOP = 0.93        # 强制止损
SINGLE_POS_RATIO = 0.20  # 单只建议仓位 2成

# ==================== 获取日期 ====================
beijing_now = datetime.utcnow() + timedelta(hours=8)
trade_date = beijing_now.strftime('%Y%m%d')
print(f"✅ 处理日期: {trade_date}")

# ==================== 获取行情 ====================
print("正在获取全市场行情...")
try:
    stock_spot = ak.stock_zh_a_spot_em()
except Exception as e:
    print("获取行情失败:", e)
    exit(1)

# 重命名列
stock_spot = stock_spot.rename(columns={
    '代码': 'ts_code',
    '名称': 'name',
    '最新价': 'close',
    '涨跌幅': 'pct_chg',
    '成交额': 'amount',
    '换手率': 'turnover_rate',
    '量比': 'volume_ratio'
})

stock_spot['ts_code'] = stock_spot['ts_code'].astype(str).str.zfill(6)

# ==================== 过滤：只保留60/00主板（你能买的） ====================
ban_key = ['ST','退','退市','C','N','U']
stock_spot = stock_spot[~stock_spot['name'].str.contains('|'.join(ban_key), na=False)]
# 剔除 300/688/8/9 开头
stock_spot = stock_spot[
    (stock_spot['ts_code'].str.startswith('60')) |
    (stock_spot['ts_code'].str.startswith('00'))
]

print(f"✅ 可交易主板总数: {len(stock_spot)} 只")

# ==================== 稳健筛选 ====================
filtered = stock_spot[
    (stock_spot['pct_chg'] > 0) &
    (stock_spot['amount'] > 30000000)
].copy()

filtered['score'] = filtered['pct_chg'] * 4 + filtered['volume_ratio'] * 8 + filtered['turnover_rate'] * 1
filtered = filtered.sort_values('score', ascending=False).head(10)
top_candidates = filtered.head(2)

# ==================== 历史记录 ====================
HISTORY_FILE = 'recommendation_history.csv'
if not top_candidates.empty:
    rows = []
    for _, row in top_candidates.iterrows():
        rows.append({
            'date': trade_date,
            'ts_code': row['ts_code'],
            'name': row['name'],
            'score': round(row['score'], 1),
            'close': round(row['close'], 2),
            'pct_chg': round(row['pct_chg'], 2)
        })
    file_exists = os.path.isfile(HISTORY_FILE)
    with open(HISTORY_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

hist_count = len(pd.read_csv(HISTORY_FILE)) if os.path.isfile(HISTORY_FILE) else 0

# ==================== 生成详细买卖+仓位表格 ====================
table = "| 股票 | 日内涨幅 | 综合得分 | 建议仓位 | 稳健买入 | 加仓位置 | 止盈1 | 目标止盈 | 强制止损 |\n"
table += "|------|----------|----------|----------|----------|----------|-------|----------|----------|\n"

for _, row in top_candidates.iterrows():
    c = float(row['close'])
    buy1  = round(c * RATIO_FIRST_BUY, 2)
    buy2  = round(c * RATIO_ADD_BUY, 2)
    sell1 = round(c * RATIO_TAKE1, 2)
    sell2 = round(c * RATIO_TAKE2, 2)
    stop  = round(c * RATIO_STOP, 2)
    pos_text = f"{int(SINGLE_POS_RATIO*100)}%"

    table += (
        f"| {row['name']} | {row['pct_chg']}% | {round(row['score'],1)} | {pos_text} | "
        f"{buy1} | {buy2} | {sell1} | {sell2} | {stop} |\n"
    )

msg = f"""## {trade_date} 【主板专属 · 稳健买卖方案】
💡 已屏蔽：创业板/科创板/北交所/ST/新股

{table}

---
### 📌 操作策略（最稳当）
1. 建仓：次日回调到【稳健买入】再分批低吸，不追高
2. 加仓：若回落至【加仓位置】可小幅补仓摊低成本
3. 止盈：
   - 短线小利：涨到止盈1 可减仓一半
   - 中线目标：拿到目标止盈全部离场
4. 风控：跌破【强制止损】无条件止损，不扛单

### 资金仓位参考
- 总资金严格控制：总仓位 ≤4成
- 单只个股固定：{int(SINGLE_POS_RATIO*100)}% 仓位
- 杜绝满仓、杜绝重仓一只

历史累计推荐：{hist_count} 只
免责声明：仅供学习参考，不构成投资建议
"""

# ==================== 推送 ====================
if PUSHPLUS_TOKEN:
    try:
        res = requests.post("http://www.pushplus.plus/send", json={
            "token": PUSHPLUS_TOKEN,
            "title": f"主板稳健买卖｜{trade_date}",
            "content": msg,
            "template": "markdown"
        }, timeout=20)
        print("✅ 推送完成：含精准买卖价+仓位建议")
    except Exception as e:
        print("❌ 推送失败", e)
else:
    print("\n" + msg)

print("🎉 脚本运行完成 —— 纯主板+精准买卖策略")
