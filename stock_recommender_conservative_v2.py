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

# 代码补0 + 过滤风险股
stock_spot['ts_code'] = stock_spot['ts_code'].astype(str).str.zfill(6)
stock_spot = stock_spot[~stock_spot['name'].str.contains('ST|退|退市|C|N|U', na=False)]
print(f"共获取 {len(stock_spot)} 只股票")

# ==================== 宽松筛选 + 强制出票 ====================
filtered = stock_spot[
    (stock_spot['pct_chg'] > 0) &
    (stock_spot['amount'] > 30000000)
].copy()

# 打分排序
filtered['score'] = filtered['pct_chg'] * 4 + filtered['volume_ratio'] * 8 + filtered['turnover_rate'] * 1
filtered = filtered.sort_values('score', ascending=False).head(10)

# 取前2只（不管历史数据，保证出票）
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

# ==================== 生成消息（一定有内容） ====================
table = "| 股票 | 涨幅 | 得分 | 买入 | 止盈 | 止损 |\n|------|------|------|------|------|------|\n"
for _, row in top_candidates.iterrows():
    c = round(row['close'], 2)
    table += f"|{row['name']}|{round(row['pct_chg'],2)}%|{round(row['score'],1)}|{round(c*0.99,2)}|{round(c*1.06,2)}|{round(c*0.95,2)}|\n"

msg = f"""## {trade_date} A股保守推荐（稳定版）
{table}

⚠️ 风控规则：
- 买入：明日小幅回调再进场
- 止盈：+6% 卖出
- 止损：-5% 严格执行

历史累计推荐：{hist_count} 只
免责声明：仅供学习交流，不构成投资建议
"""

# ==================== 推送 ====================
if PUSHPLUS_TOKEN:
    try:
        requests.post("http://www.pushplus.plus/send", json={
            "token": PUSHPLUS_TOKEN,
            "title": f"A股推荐 {trade_date}",
            "content": msg,
            "template": "markdown"
        }, timeout=20)
        print("✅ 推送完成")
    except Exception as e:
        print("推送失败", e)
else:
    print("\n" + msg)

print("🎉 脚本运行完成 —— 今日已出票")
