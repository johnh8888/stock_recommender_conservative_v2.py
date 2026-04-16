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

# ==================== 获取最新交易日 ====================
beijing_now = datetime.utcnow() + timedelta(hours=8)
trade_date = beijing_now.strftime('%Y%m%d')
print(f"✅ 处理日期: {trade_date}")

# ==================== 获取全市场当日行情（AkShare） ====================
print("正在获取全市场行情...")
try:
    stock_spot = ak.stock_zh_a_spot_em()
except Exception as e:
    print("获取行情失败:", e)
    stock_spot = pd.DataFrame()

if stock_spot.empty:
    print("行情数据为空")
    exit(1)

# 重命名列方便使用
stock_spot = stock_spot.rename(columns={
    '代码': 'ts_code',
    '名称': 'name',
    '最新价': 'close',
    '涨跌幅': 'pct_chg',
    '成交量': 'volume',
    '成交额': 'amount',
    '换手率': 'turnover_rate',
    '量比': 'volume_ratio'
})

stock_spot['ts_code'] = stock_spot['ts_code'].astype(str).str.zfill(6)
# 过滤 ST、退市、风险票
stock_spot = stock_spot[~stock_spot['name'].str.contains('ST|退|退市|C|N', na=False)]

print(f"共获取 {len(stock_spot)} 只股票")

# ==================== 初步筛选 + 多因子打分 ====================
filtered = stock_spot[
    (stock_spot['pct_chg'] >= 1.5) & (stock_spot['pct_chg'] <= 6.5) &
    (stock_spot['amount'] > 80000000) &
    (stock_spot['volume_ratio'] > 1.5)
].copy()

# 多因子打分
filtered['score_momentum'] = filtered['pct_chg'] * 5
filtered['score_liquidity'] = (filtered['volume_ratio'] > 1.8).astype(int) * 30
filtered['total_score'] = (
    filtered['score_momentum']
    + filtered['score_liquidity']
    + filtered['turnover_rate'] * 2
)

# 取前30候选
filtered = filtered.sort_values('total_score', ascending=False).head(30)

# ==================== 补充历史数据（MA20/MA60 趋势） ====================
print("正在补充历史趋势数据...")
candidates = []
for _, row in filtered.iterrows():
    try:
        # 获取近120天日线
        hist = ak.stock_zh_a_hist(
            symbol=row['ts_code'],
            period="daily",
            start_date=(beijing_now - timedelta(days=120)).strftime('%Y%m%d')
        )
        if len(hist) < 60:
            continue

        hist = hist.sort_values('日期')
        close = hist['收盘'].iloc[-1]
        ma20 = hist['收盘'].rolling(20).mean().iloc[-1]
        ma60 = hist['收盘'].rolling(60).mean().iloc[-1]

        # 均线多头额外加分
        score_trend = 40 if (close > ma20 > ma60) else 0
        row['total_score'] += score_trend
        candidates.append(row)
    except Exception:
        continue

if not candidates:
    top_candidates = pd.DataFrame()
else:
    df_final = pd.DataFrame(candidates).sort_values('total_score', ascending=False)
    # 优化：避免同类型扎堆，每组只留1只，更均衡
    df_final = df_final.groupby(df_final['name'].str[0]).head(1)
    top_candidates = df_final.head(2)  # 固定推荐2只

# ==================== 保存历史记录 ====================
HISTORY_FILE = 'recommendation_history.csv'
if not top_candidates.empty:
    rows = []
    for _, row in top_candidates.iterrows():
        rows.append({
            'date': trade_date,
            'ts_code': row['ts_code'],
            'name': row['name'],
            'score': round(row['total_score'], 1),
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

# ==================== 生成推送消息 ====================
if top_candidates.empty:
    msg = f"## {trade_date} A股保守推荐 (AkShare版)\n\n**今日无符合条件的股票**，建议空仓观望。"
else:
    table = "| 股票 | 涨幅 | 得分 | 买入参考 | 止盈 | 止损 |\n|------|------|------|----------|------|------|\n"
    for _, row in top_candidates.iterrows():
        close = round(row['close'], 2)
        table += (
            f"| {row['name']} | {round(row['pct_chg'],2)}% | {round(row['total_score'],1)} "
            f"| {round(close*0.99,2)} | {round(close*1.06,2)} | {round(close*0.95,2)} |\n"
        )

    msg = f"""## {trade_date} A股保守推荐 (AkShare v3.0)

**推荐前2只**（保守筛选 + 多因子）

{table}

**风控提醒**：
- 买入：明日回调时分批，单仓 ≤ 5%
- 止盈：+6%
- 止损：-5%（必须执行！）

**历史记录**：已累计推荐 {hist_count} 只
**数据来源**：AkShare（免费）
**免责声明**：仅供学习参考，非投资建议。股市有风险！
"""

# ==================== PushPlus 推送（唯一一段，无重复） ====================
if PUSHPLUS_TOKEN:
    try:
        push_url = "http://www.pushplus.plus/send"
        data = {
            "token": PUSHPLUS_TOKEN,
            "title": f"A股保守推荐 - {trade_date}",
            "content": msg,
            "template": "markdown"
        }
        requests.post(push_url, data=data, timeout=15)
        print("✅ PushPlus 推送完成")
    except Exception as e:
        print("❌ 推送失败:", e)
else:
    print("\n" + msg)

print("\n🎉 脚本运行完成")
