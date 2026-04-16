import tushare as ts
import pandas as pd
import requests
import os
import csv
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ==================== 配置 ====================
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
PUSHPLUS_TOKEN = os.getenv('PUSHPLUS_TOKEN')

if not TUSHARE_TOKEN:
    raise ValueError("❌ 请在 GitHub Secrets 设置 TUSHARE_TOKEN")

pro = ts.pro_api(TUSHARE_TOKEN)

# ==================== 最新交易日 ====================
beijing_now = datetime.utcnow() + timedelta(hours=8)
today_str = beijing_now.strftime('%Y%m%d')

cal = pro.trade_cal(exchange='SSE', start_date=(beijing_now - timedelta(days=90)).strftime('%Y%m%d'), 
                    end_date=today_str)
trading_dates = cal[cal['is_open'] == 1]['cal_date'].sort_values(ascending=False).tolist()
trade_date = trading_dates[0]

print(f"✅ 处理交易日: {trade_date}")

# ==================== 数据获取 ====================
daily = pro.daily(trade_date=trade_date)
basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
daily_basic = pro.daily_basic(trade_date=trade_date, fields='ts_code,turnover_rate,volume_ratio,pe,pb,circ_mv')

# 历史数据（MA、RSI、5日涨幅）
hist = pro.daily(ts_code='', start_date=(beijing_now - timedelta(days=180)).strftime('%Y%m%d'), 
                 end_date=trade_date, fields='ts_code,trade_date,close')

# 大盘（上证）
index_daily = pro.index_daily(ts_code='000001.SH', start_date=(beijing_now - timedelta(days=5)).strftime('%Y%m%d'), 
                              end_date=trade_date)
market_chg = round(index_daily['pct_chg'].iloc[0], 2) if not index_daily.empty else 0.0

# ROE（最新季度，简化处理）
try:
    latest_period = trade_date[:6]  # 如 202504
    roe_data = pro.fina_indicator(ts_code='', period=latest_period, fields='ts_code,roe')
except:
    roe_data = pd.DataFrame(columns=['ts_code', 'roe'])

# ==================== 计算技术指标 ====================
hist = hist.sort_values(['ts_code', 'trade_date'])

# MA20 / MA60
ma20 = hist.groupby('ts_code')['close'].rolling(20).mean().reset_index(name='ma20')
ma20 = ma20.groupby('ts_code').tail(1)[['ts_code', 'ma20']]
ma60 = hist.groupby('ts_code')['close'].rolling(60).mean().reset_index(name='ma60')
ma60 = ma60.groupby('ts_code').tail(1)[['ts_code', 'ma60']]

# RSI
def calc_rsi(group, period=14):
    delta = group['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

rsi_series = hist.groupby('ts_code').apply(calc_rsi).reset_index(name='rsi')
rsi_series = rsi_series.groupby('ts_code').tail(1)[['ts_code', 'rsi']]

# 5日涨幅
last5 = hist[hist['trade_date'] >= (pd.to_datetime(trade_date) - timedelta(days=7)).strftime('%Y%m%d')]
pct5 = last5.groupby('ts_code').apply(
    lambda x: (x['close'].iloc[-1] / x['close'].iloc[0] - 1) * 100 if len(x) > 1 else 0
).reset_index(name='pct_5d')

# 合并所有数据
df = daily.merge(basic, on='ts_code') \
          .merge(daily_basic, on='ts_code') \
          .merge(ma20, on='ts_code', how='left') \
          .merge(ma60, on='ts_code', how='left') \
          .merge(rsi_series, on='ts_code', how='left') \
          .merge(pct5, on='ts_code', how='left')

if not roe_data.empty:
    df = df.merge(roe_data, on='ts_code', how='left')

df = df[~df['name'].str.contains('ST|退|退市', na=False)]

# ==================== 多因子打分 ====================
df['score_value'] = ((40 / df['pe'].clip(upper=40)) * 25).clip(0, 25)
df['score_momentum'] = ((df['pct_chg'] + df['pct_5d']) / 2).clip(0, 10) * 4
df['score_trend'] = ((df['close'] > df['ma20']) & (df['ma20'] > df['ma60'])).astype(int) * 30
df['score_liquidity'] = (df['volume_ratio'] > 1.8).astype(int) * 15
df['score_risk'] = (df['rsi'] < 70).astype(int) * 10
df['total_score'] = df[['score_value', 'score_momentum', 'score_trend', 'score_liquidity', 'score_risk']].sum(axis=1)

# ==================== 保守筛选 + 行业均衡 ====================
filtered = df[
    (df['total_score'] > 65) &
    (df['pct_chg'] >= 1.5) & (df['pct_chg'] <= 6.5) &
    (df['amount'] > 80000000) &
    (df['pe'] > 0) & (df['pe'] < 40) &
    (df['pb'] < 3.5) &
    (df.get('roe', 0) > 5) &          # ROE > 5%
    (df['circ_mv'] > 500000)
].copy()

if not filtered.empty:
    filtered = filtered.sort_values('total_score', ascending=False)
    filtered = filtered.groupby('industry').head(1)  # 每个行业最多1只

top_candidates = filtered.head(4)

# ==================== 保存历史推荐到 CSV ====================
HISTORY_FILE = 'recommendation_history.csv'
def save_history():
    if top_candidates.empty:
        return
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

save_history()
hist_count = len(pd.read_csv(HISTORY_FILE)) if os.path.isfile(HISTORY_FILE) else 0

# ==================== 生成推送消息 ====================
if top_candidates.empty:
    msg = f"## {trade_date} A股保守推荐\n\n**今日无符合严格条件的股票**。\n大盘涨幅：{market_chg}%\n建议**空仓观望**。"
else:
    table = "| 股票 | 涨幅 | 总分 | 买入参考 | 止盈 | 止损 |\n|------|------|------|----------|------|------|\n"
    for _, row in top_candidates.iterrows():
        close = round(row['close'], 2)
        table += f"| {row['name']} | {round(row['pct_chg'],2)}% | {row['total_score']:.1f} | {round(close*0.99,2)} | {round(close*1.06,2)} | {round(close*0.95,2)} |\n"

    msg = f"""## {trade_date} A股保守多因子推荐 v2.0

**大盘**：上证 {market_chg}%
**历史记录**：已累计推荐 {hist_count} 只股票（详见仓库 CSV 文件）

**推荐前2只**（行业均衡）：

{table}

**买入参考**：明日回调时分批，单仓 ≤ **5%**  
**止盈**：+6% 逐步卖出  
**止损**：-5% **必须执行**

**筛选逻辑**：多因子打分 + 低估值 + ROE>5% + 趋势向上 + RSI<70  
**免责声明**：仅供学习参考，非投资建议。股市有风险，入市需谨慎。
"""

# ==================== PushPlus 推送 ====================
if PUSHPLUS_TOKEN:
    push_url = "http://www.pushplus.plus/send"
    data = {
        "token": PUSHPLUS_TOKEN,
        "title": f"A股保守推荐 v2 - {trade_date}",
        "content": msg,
        "template": "markdown"
    }
    requests.post(push_url, data=data, timeout=15)
    print("✅ 推送成功")
else:
    print(msg)

print("🎉 v2.0 脚本运行完成")
