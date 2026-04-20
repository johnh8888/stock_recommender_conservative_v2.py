import akshare as ak
import pandas as pd
import requests
import os
import sys
import re
from datetime import datetime, timedelta
import warnings
import xgboost as xgb
import pandas_ta as ta
warnings.filterwarnings("ignore")

# ==================== 配置参数 ====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
TOTAL_CAPITAL = 20000
TRADE_RATIO = 0.6
FIX_AMOUNT = int(TOTAL_CAPITAL * TRADE_RATIO)

now      = datetime.utcnow() + timedelta(hours=8)
today    = now.strftime("%Y%m%d")
week_num = now.weekday()

def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        requests.post("http://www.pushplus.plus/send",
                      json={"token": PUSHPLUS_TOKEN, "title": title,
                            "content": content, "template": "markdown"},
                      timeout=10)

# ==================== 周五强制空仓 ====================
if week_num == 4:
    push("稳健套利v8｜周五空仓", f"## {today} 今日星期五 强制空仓\n\n绝不持股过周末，周一再战。")
    sys.exit(0)

# ==================== 获取市场数据 ====================
raw_df = ak.stock_zh_a_spot_em()
try:
    market_pct = float(raw_df[raw_df["名称"] == "上证指数"]["涨跌幅"].iloc[0])
except:
    market_pct = 0.0

df = raw_df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price",
    "涨跌幅": "pct", "成交额": "amount", "量比": "lb",
    "换手率": "turnover", "5分钟涨跌": "five_min_pct",
    "60日涨跌": "sixty_day_pct", "涨速": "rise_speed",
    "振幅": "amplitude", "市盈率-动态": "pe", "市净率": "pb"
})
numeric_cols = ["price", "pct", "amount", "lb", "turnover", "five_min_pct",
                "sixty_day_pct", "rise_speed", "amplitude", "pe", "pb"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ==================== 基础过滤 ====================
ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].str.startswith("60")) | (df["code"].str.startswith("00"))]

if market_pct < -0.4:
    push("稳健套利v8｜大盘弱势空仓", f"## {today} 大盘下跌 {market_pct:.2f}%，v8强制空仓\n\n保护资金。")
    sys.exit(0)

filtered = df[
    (df["pct"]          >= 0.7) & (df["pct"]          <= 2.3) &
    (df["amount"]       >= 1.2e8) &
    (df["lb"]           >= 1.25) & (df["lb"]           <= 1.55) &
    (df["price"]        >= 6) &
    (df["turnover"]     >= 2.2) & (df["turnover"]     <= 9.5) &
    (df["five_min_pct"] >= 0.2) &
    (df["sixty_day_pct"]>= -6.0) &
    (df["amplitude"]    <= 4.5) &
    (df["pe"]           >  0) & (df["pe"] < 55) &
    (df["pb"]           >= 0.9) & (df["pb"] <= 5.5) &
    (df["rise_speed"]   >  0.08)
].copy()

# ==================== 概念板块 + 资金流向过滤 ====================
hot_stocks = set()
try:
    flow_df = ak.stock_fund_flow_concept()
    if not flow_df.empty:
        inflow_col = next((col for col in flow_df.columns if '净流入' in col or '流入' in col), '净流入')
        flow_df[inflow_col] = pd.to_numeric(flow_df[inflow_col], errors='coerce')
        top_flow = flow_df.nlargest(5, inflow_col)
        top_flow_concepts = top_flow['概念名称'].tolist() if '概念名称' in top_flow.columns else top_flow.iloc[:,0].tolist()
        for concept in top_flow_concepts[:3]:
            try:
                cons = ak.stock_board_concept_cons(symbol=concept)
                if '代码' in cons.columns:
                    codes = cons['代码'].astype(str).str.zfill(6).tolist()
                    hot_stocks.update(codes)
            except:
                continue
except:
    pass

if hot_stocks:
    filtered = filtered[filtered['code'].isin(hot_stocks)]

if filtered.empty:
    content = f"## {today} v8极致筛选无标的\n\n今日空仓观望。大盘：{market_pct:.2f}%"
    push("稳健套利v8｜今日空仓", content)
    print("今日无符合v8条件的标的，空仓")
    sys.exit(0)

filtered["score"] = (
    filtered["pct"] * 1.6 + filtered["lb"] * 2.3 +
    (filtered["amount"] / 1e8) * 0.9 + filtered["turnover"] * 1.3 +
    filtered["five_min_pct"] * 3.5 + filtered["rise_speed"] * 2.0 +
    (filtered["sixty_day_pct"] / 15) + (60 - filtered["pe"]) * 0.15 +
    (5.5 - filtered["pb"]) * 0.18
)
filtered = filtered.sort_values("score", ascending=False).head(8)

# ==================== 新闻情感分析 + ML预测 ====================
def get_stock_news_and_sentiment(code: str):
    try:
        news_df = ak.stock_news_em(stock=code)
        if news_df.empty:
            return 0.0, ["暂无最新新闻"]
        news_df = news_df[['标题', '内容', '发布时间']].head(5).copy()
        positive_kw = ['利好', '上涨', '增长', '合作', '订单', '业绩', '回购', '收购', '新项目']
        negative_kw = ['利空', '处罚', '调查', '亏损', '违规', '风险', '下跌', '退市', '诉讼', '减持']
        
        recent_news = []
        scores = []
        for _, row in news_df.iterrows():
            text = str(row['标题']) + " " + str(row.get('内容', ''))
            pos = sum(1 for w in positive_kw if w in text)
            neg = sum(1 for w in negative_kw if w in text)
            score = pos - neg
            scores.append(score)
            recent_news.append(f"{row['发布时间'][:10]} | {row['标题'][:60]}... ({score:+d})")
        
        return sum(scores)/len(scores) if scores else 0.0, recent_news[:3]
    except:
        return 0.0, ["新闻获取失败"]

candidate = filtered.iloc[0]
code = candidate["code"]
p = float(candidate["price"])
news_sentiment, recent_news_list = get_stock_news_and_sentiment(code)

# 强利空直接空仓
if news_sentiment <= -1.5:
    content = f"## {today} v8新闻强利空\n\n情感分 {news_sentiment:.1f}，强制空仓保护资金。\n\n最新新闻：\n" + "\n".join(recent_news_list)
    push("稳健套利v8｜新闻利空空仓", content)
    print("因新闻强利空强制空仓")
    sys.exit(0)

# ML预测
def ml_price_predict(code: str, current_price: float):
    try:
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=today, adjust="qfq")
        if len(hist) < 30:
            raise Exception("数据不足")
        
        hist["pct"] = hist["收盘"].pct_change()
        hist["vol_5"] = hist["成交量"].rolling(5).mean()
        hist["rsi"] = ta.rsi(hist["收盘"], length=14)
        hist["macd"] = ta.macd(hist["收盘"])["MACD_12_26_9"]
        hist["return_5"] = hist["收盘"].pct_change(5)

        min_df = ak.stock_zh_a_minute(symbol=code, period="1", adjust="qfq")
        intraday_pct = min_vol = min_volatility = 0
        if not min_df.empty:
            today_min = min_df[min_df["day"] == min_df["day"].max()]
            if len(today_min) > 1:
                intraday_pct = (today_min["close"].iloc[-1] / today_min["close"].iloc[0] - 1) * 100

        hist = hist.dropna().copy()
        features = ["pct", "成交量", "换手率", "rsi", "macd", "return_5", "vol_5"]
        X = hist[features].iloc[:-1]
        y_return = hist["pct"].shift(-1).iloc[:-1]
        y_low = hist["最低"].shift(-1).iloc[:-1]
        y_high = hist["最高"].shift(-1).iloc[:-1]

        today_feat = hist[features].iloc[-1:].copy()
        today_feat["intraday_pct"] = intraday_pct
        today_feat["min_vol"] = min_vol
        today_feat["min_volatility"] = min_volatility

        model = xgb.XGBRegressor(n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42)
        model_return = model.fit(X, y_return)
        model_low = model.fit(X, y_low)
        model_high = model.fit(X, y_high)

        pred_return = model_return.predict(today_feat)[0]
        pred_low = model_low.predict(today_feat)[0]
        pred_high = model_high.predict(today_feat)[0]

        ml_buy = round(max(pred_low * 0.995, current_price * 0.985), 2)
        ml_tp1 = round(pred_high * 0.98, 2) if pred_return > 0.022 else round(current_price * 1.022, 2)
        ml_tp2 = round(pred_high, 2)
        ml_pct = round(pred_return * 100, 2)
        return ml_buy, ml_tp1, ml_tp2, ml_pct
    except:
        return round(current_price * 0.99, 2), round(current_price * 1.022, 2), round(current_price * 1.035, 2), 0.0

ml_buy, ml_tp1, ml_tp2, ml_pct = ml_price_predict(code, p)

# 新闻影响调整
news_impact = "中性"
stop = round(p * 0.968, 2)
if news_sentiment >= 1.0:
    news_impact = "利好"
    ml_tp2 = round(ml_tp2 * 1.012, 2)
elif news_sentiment <= -0.8:
    news_impact = "轻微利空"
    stop = round(p * 0.978, 2)

# ==================== 历史记录 & 胜率统计 ====================
csv_file = "recommendation_history.csv"
new_row = pd.DataFrame([{
    "date": today, "code": code, "name": candidate["name"], "price": p,
    "pct": candidate["pct"], "ml_expected_pct": ml_pct, "news_sentiment": round(news_sentiment, 2),
    "news_impact": news_impact, "realized_pct": pd.NA, "win": pd.NA
}])

if os.path.exists(csv_file):
    history_df = pd.read_csv(csv_file)
    already_today = ((history_df["date"] == today) & (history_df["code"] == code)).any()
    this_week = history_df[history_df["date"].str[:6] == today[:6]]
    already_this_week = (this_week["code"] == code).any()
    if not already_today and not already_this_week:
        history_df = pd.concat([history_df, new_row], ignore_index=True)
    # 更新历史胜率（省略自动更新部分，保持简洁）
    history_df.to_csv(csv_file, index=False, encoding="utf-8")
    count = len(history_df)
else:
    history_df = new_row
    history_df.to_csv(csv_file, index=False, encoding="utf-8")
    count = 1

valid_df = history_df.dropna(subset=['realized_pct']) if 'realized_pct' in history_df.columns else pd.DataFrame()
total_trades = len(valid_df)
win_rate = (valid_df['win'].sum() / total_trades * 100) if total_trades > 0 else 0
recent_win_rate = win_rate
avg_return = valid_df['realized_pct'].mean() if total_trades > 0 else 0

# ==================== 推送 ====================
news_summary = "\n".join([f"• {n}" for n in recent_news_list])
content = f"""
## {today} 稳健套利v8｜新闻情感实时影响买卖

**大盘**：{market_pct:.2f}%　**ML预测**：{ml_pct:+.2f}%　**新闻情感**：{news_sentiment:.1f}（{news_impact}）

| 股票 | 涨幅 | 金额 | ML低吸价 | 目标止盈 | 止损 | 新闻影响 |
|------|------|------|----------|----------|------|----------|
| {candidate['name']}({code}) | {candidate['pct']:.2f}% | {FIX_AMOUNT}元 | {ml_buy} | {ml_tp2} | {stop} | {news_impact} |

**最新新闻**：
{news_summary}

**胜率统计**：总胜率 {win_rate:.1f}% | 平均收益 {avg_return:+.2f}%

**纪律**：只挂ML低吸价，不追高；严格执行调整后的止盈止损；周五清仓。
"""
push("稳健套利v8｜运行完成", content)
print(f"运行完成 | 推荐：{candidate['name']} | 新闻情感：{news_sentiment:.1f}")
