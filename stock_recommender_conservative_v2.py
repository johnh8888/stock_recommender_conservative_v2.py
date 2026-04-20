<FILE filename="stock_recommender_conservative_v8.py" size="17850 bytes">
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

# ==================== 基础过滤 + v7 极致筛选 ====================
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

# ==================== v7 概念板块 + 资金流向过滤 ====================
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

stock = None

if not filtered.empty:
    filtered["score"] = (
        filtered["pct"] * 1.6 + filtered["lb"] * 2.3 +
        (filtered["amount"] / 1e8) * 0.9 + filtered["turnover"] * 1.3 +
        filtered["five_min_pct"] * 3.5 + filtered["rise_speed"] * 2.0 +
        (filtered["sixty_day_pct"] / 15) + (60 - filtered["pe"]) * 0.15 +
        (5.5 - filtered["pb"]) * 0.18
    )
    filtered = filtered.sort_values("score", ascending=False).head(8)

    # ==================== v8 新增：个股新闻自动抓取 + 情感分析（影响买入卖出决策） ====================
    def get_stock_news_and_sentiment(code: str):
        try:
            news_df = ak.stock_news_em(stock=code)  # 东财个股最新20条新闻
            if news_df.empty:
                return 0.0, ["暂无最新新闻"], None
            # 保留关键列（标题、内容、发布时间）
            news_df = news_df[['标题', '内容', '发布时间']].head(5).copy()
            positive_kw = ['利好', '上涨', '增长', '合作', '订单', '业绩', '回购', '收购', '新项目', '扩张']
            negative_kw = ['利空', '处罚', '调查', '亏损', '违规', '风险', '下跌', '退市', '诉讼', '减持']
            
            recent_news = []
            scores = []
            for _, row in news_df.iterrows():
                text = str(row['标题']) + " " + str(row.get('内容', ''))
                pos_score = sum(1 for w in positive_kw if w in text)
                neg_score = sum(1 for w in negative_kw if w in text)
                score = pos_score - neg_score
                scores.append(score)
                recent_news.append(f"{row['发布时间'][:10]} | {row['标题'][:60]}... ({score:+d})")
            
            avg_sentiment = sum(scores) / len(scores) if scores else 0.0
            return avg_sentiment, recent_news[:3], news_df
        except Exception as e:
            print(f"新闻抓取异常: {e}")
            return 0.0, ["新闻获取失败"], None

    # 对Top1候选进行新闻分析（影响最终决策）
    candidate = filtered.iloc[0]
    code = candidate["code"]
    p = float(candidate["price"])
    
    news_sentiment, recent_news_list, news_df = get_stock_news_and_sentiment(code)
    
    # v8 决策逻辑：新闻影响买入/卖出参数
    if news_sentiment <= -1.5:  # 强利空 → 直接空仓（保守风控）
        content = f"## {today} v8新闻强利空检测\n\n检测到近期重大利空新闻（情感分 {news_sentiment:.1f}），v8强制空仓保护资金。\n\n最新新闻：\n" + "\n".join(recent_news_list)
        push("稳健套利v8｜新闻利空空仓", content)
        print(f"今日因新闻利空强制空仓（{candidate['name']}）")
        sys.exit(0)
    
    # ML + 分钟线预测（v6保留）
    def ml_price_predict(code: str, current_price: float):
        try:
            end_date = today
            start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
            hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
            if len(hist) < 30:
                return None, None, None, None

            hist["pct"] = hist["收盘"].pct_change()
            hist["vol_5"] = hist["成交量"].rolling(5).mean()
            hist["rsi"] = ta.rsi(hist["收盘"], length=14)
            hist["macd"] = ta.macd(hist["收盘"])["MACD_12_26_9"]
            hist["return_5"] = hist["收盘"].pct_change(5)

            min_df = ak.stock_zh_a_minute(symbol=code, period="1", adjust="qfq")
            if not min_df.empty:
                today_min = min_df[min_df["day"] == min_df["day"].max()]
                intraday_pct = (today_min["close"].iloc[-1] / today_min["close"].iloc[0] - 1) * 100 if len(today_min) > 1 else 0
                min_vol = today_min["volume"].mean()
                min_volatility = today_min["high"].max() - today_min["low"].min()
            else:
                intraday_pct = min_vol = min_volatility = 0

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

            model_return = xgb.XGBRegressor(n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42)
            model_low = xgb.XGBRegressor(n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42)
            model_high = xgb.XGBRegressor(n_estimators=80, max_depth=4, learning_rate=0.1, random_state=42)

            model_return.fit(X, y_return)
            model_low.fit(X, y_low)
            model_high.fit(X, y_high)

            pred_return = model_return.predict(today_feat)[0]
            pred_low = model_low.predict(today_feat)[0]
            pred_high = model_high.predict(today_feat)[0]

            ml_buy_ref = round(max(pred_low * 0.995, current_price * 0.985), 2)
            ml_tp1 = round(pred_high * 0.98, 2) if pred_return > 0.022 else round(current_price * 1.022, 2)
            ml_tp2 = round(pred_high, 2)
            ml_expected_pct = round(pred_return * 100, 2)

            return ml_buy_ref, ml_tp1, ml_tp2, ml_expected_pct
        except:
            return None, None, None, None

    ml_buy, ml_tp1, ml_tp2, ml_pct = ml_price_predict(code, p)

    if ml_buy is None:
        ml_buy = round(p * 0.990, 2)
        ml_tp1 = round(p * 1.022, 2)
        ml_tp2 = round(p * 1.035, 2)
        ml_pct = 0.0
        use_ml = False
    else:
        use_ml = True

    stock = candidate
    buy_ref = ml_buy
    tp1 = ml_tp1
    tp2 = ml_tp2
    stop = round(p * 0.968, 2)

    # v8 新闻影响买入卖出参数调整
    news_impact = "中性"
    if news_sentiment >= 1.0:
        news_impact = "利好"
        tp2 = round(tp2 * 1.012, 2)   # 利好 → 目标止盈略上调
    elif news_sentiment <= -0.8:
        news_impact = "轻微利空"
        stop = round(p * 0.978, 2)    # 利空 → 止损位收紧（-2.2%）

    # ==================== 历史记录 + 自动胜率 + 新增新闻情感 ====================
    csv_file = "recommendation_history.csv"
    new_row = pd.DataFrame([{
        "date": today, "code": code, "name": stock["name"], "price": p,
        "pct": stock["pct"], "ml_expected_pct": ml_pct, "ml_used": use_ml,
        "news_sentiment": round(news_sentiment, 2), "news_impact": news_impact,
        "realized_pct": pd.NA, "win": pd.NA
    }])

    if os.path.exists(csv_file):
        history_df = pd.read_csv(csv_file)
        already_today = ((history_df["date"] == today) & (history_df["code"] == code)).any()
        this_week = history_df[history_df["date"].str[:6] == today[:6]]
        already_this_week = (this_week["code"] == code).any()
        if not already_today and not already_this_week:
            history_df = pd.concat([history_df, new_row], ignore_index=True)
        
        # 自动更新历史胜率 & 新闻字段
        if 'realized_pct' not in history_df.columns: history_df['realized_pct'] = pd.NA
        if 'win' not in history_df.columns: history_df['win'] = pd.NA
        if 'news_sentiment' not in history_df.columns: history_df['news_sentiment'] = pd.NA
        
        for idx, row in history_df.iterrows():
            if pd.isna(row.get('realized_pct')) and str(row['date']) != today:
                try:
                    rec_date = datetime.strptime(str(row['date']), "%Y%m%d")
                    next_start = (rec_date + timedelta(days=1)).strftime("%Y%m%d")
                    hist_next = ak.stock_zh_a_hist(symbol=str(row['code']), period="daily",
                                                   start_date=next_start,
                                                   end_date=(rec_date + timedelta(days=6)).strftime("%Y%m%d"),
                                                   adjust="qfq")
                    if not hist_next.empty:
                        next_close = float(hist_next['收盘'].iloc[0])
                        realized = round((next_close / float(row['price']) - 1) * 100, 2)
                        history_df.at[idx, 'realized_pct'] = realized
                        history_df.at[idx, 'win'] = 1 if realized >= 1.0 else 0
                except:
                    continue
        history_df.to_csv(csv_file, index=False, encoding="utf-8")
        count = len(history_df)
    else:
        history_df = new_row
        history_df.to_csv(csv_file, index=False, encoding="utf-8")
        count = 1

    # 胜率统计
    valid_df = history_df.dropna(subset=['realized_pct'])
    total_trades = len(valid_df)
    win_rate = (valid_df['win'].sum() / total_trades * 100) if total_trades > 0 else 0
    avg_return = valid_df['realized_pct'].mean() if total_trades > 0 else 0
    recent_df = valid_df.tail(20)
    recent_win_rate = (recent_df['win'].sum() / len(recent_df) * 100) if len(recent_df) > 0 else 0

    # ==================== 推送（包含最新新闻 + 新闻对买入卖出的影响） ====================
    ml_note = "✅ ML分钟线预测已启用" if use_ml else "⚠️ ML预测失败，使用固定参数"
    news_summary = "\n".join([f"• {n}" for n in recent_news_list])
    content = f"""
## {today} 稳健套利v8｜ML+资金流向+概念+新闻情感分析（自动影响买卖决策）

**大盘**：{market_pct:.2f}%　　**ML预测次日收益率**：{ml_pct:+.2f}%　　**新闻情感分**：{news_sentiment:.1f}（{news_impact}）

**v8核心升级**：
- 自动抓取个股最新新闻（东财 ak.stock_news_em）
- 新闻情感分析（利好/利空关键词）→ 动态调整止盈止损位
- 强利空直接空仓；利好上调目标价；轻微利空收紧止损

| 股票 | 今日涨幅 | 操作金额 | ML低吸参考价 | 保本止盈 | ML目标止盈 | 防守止损 | 5分钟 | ML预测 | 新闻影响 |
|------|----------|----------|--------------|----------|------------|----------|-------|--------|----------|
| {stock['name']}({code}) | {stock['pct']:.2f}% | {FIX_AMOUNT}元 | {buy_ref} | {tp1} | {tp2} | {stop} | {stock.get('five_min_pct',0):.2f}% | {ml_pct:+.2f}% | {news_impact} |

**最新新闻摘要（情感影响买卖决策）**：
{news_summary}

**自动胜率统计**（截至今日）：
- 总交易次数：{total_trades} 次
- 总胜率：{win_rate:.1f}%（≥1%视为胜）
- 近20笔胜率：{recent_win_rate:.1f}%
- 平均收益：{avg_return:+.2f}%

**操作纪律**：次日只挂ML低吸价，不追高；固定金额；严格按调整后止盈止损执行；周五上午清仓。

累计推荐：{count} 只｜v8已实现「新闻实时影响买入卖出」
{ml_note}
"""
    push("稳健套利v8｜新闻情感+动态买卖版", content)
    print(f"运行完成｜推荐：{stock['name']} | ML预测：{ml_pct:+.2f}% | 新闻情感：{news_sentiment:.1f}（{news_impact}） | 胜率：{win_rate:.1f}%")

else:
    content = f"## {today} v8极致筛选无标的\n\n今日空仓观望。大盘：{market_pct:.2f}%"
    push("稳健套利v8｜今日空仓", content)
    print("今日无符合v8极致条件的标的，空仓")
</FILE>
