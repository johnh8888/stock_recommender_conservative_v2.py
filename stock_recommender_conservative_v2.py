import akshare as ak
import pandas as pd
import requests
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ==================== 配置 ====================
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN")
TOTAL_CAPITAL = 20000
FIX_AMOUNT = int(TOTAL_CAPITAL * 0.6)

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()

def push(title: str, content: str):
    if PUSHPLUS_TOKEN:
        requests.post("http://www.pushplus.plus/send",
                      json={"token": PUSHPLUS_TOKEN, "title": title,
                            "content": content, "template": "markdown"},
                      timeout=10)

if week_num == 4:
    push("稳健套利v8-lite｜周五空仓", f"## {today} 今日星期五 强制空仓\n\n绝不持股过周末。")
    sys.exit(0)

# 获取数据
raw_df = ak.stock_zh_a_spot_em()
try:
    market_pct = float(raw_df[raw_df["名称"] == "上证指数"]["涨跌幅"].iloc[0])
except:
    market_pct = 0.0

df = raw_df.rename(columns={"代码": "code", "名称": "name", "最新价": "price",
                            "涨跌幅": "pct", "成交额": "amount", "量比": "lb",
                            "换手率": "turnover", "5分钟涨跌": "five_min_pct",
                            "振幅": "amplitude", "市盈率-动态": "pe", "市净率": "pb"})

for col in ["price", "pct", "amount", "lb", "turnover", "five_min_pct", "amplitude", "pe", "pb"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# 过滤
ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].str.startswith("60")) | (df["code"].str.startswith("00"))]

if market_pct < -0.4:
    push("稳健套利v8-lite｜大盘弱势空仓", f"## {today} 大盘下跌 {market_pct:.2f}%，强制空仓")
    sys.exit(0)

filtered = df[
    (df["pct"] >= 0.7) & (df["pct"] <= 2.5) &
    (df["amount"] >= 1.2e8) &
    (df["lb"] >= 1.2) & (df["lb"] <= 1.7) &
    (df["price"] >= 6) &
    (df["turnover"] >= 2.0) & (df["turnover"] <= 10) &
    (df["five_min_pct"] >= 0) &
    (df["amplitude"] <= 5)
].copy()

# 资金流向+概念过滤
hot_stocks = set()
try:
    flow_df = ak.stock_fund_flow_concept()
    if not flow_df.empty:
        inflow_col = next((col for col in flow_df.columns if '流入' in col), None)
        if inflow_col:
            top_concepts = flow_df.nlargest(5, inflow_col)['概念名称'].tolist()[:3]
            for concept in top_concepts:
                try:
                    cons = ak.stock_board_concept_cons(symbol=concept)
                    hot_stocks.update(cons['代码'].astype(str).str.zfill(6).tolist())
                except:
                    continue
except:
    pass

if hot_stocks:
    filtered = filtered[filtered['code'].isin(hot_stocks)]

if filtered.empty:
    push("稳健套利v8-lite｜无标的", f"## {today} 今日无符合条件标的\n\n空仓观望")
    print("今日无标的，空仓")
    sys.exit(0)

filtered = filtered.sort_values("pct", ascending=False).head(5)
candidate = filtered.iloc[0]
code = candidate["code"]
p = float(candidate["price"])

# 新闻情感
def get_news_sentiment(code):
    try:
        news = ak.stock_news_em(stock=code)
        if news.empty:
            return 0.0, ["无新闻"]
        positive = ['利好','上涨','增长','订单','合作','回购','业绩']
        negative = ['利空','处罚','亏损','违规','减持','风险']
        scores = []
        texts = []
        for _, row in news.head(4).iterrows():
            text = str(row.get('标题','')) + str(row.get('内容',''))
            score = sum(1 for w in positive if w in text) - sum(1 for w in negative if w in text)
            scores.append(score)
            texts.append(row['标题'][:50])
        return sum(scores)/len(scores) if scores else 0, texts
    except:
        return 0.0, ["新闻获取失败"]

sentiment, news_list = get_news_sentiment(code)

if sentiment <= -2:
    push("v8-lite｜利空空仓", f"## {today} 检测到强利空\n\n情感分 {sentiment:.1f}，强制空仓")
    sys.exit(0)

# 推送
content = f"""
## {today} 稳健套利v8-lite｜推荐

**大盘**：{market_pct:.2f}%　**新闻情感**：{sentiment:.1f}

**推荐股票**：{candidate['name']}({code})  
**当前价**：{p}  
**建议低吸参考**：{round(p*0.99,2)}  
**目标止盈**：{round(p*1.035,2)}  
**止损**：{round(p*0.968,2)}

**最新新闻**：
""" + "\n".join([f"• {n}" for n in news_list[:3]])

push("稳健套利v8-lite｜今日推荐", content)
print(f"运行完成 | 推荐：{candidate['name']}({code}) | 新闻情感：{sentiment:.1f}")
