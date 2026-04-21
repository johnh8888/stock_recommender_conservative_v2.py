import akshare as ak
import pandas as pd
import requests
import os
import sys
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ==================== 配置 ====================
PUSHPLUS_TOKEN = "7045c58ecdfd490f859992abeaa0d557"
TOTAL_CAPITAL = 20000
FIX_AMOUNT = int(TOTAL_CAPITAL * 0.6)

now = datetime.utcnow() + timedelta(hours=8)
today = now.strftime("%Y%m%d")
week_num = now.weekday()

def push(title: str, content: str):
    try:
        requests.post(
            "http://www.pushplus.plus/send",
            json={"token": PUSHPLUS_TOKEN, "title": title,
                  "content": content, "template": "markdown"},
            timeout=15
        )
    except:
        pass

# 周五强制空仓
if week_num == 4:
    push("稳健套利v8-lite｜周五空仓", f"## {today} 今日星期五 强制空仓\n\n绝不持股过周末，周一再战。")
    print("周五强制空仓")
    sys.exit(0)

print(f"[{now.strftime('%H:%M:%S')}] 开始运行稳健套利v8-lite...")

# 带重试机制的请求函数
def fetch_with_retry(func, max_retries=5, base_delay=5):
    for attempt in range(max_retries):
        try:
            result = func()
            if result is not None and (not isinstance(result, pd.DataFrame) or not result.empty):
                return result
        except Exception as e:
            print(f"第{attempt+1}次请求失败: {type(e).__name__} - {e}")
        time.sleep(base_delay * (attempt + 1))
    print("所有重试均失败")
    return pd.DataFrame() if isinstance(func(), pd.DataFrame) else None

# 获取市场数据
raw_df = fetch_with_retry(ak.stock_zh_a_spot_em)

try:
    sh_row = raw_df[raw_df["名称"] == "上证指数"]
    market_pct = float(sh_row["涨跌幅"].iloc[0]) if not sh_row.empty else 0.0
except:
    market_pct = 0.0

# 数据处理
df = raw_df.rename(columns={
    "代码": "code", "名称": "name", "最新价": "price",
    "涨跌幅": "pct", "成交额": "amount", "量比": "lb",
    "换手率": "turnover", "5分钟涨跌": "five_min_pct",
    "振幅": "amplitude"
})

for col in ["price", "pct", "amount", "lb", "turnover", "five_min_pct", "amplitude"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# 过滤高危标的
ban_pattern = r"(^ST|^\*ST|退市|^N\d|^C[^N]|XD|XR)"
df = df[~df["name"].str.contains(ban_pattern, na=False, regex=True)]
df = df[(df["code"].str.startswith("60")) | (df["code"].str.startswith("00"))]

if market_pct < -0.5:
    push("稳健套利v8-lite｜大盘弱势空仓", f"## {today} 大盘下跌 {market_pct:.2f}%，今日强制空仓保护资金")
    print("大盘弱势，空仓")
    sys.exit(0)

# 稳健选股
filtered = df[
    (df["pct"] >= 0.6) & (df["pct"] <= 2.8) &
    (df["amount"] >= 8e7) &
    (df["lb"] >= 1.15) & (df["lb"] <= 1.75) &
    (df["price"] >= 5.5) &
    (df["turnover"] >= 1.8) & (df["turnover"] <= 11) &
    (df["five_min_pct"] >= -0.8) &
    (df["amplitude"] <= 5.5)
].copy()

# 资金流向 + 热门概念过滤
hot_stocks = set()
try:
    flow_df = fetch_with_retry(ak.stock_fund_flow_concept)
    if not flow_df.empty:
        inflow_cols = [col for col in flow_df.columns if '流入' in col or '净' in col]
        inflow_col = inflow_cols[0] if inflow_cols else None
        if inflow_col:
            top_concepts = flow_df.nlargest(5, inflow_col)['概念名称'].head(3).tolist()
            for concept in top_concepts:
                try:
                    cons = fetch_with_retry(lambda c=concept: ak.stock_board_concept_cons(symbol=c))
                    if not cons.empty and '代码' in cons.columns:
                        hot_stocks.update(cons['代码'].astype(str).str.zfill(6).tolist())
                except:
                    continue
except Exception as e:
    print(f"概念过滤失败: {e}")

if hot_stocks:
    filtered = filtered[filtered['code'].isin(hot_stocks)]

if filtered.empty:
    push("稳健套利v8-lite｜今日无标的", f"## {today} 今日无符合条件标的\n\n空仓观望\n大盘：{market_pct:.2f}%")
    print("今日无符合条件的标的，空仓")
    sys.exit(0)

# 选出最佳标的
filtered = filtered.sort_values("pct", ascending=False)
candidate = filtered.iloc[0]
code = candidate["code"]
p = float(candidate["price"])

# 增强版新闻情感分析
def get_news_sentiment(code):
    try:
        news = fetch_with_retry(lambda: ak.stock_news_em(stock=code), max_retries=4)
        if news is None or news.empty:
            return 0.5, ["新闻接口临时不可用，建议手动查看东方财富 '600392' 最新公告"]
        
        positive = ['利好', '上涨', '增长', '订单', '合作', '业绩', '回购', '规划', '发展', '扩张']
        negative = ['利空', '处罚', '亏损', '违规', '减持', '风险', '调查', '诉讼']
        
        scores = []
        texts = []
        for _, row in news.head(6).iterrows():
            text = str(row.get('标题', '')) + " " + str(row.get('内容', ''))
            pos_score = sum(1 for w in positive if w in text)
            neg_score = sum(1 for w in negative if w in text)
            score = pos_score - neg_score
            scores.append(score)
            texts.append(row['标题'][:65] + "...")
        
        avg_sentiment = sum(scores) / len(scores) if scores else 0.5
        return avg_sentiment, texts[:3]
    except Exception as e:
        print(f"新闻获取异常: {e}")
        return 0.5, ["新闻接口超时，推荐参考公司近期回购公告及行业规划"]

sentiment, news_list = get_news_sentiment(code)

# 强利空保护
if sentiment <= -2.0:
    push("稳健套利v8-lite｜利空空仓", f"## {today} 检测到较强利空\n情感分 {sentiment:.1f}，今日强制空仓")
    print("新闻利空，空仓")
    sys.exit(0)

# 推送内容
news_summary = "\n".join([f"• {n}" for n in news_list])
content = f"""
## {today} 稳健套利v8-lite｜今日推荐

**大盘**：{market_pct:.2f}%  
**新闻情感**：{sentiment:.1f}（正数越大利好越强）

**推荐股票**：**{candidate['name']} ({code})**  
**当前价**：{p:.2f} 元  
**低吸参考价**：{round(p * 0.99, 2)} 元  
**保本止盈**：{round(p * 1.022, 2)} 元  
**目标止盈**：{round(p * 1.035, 2)} 元  
**防守止损**：{round(p * 0.968, 2)} 元

**最新新闻**：
{news_summary}

**操作纪律**（必须严格执行）：
1. 次日只挂低吸参考价，绝不追高
2. 固定下单 {FIX_AMOUNT} 元，不加仓不补仓
3. 涨到保本止盈先落袋部分利润
4. 强势冲高可拿到目标止盈全部清仓
5. 跌破止损位严格离场
6. 周五上午必须清仓

长期坚持纪律 + 严格风控 = 每周稳定小赚的关键。
"""

push("稳健套利v8-lite｜今日推荐", content)
print(f"✅ 运行完成！推荐：{candidate['name']} ({code}) | 新闻情感：{sentiment:.1f}")