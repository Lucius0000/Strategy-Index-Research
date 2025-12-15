# -*- coding: utf-8 -*-
"""
- 从 yfinance 或 akshare 获取指定标的每年首个交易日的 Open 开盘价作为“年初价”
  * 规则：若 TICKER == '000300.SS'（沪深300），则走 akshare；否则走 yfinance
- 以相邻两年年初价之比计算年涨跌幅
- 按“幅度 × 连续性”逻辑给“周期”上色，其中幅度依据 return 的中位数，连续性阈值为 2 年
- 自动总结“市场周期”，打印到控制台，并追加到 cycle_coloring 表的下方
"""

import datetime as dt
import os
from typing import Tuple, List, Dict
import numpy as np
import pandas as pd
import yfinance as yf
import akshare as ak


# ========== 全局配置 ==========
# 常见指数（Yahoo Finance 代码）：
# 标普500：^GSPC    纳指综合：^IXIC
# 沪深300：000300.SS  恒生指数：^HSI
TICKER       = "000300.SS"   # 投资标的；若要用沪深300请设为 "000300.SS"
START_YEAR   = 2005      # 开始年份（如 2004）
AUTO_ADJUST  = True      # yfinance：是否使用调整后的 OHLC（适合长周期比较）
OUTPUT_FILE  = None      # 输出文件名；None 则自动生成 "output/<TICKER>_annual.xlsx"

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# ---------- 工具：akshare 数据规整 ----------
def _normalize_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """把中/英文常见列名规约成 Date/Open/High/Low/Close/Volume，并设 Date 为索引升序。"""
    if df is None or df.empty:
        return pd.DataFrame()
    mapping = {
        'date':'Date','日期':'Date','时间':'Date',
        'open':'Open','开盘':'Open',
        'high':'High','最高':'High',
        'low':'Low','最低':'Low',
        'close':'Close','收盘':'Close','收盘价':'Close',
        'volume':'Volume','成交量':'Volume'
    }
    df = df.rename(columns={c: mapping.get(c, c) for c in df.columns})
    keep = [c for c in ['Date','Open','High','Low','Close','Volume'] if c in df.columns]
    df = df.loc[:, keep].copy()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = (df.dropna(subset=['Date'])
                .drop_duplicates(subset=['Date'])
                .sort_values('Date')
                .set_index('Date'))
    return df

def _ak_symbol_for_hs300(ticker: str) -> str:
    """把 '000300.SS' 转成 ak 的 symbol 'sh000300'。"""
    code = ticker.split('.')[0]
    # 沪深300在上交所
    return f"sh{code}"


# 1) 数据抓取与整理（使用 Open 作为年初价）
def fetch_year_starts(symbol: str, start_year: int, end_date: str = None, auto_adjust: bool = True) -> pd.DataFrame:
    """
    下载指定 symbol 的日线数据，取每年首个交易日的“开盘价”(Open) 作为年初价。
    特别规则：symbol == '000300.SS' 时用 akshare 的股票指数日线。
    返回 DataFrame: [Year, start_price]
    """
    if end_date is None:
        end_date = dt.date.today().strftime("%Y-%m-%d")
    start_date = f"{start_year}-01-01"

    # —— 沪深300用 akshare —— #
    if symbol == "000300.SS":
        if ak is None:
            raise RuntimeError("当前环境未安装 akshare，请先 pip install akshare 再运行。")
        sym = _ak_symbol_for_hs300(symbol)         # 'sh000300'
        raw = ak.stock_zh_index_daily(symbol=sym)  # 接口：指数日线
        df = _normalize_price_columns(raw)         # 统一列名为 Date/Open/High/Low/Close/Volume
        if df.empty:
            raise RuntimeError("akshare 返回沪深300数据为空，请稍后重试。")

        # 只保留 start_date 之后的数据
        df = df.loc[df.index >= pd.to_datetime(start_date)].copy()

        # 使用 Open 列
        if "Open" not in df.columns:
            raise RuntimeError("akshare 数据中未找到 Open 列。")
        open_series = pd.to_numeric(df["Open"], errors="coerce")

        tmp = pd.DataFrame({"Open": open_series})
        tmp["Year"] = tmp.index.year

        # 每年首个交易日
        first_trading_day = tmp.groupby("Year").head(1).copy()
        first_trading_day = (first_trading_day
                             .reset_index(drop=False)
                             .rename(columns={"index": "first_trading_day"}))
        year_start = first_trading_day[["Year", "Open"]].rename(columns={"Open": "start_price"}).copy()
        year_start["start_price"] = pd.to_numeric(year_start["start_price"], errors="coerce")
        year_start = year_start.dropna(subset=["start_price"]).reset_index(drop=True)
        return year_start

    # —— 其它标的仍使用 yfinance —— #
    df = yf.download(
        symbol,
        start=start_date,
        end=end_date,
        auto_adjust=auto_adjust,
        progress=False,
        group_by="column"
    )
    if df is None or df.empty:
        raise RuntimeError(f"未从 yfinance 获取到 {symbol} 的数据，请检查代码或网络/时间区间。")

    df = df.loc[:, ~df.columns.duplicated()].sort_index()
    # 选择 Open 列；兼容多级列的极端情况
    open_col = None
    for cand in ["Open", "open"]:
        if cand in df.columns:
            open_col = cand
            break
    if open_col is None and isinstance(df.columns, pd.MultiIndex):
        try:
            opens = [c for c in df.columns if isinstance(c, tuple) and str(c[0]).lower() == "open"]
            open_col = opens[0] if opens else None
        except Exception:
            open_col = None
    if open_col is None:
        raise RuntimeError(f"{symbol} 数据中未找到 'Open' 列，现有列：{list(df.columns)}")

    open_series = df[open_col]
    if isinstance(open_series, pd.DataFrame):
        num_cols = [c for c in open_series.columns if pd.api.types.is_numeric_dtype(open_series[c])]
        open_series = open_series[num_cols[0]] if num_cols else open_series.iloc[:, 0]
    open_series = pd.to_numeric(open_series, errors="coerce")

    tmp = pd.DataFrame({"Open": open_series})
    tmp["Year"] = tmp.index.year

    first_trading_day = tmp.groupby("Year").head(1).copy()
    first_trading_day = first_trading_day.reset_index(drop=False).rename(columns={"index": "first_trading_day"})
    year_start = first_trading_day[["Year", "Open"]].rename(columns={"Open": "start_price"}).copy()
    year_start["start_price"] = pd.to_numeric(year_start["start_price"], errors="coerce")
    year_start = year_start.dropna(subset=["start_price"]).reset_index(drop=True)
    return year_start


def compute_returns_from_year_starts(year_start_df: pd.DataFrame) -> pd.DataFrame:
    """
    基于年初价计算“年涨跌幅”： (下一年年初价 / 当年年初价 - 1)
    注意：最后一年没有下一年年初价 -> annual_return 为 NaN；但要在最终表里保留最后一年“开始价”。
    """
    df = year_start_df.copy()
    df = df.loc[:, ~df.columns.duplicated()].sort_values("Year").reset_index(drop=True)

    sp = df["start_price"]
    if isinstance(sp, pd.DataFrame):
        sp = sp.iloc[:, 0].squeeze("columns")
    sp = pd.to_numeric(sp, errors="coerce")

    sp_next = sp.shift(-1)
    annual_return = sp_next / sp - 1.0

    df["start_price_next"] = sp_next.values
    df["annual_return"] = annual_return.values

    # 方向（NaN 记为 0；真正使用会基于 df_valid）
    def sign_func(x):
        if pd.isna(x): return 0
        if x > 0: return 1
        if x < 0: return -1
        return 0
    df["sign"] = df["annual_return"].apply(sign_func)

    # 连续性（遇到 0/NaN 断开）
    streak_len = []
    current_sign = 0
    current_len = 0
    for s in df["sign"]:
        if s == 0:
            current_sign = 0
            current_len = 0
            streak_len.append(0)
        else:
            if s == current_sign:
                current_len += 1
            else:
                current_sign = s
                current_len = 1
            streak_len.append(current_len)
    df["streak_len"] = streak_len
    return df


# 2) 自适应阈值与上色规则
def adaptive_thresholds(returns_series: pd.Series) -> Tuple[float, float]:
    """
    自适应阈值：
      正侧：正收益样本 |return| 的中位数；负侧：负收益样本 |return| 的中位数
      回退：全体 |return| 的 60% 分位数；兜底：15%
    """
    s = pd.Series(returns_series).dropna()
    pos = s[s > 0].abs()
    neg = s[s < 0].abs()
    overall = s.abs()

    pos_th = pos.median() if not pos.empty else np.nan
    neg_th = neg.median() if not neg.empty else np.nan
    fallback = overall.quantile(0.60) if not overall.empty else np.nan

    def resolve(th):
        if pd.isna(th):
            if not pd.isna(fallback) and fallback > 0:
                return float(fallback)
            else:
                return 0.15  # 15% 兜底
        return float(th)

    return resolve(pos_th), resolve(neg_th)


def classify_color(row: pd.Series, pos_th: float, neg_th: float, streak_threshold: int = 2) -> str:
    """
    返回：'深绿'/'浅绿'/'深红'/'浅红' 或 '中性'
    仅当 annual_return 非 NaN 时才会被调用（调用处使用 df_valid）。
    """
    s = row["sign"]
    r = abs(row["annual_return"])
    streak = row["streak_len"]
    if s == 0 or pd.isna(row["annual_return"]):
        return "中性"
    deep = (streak >= streak_threshold)
    if s > 0:
        deep = deep or (r >= pos_th)
        return "深绿" if deep else "浅绿"
    else:
        deep = deep or (r >= neg_th)
        return "深红" if deep else "浅红"


# === 市场周期自动总结 ===
def summarize_market_cycles(df: pd.DataFrame, pos_th: float, neg_th: float) -> List[Dict]:
    """
    将连续同向（sign相同）的年份合并为一个“市场周期段”，并按强弱标注：
      - 上涨段：若段内包含“深绿” 且 平均年化涨幅 >= 正侧阈值 -> "大牛"，否则 "震荡上行"
      - 下跌段：若段内包含“深红” 且 平均年化跌幅绝对值 >= 负侧阈值 -> "熊市"，否则 "震荡下行"
    仅使用 annual_return 非 NaN 的年份。
    """
    rows = df.dropna(subset=["annual_return"]).sort_values("Year").reset_index(drop=True)
    n = len(rows)
    i = 0
    cycles = []
    while i < n:
        sgn = int(rows.loc[i, "sign"])
        start_y = int(rows.loc[i, "Year"])
        j = i
        deep_count = 0
        sum_ret = 0.0
        cnt = 0
        while j < n and int(rows.loc[j, "sign"]) == sgn:
            cnt += 1
            r = float(rows.loc[j, "annual_return"])
            sum_ret += r
            col = str(rows.loc[j, "color"]) if "color" in rows.columns else ""
            if sgn > 0 and col == "深绿":
                deep_count += 1
            if sgn < 0 and col == "深红":
                deep_count += 1
            j += 1
        end_y = int(rows.loc[j-1, "Year"])

        if sgn > 0:
            avg_ret = sum_ret / max(cnt, 1)
            label = "大牛" if (deep_count > 0 and avg_ret >= pos_th) else "震荡上行"
        elif sgn < 0:
            avg_ret = sum_ret / max(cnt, 1)
            label = "熊市" if (deep_count > 0 and abs(avg_ret) >= neg_th) else "震荡下行"
        else:
            label = "震荡"

        cycles.append({"start": start_y, "end": end_y, "label": label})
        i = j
    return cycles

def format_period(start: int, end: int) -> str:
    return f"{start}-{end}" if start != end else f"{start}"


# 3) 导出到 Excel（追加“市场周期总结”）
def export_to_excel(df: pd.DataFrame, symbol: str, cycles: List[Dict], outfile: str = None):
    """
    输出：
      - annual_data（中文列名）：年份、开始价、涨跌、周期（周期真实着色；包含当前年份）
      - cycle_coloring：横向“周期”预览 + 下方“市场周期总结”
      - calc_tmp（隐藏）：中间过程检查
    """
    # 过渡表
    calc_tmp = df.copy()
    calc_tmp["annual_return(%)"] = (calc_tmp["annual_return"] * 100.0).round(2)
    calc_tmp = calc_tmp[[
        "Year", "start_price", "start_price_next", "annual_return", "annual_return(%)", "streak_len", "sign"
    ]]

    # 主输出表（中文列名，包含最后一年）
    df_full = df.sort_values("Year").reset_index(drop=True)
    annual_out = pd.DataFrame({
        "年份": df_full["Year"],
        "开始价": df_full["start_price"],
        "涨跌": df_full["annual_return"],  # 数值以比例存储，Excel 里设百分比格式
        "周期": ""
    })

    # 横向“周期”仅基于有涨跌年份
    df_valid = df_full.dropna(subset=["annual_return"]).reset_index(drop=True)
    years = df_valid["Year"].tolist()
    color_labels = df_valid["color"].tolist() if "color" in df_valid.columns else [""] * len(years)
    cycle_df = pd.DataFrame(columns=["周期"] + [str(y) for y in years])
    cycle_df.loc[0] = ["周期"] + color_labels

    # 文件名
    if outfile is None:
        symbol_safe = symbol.replace("^", "").replace("/", "_")
        outfile = f"output/{symbol_safe}_annual.xlsx"
    outdir = os.path.dirname(outfile)
    if outdir and not os.path.exists(outdir):
        os.makedirs(outdir, exist_ok=True)

    import xlsxwriter
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        annual_out.to_excel(writer, sheet_name="annual_data", index=False)
        cycle_df.to_excel(writer, sheet_name="cycle_coloring", index=False)
        calc_tmp.to_excel(writer, sheet_name="calc_tmp", index=False)

        wb  = writer.book
        ws1 = writer.sheets["annual_data"]
        ws2 = writer.sheets["cycle_coloring"]
        ws3 = writer.sheets["calc_tmp"]

        # 样式
        fmt_deep_green = wb.add_format({"bg_color": "#0B8043", "font_color": "#FFFFFF", "align": "center"})
        fmt_light_green = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "align": "center"})
        fmt_deep_red   = wb.add_format({"bg_color": "#9C0006", "font_color": "#FFFFFF", "align": "center"})
        fmt_light_red  = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "align": "center"})
        fmt_neutral    = wb.add_format({"align": "center"})
        header_fmt     = wb.add_format({"bold": True, "align": "center"})
        numfmt_2       = wb.add_format({"num_format": "0.00"})   # 两位小数
        pctfmt_2       = wb.add_format({"num_format": "0.00%"})  # 百分比两位小数

        # annual_data 列宽与格式（中文表头）
        for col_idx, col_name in enumerate(annual_out.columns):
            ws1.write(0, col_idx, col_name, header_fmt)
        # 列序：0 年份 | 1 开始价 | 2 涨跌 | 3 周期
        ws1.set_column(0, 0, 10)
        ws1.set_column(1, 1, 14, numfmt_2)
        ws1.set_column(2, 2, 12, pctfmt_2)
        ws1.set_column(3, 3, 8)

        # “周期”列真实上色（仅对有涨跌的年份着色）
        color_map_rows = {int(y): c for y, c in zip(df_valid["Year"].tolist(), color_labels)}
        for i, year in enumerate(annual_out["年份"].tolist()):
            label = color_map_rows.get(int(year), None)
            if label:
                fmt = fmt_neutral
                if label == "深绿": fmt = fmt_deep_green
                elif label == "浅绿": fmt = fmt_light_green
                elif label == "深红": fmt = fmt_deep_red
                elif label == "浅红": fmt = fmt_light_red
                ws1.write(i + 1, 3, "", fmt)

        # cycle_coloring 横向预览
        for col_idx, col_name in enumerate(cycle_df.columns):
            ws2.write(0, col_idx, col_name, header_fmt)
        ws2.write(1, 0, "周期", header_fmt)
        for i, label in enumerate(color_labels):
            col = i + 1
            fmt = fmt_neutral
            if label == "深绿": fmt = fmt_deep_green
            elif label == "浅绿": fmt = fmt_light_green
            elif label == "深红": fmt = fmt_deep_red
            elif label == "浅红": fmt = fmt_light_red
            ws2.write(1, col, "", fmt)
        for col_idx, col_name in enumerate(cycle_df.columns):
            ws2.set_column(col_idx, col_idx, max(6, len(str(col_name))))

        # 追加“市场周期总结”
        start_row = 3
        ws2.write(start_row, 0, "市场周期总结", header_fmt)
        ws2.write(start_row, 1, "阶段", header_fmt)
        for k, seg in enumerate(cycles, start=1):
            ws2.write(start_row + k, 0, format_period(seg["start"], seg["end"]))
            ws2.write(start_row + k, 1, seg["label"])

        ws3.hide()


# 4) 主流程
def main():
    symbol = str(TICKER)
    start_year = int(START_YEAR)
    auto_adjust = bool(AUTO_ADJUST)
    outfile = OUTPUT_FILE

    # 拉数据 & 计算
    year_start = fetch_year_starts(symbol=symbol, start_year=start_year, auto_adjust=auto_adjust)
    annual_df = compute_returns_from_year_starts(year_start)

    # 自适应阈值 + 上色（仅在 valid 年份上）
    pos_th, neg_th = adaptive_thresholds(annual_df["annual_return"])
    df_valid = annual_df.dropna(subset=["annual_return"]).copy()
    df_valid["color"] = df_valid.apply(
        lambda r: classify_color(r, pos_th=pos_th, neg_th=neg_th, streak_threshold=2),
        axis=1
    )
    # 将颜色写回原 df（无涨跌的年份保持为空）
    annual_df = annual_df.merge(
        df_valid[["Year", "color"]],
        on="Year",
        how="left"
    )

    # 市场周期自动总结（只基于 valid 年份）
    cycles = summarize_market_cycles(annual_df, pos_th=pos_th, neg_th=neg_th)

    # 控制台输出市场周期
    print("市场周期总结：")
    for seg in cycles:
        print(f"- {format_period(seg['start'], seg['end'])} {seg['label']}")
    print(f"阈值：pos_th={pos_th:.4f}  neg_th={neg_th:.4f}  （symbol={symbol}, start_year={start_year}, auto_adjust={auto_adjust}）")

    # 导出（annual_data 含当前年份开始价；cycle_coloring 及总结保持原逻辑）
    export_to_excel(annual_df, symbol=symbol, cycles=cycles, outfile=outfile)


if __name__ == "__main__":
    main()
