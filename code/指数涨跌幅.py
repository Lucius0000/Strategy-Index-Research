# -*- coding: utf-8 -*-
"""
cycles_return.py
依据“市场周期表格”给出的周期开始/结束月份（精确到月），
取当月首个交易日的开盘价，计算每个周期的指数涨跌幅，
并保存原始指数日线到 output/raw_data/<TICKER>_涨跌幅历史数据.xlsx。

使用说明（配置区见下方）：
- 中国指数数据用 akshare；香港/美国用 yfinance
- 周期表需含“开始时间”“结束时间”（或等价中英文列名），日期精确到月或能被解析为日期
- 结果输出到 output/<TICKER>_涨跌幅.xlsx
"""

import os
from typing import Optional, Tuple, List
import pandas as pd
import numpy as np

import yfinance as yf
import akshare as ak

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# ========== 配置区（请按需修改） ==========
TICKER: str = "^HSI"                 # 指数代码（CN: .SS/.SZ；HK/US: Yahoo 代码，如 ^GSPC）
COUNTRY: str = "HK"                   # 'CN'（akshare）、'HK'/'US'（yfinance）
CYCLE_FILE: str = "data/HSI.xlsx"   # 市场周期表格路径（.xlsx/.xls/.csv）
CYCLE_SHEET: Optional[str] = None     # Excel 工作表名；None 则默认第一个
# 支持的列名集合（按顺序匹配）
START_COL_CANDIDATES = ["开始时间", "开始", "起始", "Start", "StartDate", "Start_Time"]
END_COL_CANDIDATES   = ["结束时间", "结束", "终止", "End", "EndDate", "End_Time"]
LABEL_COL_CANDIDATES = ["周期类型", "类型", "阶段", "Label", "Type"]

AUTO_ADJUST_YF: bool = True        # yfinance 是否复权
OUTPUT_DIR: str = "output"         # 输出目录
RAW_SUBDIR: str = "raw_data"       # 原始数据子目录（位于 OUTPUT_DIR 下）
# =======================================


# ---------- 工具函数 ----------
def _ensure_dir(path: str):
    if path and not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


def _read_cycle_table(path: str, sheet: Optional[str]) -> pd.DataFrame:
    """
    读取“市场周期表格”并把开始/结束/类型列统一命名为：开始时间 / 结束时间 / 周期类型
    所有日期统一归约到该月1日；丢弃开始时间无效的行。
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, sheet_name=0 if sheet is None else sheet)
    elif ext in [".csv"]:
        df = pd.read_csv(path)
    else:
        raise ValueError("仅支持 Excel (.xlsx/.xls) 或 CSV 文件作为周期表。")

    def pick_col(cands: List[str]) -> Optional[str]:
        for c in cands:
            if c in df.columns:
                return c
        lower_map = {str(col).strip().lower(): col for col in df.columns}
        for c in cands:
            key = str(c).strip().lower()
            if key in lower_map:
                return lower_map[key]
        return None

    start_col = pick_col(START_COL_CANDIDATES)
    end_col   = pick_col(END_COL_CANDIDATES)
    label_col = pick_col(LABEL_COL_CANDIDATES)
    if not start_col or not end_col:
        raise ValueError(
            f"未在表格中找到开始/结束列。支持的开始列名：{START_COL_CANDIDATES}；结束列名：{END_COL_CANDIDATES}"
        )

    out = df.copy()
    out.rename(columns={start_col: "开始时间", end_col: "结束时间"}, inplace=True)
    if label_col:
        out.rename(columns={label_col: "周期类型"}, inplace=True)

    # 统一到“该月1日”，支持字符串/数字/日期混排
    def to_month_start(x):
        ts = pd.to_datetime(x, errors="coerce")
        if pd.isna(ts):
            return pd.NaT
        return ts.to_period("M").to_timestamp(how="start")

    out["开始时间"] = out["开始时间"].apply(to_month_start)
    out["结束时间"] = out["结束时间"].apply(to_month_start)

    # 丢弃开始时间无效的行，结束时间可为空（开放上边界）
    before = len(out)
    out = out.dropna(subset=["开始时间"]).reset_index(drop=True)
    if len(out) < before:
        print(f"[警告] 周期表中有 {before - len(out)} 行开始时间无效，已跳过。")

    return out


def _normalize_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    mapping = {
        'date':'Date', '日期':'Date', '时间':'Date',
        'open':'Open', '开盘':'Open',
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


def _ak_symbol_from_yahoo_like(ticker: str) -> str:
    """
    将常见的 Yahoo 形态代码转为 akshare 指数日线接口的 symbol：
    - 000300.SS -> sh000300
    - 399001.SZ -> sz399001
    - 若已是 sh000300/sz399001 也直接返回
    """
    t = ticker.strip().lower()
    if t.startswith(("sh", "sz")) and len(t) >= 8:
        return t
    if "." in t:
        code, exch = t.split(".", 1)
        if exch == "ss":
            return f"sh{code}"
        if exch == "sz":
            return f"sz{code}"
    if t.startswith(("6", "0")):
        return f"sh{t}"
    if t.startswith(("1", "3")):
        return f"sz{t}"
    return ticker


# ---------- 关键修复：兼容 yfinance 单层/多层列的提取 ----------
def _extract_ohlcv(df: pd.DataFrame, field: str) -> pd.Series:
    """
    从 yfinance 下载结果中抽取指定字段（Open/High/Low/Close/Adj Close/Volume），
    兼容：
      1) 单层列：['Open','High',...]
      2) 多层列：MultiIndex，形如 ('Open','^GSPC') 或 ('^GSPC','Open')
    返回 1D Series（索引为 DatetimeIndex）。
    """
    # 1) 单层列直接取
    if field in df.columns:
        s = df[field]
    # 2) 多层列：尝试两种层级顺序
    elif isinstance(df.columns, pd.MultiIndex):
        # 形式A：('Open', ticker)
        candA = [c for c in df.columns if isinstance(c, tuple) and str(c[0]).lower() == field.lower()]
        # 形式B：(ticker, 'Open')
        candB = [c for c in df.columns if isinstance(c, tuple) and str(c[-1]).lower() == field.lower()]
        if candA:
            s = df[candA[0]]
        elif candB:
            s = df[candB[0]]
        else:
            raise RuntimeError(f"未在 yfinance 返回中找到字段 {field}，现有列：{list(df.columns)}")
    else:
        raise RuntimeError(f"未在 yfinance 返回中找到字段 {field}，现有列：{list(df.columns)}")

    # 如果仍是 DataFrame（例如只有一个次级列），压成 Series
    if isinstance(s, pd.DataFrame):
        if s.shape[1] == 1:
            s = s.iloc[:, 0]
        else:
            # 多列时优先选择首列
            s = s.iloc[:, 0]
    s = pd.to_numeric(s, errors="coerce")
    s.name = field
    return s


def _fetch_daily_cn(ticker: str, start: str, end: Optional[str] = None) -> pd.DataFrame:
    sym = _ak_symbol_from_yahoo_like(ticker)
    df = ak.stock_zh_index_daily(symbol=sym)
    df = _normalize_price_columns(df)
    if df.empty:
        raise RuntimeError("akshare 返回数据为空")
    if start:
        df = df.loc[df.index >= pd.to_datetime(start)]
    if end:
        df = df.loc[df.index <= pd.to_datetime(end)]
    return df


def _fetch_daily_yf(ticker: str, start: str, end: Optional[str] = None, auto_adjust: bool = True) -> pd.DataFrame:
    df = yf.download(ticker, start=start, end=end, auto_adjust=auto_adjust, progress=False)
    if df is None or df.empty:
        raise RuntimeError(f"yfinance 未获取到 {ticker} 的数据")

    # 统一抽取字段（无论单层还是多层列）
    out = pd.DataFrame({
        "Open":   _extract_ohlcv(df, "Open"),
        "High":   _extract_ohlcv(df, "High") if "High" in [c if isinstance(c, str) else c[0] for c in df.columns] or isinstance(df.columns, pd.MultiIndex) else np.nan,
        "Low":    _extract_ohlcv(df, "Low")  if "Low"  in [c if isinstance(c, str) else c[0] for c in df.columns] or isinstance(df.columns, pd.MultiIndex) else np.nan,
        "Close":  _extract_ohlcv(df, "Close") if "Close" in [c if isinstance(c, str) else c[0] for c in df.columns] or isinstance(df.columns, pd.MultiIndex) else np.nan,
        "Volume": _extract_ohlcv(df, "Volume") if "Volume" in [c if isinstance(c, str) else c[0] for c in df.columns] or isinstance(df.columns, pd.MultiIndex) else np.nan,
    })
    out = _normalize_price_columns(out.reset_index())
    return out


def _fetch_daily_all(ticker: str, country: str, start: str, end: Optional[str] = None) -> pd.DataFrame:
    c = (country or "").upper()
    if c == "CN":
        return _fetch_daily_cn(ticker, start, end)
    elif c in ("HK", "US"):
        return _fetch_daily_yf(ticker, start, end, AUTO_ADJUST_YF)
    else:
        raise ValueError("COUNTRY 仅支持 'CN' / 'HK' / 'US'")


def _month_first_open(prices: pd.DataFrame, year, month) -> float:
    """
    返回该'月'中首个交易日的开盘价；若当月无交易日，向后寻找下一个有交易日。
    year/month 允许传入 float/str/np 类型，内部会强制转为 int。
    """
    def _as_int(v, name):
        try:
            iv = int(pd.to_numeric(v))
        except Exception:
            raise TypeError(f"{name} 无法转换为整数: {v!r}")
        return iv

    y = _as_int(year, "year")
    m = _as_int(month, "month")
    if not (1 <= m <= 12):
        raise ValueError(f"month 超出范围(1-12): {m}")

    month_start = pd.Timestamp(year=y, month=m, day=1)
    month_end = month_start + pd.offsets.MonthEnd(1)
    df = prices.loc[(prices.index >= month_start) & (prices.index <= month_end)]
    if df.empty:
        df2 = prices.loc[prices.index >= month_start]
        if df2.empty:
            return np.nan
        return float(df2.iloc[0]["Open"])
    return float(df.iloc[0]["Open"])


def _collect_needed_date_range(cycle_df: pd.DataFrame) -> Tuple[str, str]:
    """为减少请求量，仅拉所需日期范围。"""
    min_start = cycle_df["开始时间"].min()
    max_end = cycle_df["结束时间"].max()
    if pd.isna(min_start):
        raise ValueError("周期表的开始时间存在空值或无效日期")
    start = pd.Timestamp(min_start).strftime("%Y-%m-%d")
    end_ts = pd.Timestamp(max_end) + pd.offsets.MonthEnd(1) + pd.offsets.Day(1)
    end = end_ts.strftime("%Y-%m-%d")
    return start, end


def _ym_str(ts: pd.Timestamp) -> str:
    """将时间戳格式化为 yyyy/m（不补零的月份）。"""
    return f"{ts.year}/{ts.month}"


def compute_cycle_returns(ticker: str, country: str, cycles: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回：
    - out_df：每个周期的涨跌幅汇总
    - px：原始日线（已规整）
    """
    start, end = _collect_needed_date_range(cycles)
    px = _fetch_daily_all(ticker, country, start=start, end=end)
    if "Open" not in px.columns:
        raise RuntimeError("价格数据缺少 Open 列")

    results = []
    for ridx, row in cycles.iterrows():
        s = pd.to_datetime(row["开始时间"], errors="coerce")
        e = pd.to_datetime(row["结束时间"], errors="coerce") if not pd.isna(row["结束时间"]) else pd.NaT
        label = row.get("周期类型", "")

        if pd.isna(s):
            print(f"[警告] 第 {ridx} 行开始时间无效，已跳过。原值={row['开始时间']!r}")
            continue

        try:
            s_open = _month_first_open(px, s.year, s.month)
            if not pd.isna(e):
                e_open = _month_first_open(px, e.year, e.month)
            else:
                last_idx = px.index.max()
                e_open = _month_first_open(px, last_idx.year, last_idx.month)
        except Exception as ex:
            print(f"[警告] 第 {ridx} 行计算失败：{ex}")
            s_open, e_open = np.nan, np.nan

        pct = np.nan
        if pd.notna(s_open) and pd.notna(e_open) and s_open != 0:
            pct = e_open / s_open - 1.0

        results.append({
            "周期开始": _ym_str(s),
            "周期结束": (_ym_str(e) if pd.notna(e) else ""),
            "周期类型": label if label is not None else "",
            "起始开盘价": s_open,
            "结束开盘价": e_open,
            "涨跌幅": pct
        })

    out_df = pd.DataFrame(results)
    return out_df, px


def export_results(cycle_df: pd.DataFrame, raw_px: pd.DataFrame, ticker: str, outdir: str, raw_subdir: str):
    _ensure_dir(outdir)
    safe = ticker.replace("^", "").replace("/", "_")
    path_summary = os.path.join(outdir, f"{safe}_涨跌幅.xlsx")

    import xlsxwriter
    with pd.ExcelWriter(path_summary, engine="xlsxwriter") as writer:
        cycle_df.to_excel(writer, sheet_name="cycle_returns", index=False)

        wb = writer.book
        ws = writer.sheets["cycle_returns"]

        # 样式
        header = wb.add_format({"bold": True, "align": "center"})
        num2 = wb.add_format({"num_format": "0.00"})
        pct2 = wb.add_format({"num_format": "0.00%"})

        # 条件格式（单元格底色）
        pct_green_fill = wb.add_format({"num_format": "0.00%", "bg_color": "#C6EFCE", "font_color": "#006100"})
        pct_red_fill   = wb.add_format({"num_format": "0.00%", "bg_color": "#FFC7CE", "font_color": "#9C0006"})

        # 表头样式
        for c, name in enumerate(cycle_df.columns):
            ws.write(0, c, name, header)

        # 列宽&基础格式
        col_map = {
            "周期开始": (0, 12, None),
            "周期结束": (1, 12, None),
            "周期类型": (2, 12, None),
            "起始开盘价": (3, 14, num2),
            "结束开盘价": (4, 14, num2),
            "涨跌幅":   (5, 12, pct2),  # 先设百分比，条件格式覆盖填充色
        }
        for name, (ci, width, fmt) in col_map.items():
            if name in cycle_df.columns:
                ws.set_column(ci, ci, width, fmt)

        # —— 条件格式：涨跌幅单元格填充色（>0 绿色填充，<0 红色填充）——
        n_rows = len(cycle_df)
        if n_rows > 0:
            ws.conditional_format(1, 5, n_rows, 5, {
                "type": "cell", "criteria": ">", "value": 0, "format": pct_green_fill
            })
            ws.conditional_format(1, 5, n_rows, 5, {
                "type": "cell", "criteria": "<", "value": 0, "format": pct_red_fill
            })

    print(f"已导出周期汇总：{path_summary}")

    # 原始数据（index 在 Date 上，导出时恢复成列）
    _ensure_dir(os.path.join(outdir, raw_subdir))
    path_raw = os.path.join(outdir, raw_subdir, f"{safe}_涨跌幅历史数据.xlsx")
    raw_to_save = raw_px.copy().reset_index().rename(columns={"index": "Date"})
    with pd.ExcelWriter(path_raw, engine="xlsxwriter") as writer:
        raw_to_save.to_excel(writer, sheet_name="daily", index=False)
    print(f"已保存原始日线：{path_raw}")


def main():
    # 读取周期表
    cycles = _read_cycle_table(CYCLE_FILE, CYCLE_SHEET)
    # 计算（返回周期结果 + 原始日线）
    out_df, px = compute_cycle_returns(TICKER, COUNTRY, cycles)
    # 导出结果 + 保存原始数据
    export_results(out_df, px, TICKER, OUTPUT_DIR, RAW_SUBDIR)


if __name__ == "__main__":
    main()
