# -*- coding: utf-8 -*-
"""
- 获取“ETF”的历史价格与分红数据（A股ETF走 akshare；非A股ETF走 yfinance）
- 计算并导出逐年指标：
  1) 每年开始价：当年“第一个交易日”的 Open
  2) 每年涨跌幅：基于相邻年份“年初开盘价”计算
  3) 每年分红：
     * A股ETF（akshare）：按“累计分红”在当年年末与上年年末之差
     * 港股/海外ETF（yfinance）：当年分红现金求和
  4) 每年分红率：当年分红 / 当年“日均收盘价”（当年收盘价的算术平均）
"""

import datetime as dt
import os
from typing import Dict, Optional, Tuple, List
import numpy as np
import pandas as pd
import yfinance as yf
import akshare as ak
from pathlib import Path

# ========== 全局配置 ==========
# 常见指数（Yahoo Finance 代码）：
# 标普500：^GSPC    纳指综合：^IXIC
# 沪深300：000300.SS  恒生指数：^HSI
TICKER       = "2833.HK"   # 此脚本用于“ETF”；若输入到A股ETF如 510300.SS/159915.SZ，则用 akshare；其余如 2800.HK/IVV 则用 yfinance
START_YEAR   = 1985          # 开始年份（如 2004）
AUTO_ADJUST  = False         # yfinance：是否使用调整后的 OHLC；此处取 False，使用未复权，与 akshare 接口含义对齐
OUTPUT_FILE  = None          # 输出文件名；None 则自动生成 "output/<TICKER>_annual.xlsx"

# 批量任务列表（每项为 (ticker, start_year)）。非空时将遍历执行；为空则退回单标的模式
TICKER_JOBS: List[Tuple[str, int]] = [
]

# 代理
os.environ['http_proxy'] = os.environ.get('http_proxy', 'http://127.0.0.1:7890')
os.environ['https_proxy'] = os.environ.get('https_proxy', 'http://127.0.0.1:7890')

# 路径
DIR_RAW = Path("output/raw_data"); DIR_RAW.mkdir(parents=True, exist_ok=True)
DIR_OUT = Path("output");          DIR_OUT.mkdir(parents=True, exist_ok=True)

# ========== 公共工具 ==========
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

def _to_ex_and_code6(ticker: str) -> Tuple[str, str]:
    """根据后缀判断交易所前缀；A股：.SS -> sh, .SZ -> sz；返回 (ex, code6)"""
    code = ticker.split('.')[0]
    suffix = ticker.split('.')[-1] if '.' in ticker else ''
    if suffix.upper() == 'SS':
        return 'sh', code
    if suffix.upper() == 'SZ':
        return 'sz', code
    return '', code  # 非A股返回空前缀

def _is_a_share_etf(ticker: str) -> bool:
    """简单判定：.SS 或 .SZ 结尾视作A股ETF（指数不在本脚本关注范围内）"""
    if '.' not in ticker: return False
    suf = ticker.split('.')[-1].upper()
    return suf in {'SS','SZ'}

# === 改动点（5）：把 raw_data 三份文件合并为单一工作簿的三个 sheet ===
def _raw_book_path(ticker: str) -> Path:
    """统一的 raw 工作簿路径（合并 <ticker>_price / _dividend / _process）。"""
    return DIR_RAW / f"{ticker}_raw.xlsx"

def _write_df_to_excel(path: Path, sheet_name: str, df: pd.DataFrame):
    """
    直接用 pandas.ExcelWriter 打开/追加写入，并用 if_sheet_exists='replace' 覆盖同名 sheet。
    这样避免 Windows 上 NamedTemporaryFile 的句柄占用问题。
    需要 pandas>=1.4 且 openpyxl 已安装。
    """
    path = Path(path)
    mode = "a" if path.exists() else "w"
    # 注意：if_sheet_exists 参数仅在 mode='a' 时有效
    if mode == "a":
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=True)
    else:
        with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=True)
    return path


def _save_raw_price(ticker: str, df: pd.DataFrame):
    """保存原始价格到同一工作簿的 Price sheet（output/raw_data/<ticker>_raw.xlsx）。"""
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
        out.index = out.index.tz_convert('UTC').tz_localize(None)
    out.index.name = "Date"
    path = _raw_book_path(ticker)
    _write_df_to_excel(path, "Price", out.reset_index())
    return path

def _save_raw_dividend_series(ticker: str, df_div: Dict[str, pd.Series]):
    """保存分红序列到同一工作簿的 Dividend sheet，多列（CashDiv/CumDiv）。"""
    cols = {}
    for name, ser in df_div.items():
        s = pd.Series(dtype=float) if ser is None else ser
        if isinstance(s, pd.Series):
            s = s.sort_index().to_frame(name=name)
        cols[name] = s
    if cols:
        # 对所有列按索引对齐再写
        base = None
        for v in cols.values():
            base = v if base is None else base.join(v, how="outer")
        base = base.sort_index()
    else:
        base = pd.DataFrame()
    base.index.name = "Date"
    path = _raw_book_path(ticker)
    _write_df_to_excel(path, "Dividend", base.reset_index())
    return path

def _save_process_table(ticker: str, df: pd.DataFrame, sheet: str = "yearly_inputs"):
    """保存年度过程数据到同一工作簿的 Process sheet。"""
    path = _raw_book_path(ticker)
    _write_df_to_excel(path, sheet, df)
    return path

# ========== A股ETF（akshare） ==========
START_DATE_FALLBACK_AK = "20000101"
END_DATE_TODAY_AK = dt.datetime.now().strftime("%Y%m%d")

def fetch_price_ak_etf(ticker: str, max_retry: int = 2, sleep_sec: float = 1.0) -> pd.DataFrame:
    """A股ETF：优先新浪 fund_etf_hist_sina，失败回退东财 fund_etf_hist_em；返回标准化OHLCV日线。"""
    ex, code6 = _to_ex_and_code6(ticker)
    last_err = None
    for _ in range(max_retry):
        try:
            df = ak.fund_etf_hist_sina(symbol=f"{ex}{code6}")
            df = _normalize_price_columns(df)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
        finally:
            import time; time.sleep(sleep_sec)
    for _ in range(max_retry):
        try:
            df = ak.fund_etf_hist_em(symbol=code6, period="daily",
                                     start_date=START_DATE_FALLBACK_AK,
                                     end_date=END_DATE_TODAY_AK, adjust="")
            df = _normalize_price_columns(df)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
        finally:
            import time; time.sleep(sleep_sec)
    raise RuntimeError(f"{ticker} 价格下载失败（akshare）：{last_err}")

def fetch_div_ak_etf_cum(ticker: str, max_retry: int = 2, sleep_sec: float = 1.0) -> pd.Series:
    """A股ETF：新浪累计分红（元/份），返回升序Series，索引为日期。"""
    ex, code6 = _to_ex_and_code6(ticker)
    last_err = None
    for _ in range(max_retry):
        try:
            df = ak.fund_etf_dividend_sina(symbol=f"{ex}{code6}")
            if df is None or len(df) == 0:
                return pd.Series(dtype=float)
            df = df.rename(columns={'日期':'Date','累计分红':'CumDiv'})
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df = df.dropna(subset=['Date']).sort_values('Date')
            ser = pd.Series(df['CumDiv'].astype(float).values, index=df['Date'])
            ser = ser[~ser.index.duplicated(keep='last')]
            ser.name = 'CumDiv'
            return ser
        except Exception as e:
            last_err = e
        finally:
            import time; time.sleep(sleep_sec)
    raise RuntimeError(f"{ticker} 分红下载失败（akshare）：{last_err}")

# ========== yfinance（港股/海外ETF） ==========
START_DATE_FALLBACK_YF = "2000-01-01"
END_DATE_TODAY_YF = dt.date.today().strftime("%Y-%m-%d")

def fetch_price_yf_etf(ticker: str, max_retry: int = 2, sleep_sec: float = 1.0, auto_adjust: bool = False) -> pd.DataFrame:
    """yfinance：history / download 获取未复权日线，去时区、升序、标准化为OHLCV。"""
    last_err = None
    for _ in range(max_retry):
        try:
            tk = yf.Ticker(ticker)
            hist = tk.history(start=START_DATE_FALLBACK_YF, end=END_DATE_TODAY_YF, interval="1d", auto_adjust=auto_adjust)
            if hist is None or hist.empty:
                hist = yf.download(ticker, start=START_DATE_FALLBACK_YF, end=END_DATE_TODAY_YF,
                                   interval="1d", auto_adjust=auto_adjust, progress=False)
            if hist is not None and not hist.empty:
                out = hist.copy()
                out.index = pd.to_datetime(out.index)
                if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
                    out.index = out.index.tz_convert('UTC').tz_localize(None)
                out = out[~out.index.duplicated(keep='last')].sort_index()
                out = out.reset_index().rename(columns={'index':'Date','Date':'Date'})
                out = _normalize_price_columns(out)
                return out
        except Exception as e:
            last_err = e
        finally:
            import time; time.sleep(sleep_sec)
    raise RuntimeError(f"{ticker} 价格下载失败（yfinance）：{last_err}")

def fetch_div_yf_cash_and_cum(ticker: str, max_retry: int = 2, sleep_sec: float = 1.0) -> Dict[str, pd.Series]:
    """yfinance：现金分红序列（每次派息金额）与其累计序列。返回 {'CashDiv':Series, 'CumDiv':Series}"""
    last_err = None
    for _ in range(max_retry):
        try:
            tk = yf.Ticker(ticker)
            div = tk.dividends  # index=date, values=amount
            if div is None or len(div) == 0:
                return {"CashDiv": pd.Series(dtype=float), "CumDiv": pd.Series(dtype=float)}
            div = div.copy()
            div.index = pd.to_datetime(div.index)
            if isinstance(div.index, pd.DatetimeIndex) and div.index.tz is not None:
                div.index = div.index.tz_convert('UTC').tz_localize(None)
            div.index = div.index.normalize()
            div = div.groupby(div.index).sum().sort_index()
            div.name = "CashDiv"
            cum = div.cumsum()
            cum.name = "CumDiv"
            return {"CashDiv": div, "CumDiv": cum}
        except Exception as e:
            last_err = e
        finally:
            import time; time.sleep(sleep_sec)
    raise RuntimeError(f"{ticker} 分红下载失败（yfinance）：{last_err}")

# ========== 年度计算 ==========
def first_open_each_year(df_price: pd.DataFrame, start_year: int) -> pd.Series:
    """取每年首个交易日的 Open；返回 Series(index=Year, value=start_open)。
       若首个交易日 Open=0，则在该年份内继续向后寻找首个 Open!=0 的交易日作为开始价。"""
    if df_price is None or df_price.empty or "Open" not in df_price.columns:
        return pd.Series(dtype=float)
    df = df_price.copy()
    df = df.loc[df.index >= pd.Timestamp(f"{start_year}-01-01")]
    df["Open"] = pd.to_numeric(df["Open"], errors="coerce")
    df["Year"] = df.index.year

    starts: Dict[int, float] = {}
    for y, g in df.groupby("Year"):
        g = g.sort_index()
        # 找到当年第一个非零开盘价；若全为零/缺失，则返回 NaN
        nz = g.loc[(~g["Open"].isna()) & (g["Open"].astype(float) != 0.0)]
        starts[y] = float(nz["Open"].iloc[0]) if len(nz) else np.nan

    out = pd.Series(starts)
    out.name = "start_open"
    return out

def avg_close_each_year(df_price: pd.DataFrame, start_year: int) -> pd.Series:
    """当年日均收盘价（算术平均）。"""
    if df_price is None or df_price.empty or "Close" not in df_price.columns:
        return pd.Series(dtype=float)
    df = df_price.copy()
    df = df.loc[df.index >= pd.Timestamp(f"{start_year}-01-01")]
    df["Year"] = df.index.year
    out = df.groupby("Year")["Close"].mean().astype(float)
    out.name = "avg_close"
    return out

def annual_dividend_from_ak_cum(cum_div: pd.Series, start_year: int) -> pd.Series:
    """A股ETF累计分红 -> 年度分红（年末 - 上年年末）。"""
    if cum_div is None or cum_div.empty:
        return pd.Series(dtype=float)
    # 取每年“最后一个可用累计值”（asof 到每年12-31）
    years = range(start_year, dt.date.today().year + 1)
    year_end_val = {}
    s = cum_div.sort_index()
    for y in years:
        end = pd.Timestamp(f"{y}-12-31")
        s_end = s.loc[s.index <= end]
        year_end_val[y] = float(s_end.iloc[-1]) if len(s_end) else np.nan
    df = pd.Series(year_end_val).sort_index()
    out = df.diff()  # 本年-上年
    out.name = "dividend"
    return out

def annual_dividend_from_yf_cash(cash_div: pd.Series, start_year: int) -> pd.Series:
    """yfinance 现金分红 -> 年度分红（当年派息金额求和）。"""
    if cash_div is None or len(cash_div) == 0:
        return pd.Series(dtype=float)
    s = cash_div.copy().sort_index()
    s = s.loc[s.index >= pd.Timestamp(f"{start_year}-01-01")]
    s_year = s.groupby(s.index.year).sum()
    s_year.name = "dividend"
    return s_year

def build_annual_table(df_price: pd.DataFrame,
                       start_year: int,
                       div_series: Optional[pd.Series],
                       is_ak: bool) -> pd.DataFrame:
    """汇总出 年初价、涨跌幅、年度分红、分红率、日均收盘价。"""
    start_open = first_open_each_year(df_price, start_year)
    avg_close  = avg_close_each_year(df_price, start_year)

    # 每年涨跌幅：下一年年初 / 当年年初 - 1
    ret = (start_open.shift(-1) / start_open) - 1.0
    ret.name = "annual_return"

    # 年度分红
    if is_ak:
        # 累计分红 -> 年度分红
        dividend = annual_dividend_from_ak_cum(div_series, start_year) if div_series is not None else pd.Series(dtype=float)
    else:
        dividend = annual_dividend_from_yf_cash(div_series, start_year) if div_series is not None else pd.Series(dtype=float)

    # === 改动点（1&2）：支持分红缺失/为零；“总收益率” = （当年股价涨跌 + 分红） / 当年开始价 ===
    start_open_next = start_open.shift(-1)
    stock_change = start_open_next - start_open                          # 金额（下一年年初 - 当年年初）
    dividend_filled = dividend.copy() if isinstance(dividend, pd.Series) else pd.Series(dtype=float)
    dividend_filled = dividend_filled.reindex(start_open.index).fillna(0.0)  # 分红缺失按 0 处理
    # 分母为“当年开始价”；若开始价<=0 或缺失，则结果记为 NaN（避免除零/无意义）
    total_num = stock_change.add(dividend_filled, fill_value=0.0)
    denom = start_open
    total_yield = total_num / denom
    total_yield = total_yield.mask((denom.isna()) | (denom.astype(float) <= 0))
    total_yield.name = "total_yield"

    # 组装
    years = sorted(set(start_open.index) | set(avg_close.index) | set(dividend.index))
    df = pd.DataFrame(index=years)
    df.index.name = "Year"
    df["start_open"]      = start_open.reindex(years)
    df["annual_return"]   = ret.reindex(years)            # 比例数值
    df["dividend"]        = dividend.reindex(years)       # 金额
    df["avg_close"]       = avg_close.reindex(years)      # 金额
    df["total_yield"]     = total_yield.reindex(years)    # 比例
    # （保留 start_open_next 仅用于过程检查，不在最终输出表中展示）
    df["start_open_next"] = start_open_next.reindex(years)
    df = df.reset_index()

    # 保留 START_YEAR 之后的年份（且至少有 start_open）
    df = df[df["Year"] >= start_year]
    return df

# ========== 导出 ==========
def export_annual_to_excel(df: pd.DataFrame, ticker: str, outfile: Optional[str] = None):
    if outfile is None:
        symbol_safe = ticker.replace("^", "").replace("/", "_")
        outfile = f"output/{symbol_safe}_annual.xlsx"
    # 写出
    import xlsxwriter
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        # === 改动点（1,2,4）：删除“下一年开始价”，新增“总收益率”，并按指定顺序输出列 ===
        main = pd.DataFrame({
            "年份": df["Year"],
            "开始价": df["start_open"],
            "日均收盘价": df["avg_close"],
            "涨跌幅": df["annual_return"],      # 比例
            "分红": df["dividend"],            # 金额
            "分红率": df["dividend"] / df["avg_close"],  # 比例（与 df['dividend_yield'] 等价）
            "总收益率": df["total_yield"],      # 比例
        })
        main.to_excel(writer, sheet_name="annual_data", index=False)

        # 过程表（保留，包括 start_open_next 便于核对）
        df.to_excel(writer, sheet_name="calc_tmp", index=False)

        wb = writer.book
        ws = writer.sheets["annual_data"]
        header_fmt = wb.add_format({"bold": True, "align": "center"})
        numfmt_2   = wb.add_format({"num_format": "0.00"})
        pctfmt_2   = wb.add_format({"num_format": "0.00%"})

        # 表头样式
        for col_idx, col_name in enumerate(main.columns):
            ws.write(0, col_idx, col_name, header_fmt)

        # 列宽 & 数字格式（与顺序对应）
        ws.set_column(0, 0, 10)            # 年份
        ws.set_column(1, 1, 14, numfmt_2)  # 开始价
        ws.set_column(2, 2, 14, numfmt_2)  # 日均收盘价
        ws.set_column(3, 3, 12, pctfmt_2)  # 涨跌幅
        ws.set_column(4, 4, 12, numfmt_2)  # 分红
        ws.set_column(5, 5, 12, pctfmt_2)  # 分红率
        ws.set_column(6, 6, 12, pctfmt_2)  # 总收益率

        # === 改动点（3）：为 涨跌幅、分红、分红率、总收益率 增加红绿上色（正绿、负红、零不着色）
        # 列索引：0~6
        green_fmt = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})
        red_fmt   = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        n_rows = len(main) + 1  # 包含表头的总行数

        # 涨跌幅（第 3 列，索引 3）
        ws.conditional_format(1, 3, n_rows, 3, {"type": "cell", "criteria": ">", "value": 0, "format": green_fmt})
        ws.conditional_format(1, 3, n_rows, 3, {"type": "cell", "criteria": "<", "value": 0, "format": red_fmt})

        # 分红（第 4 列，索引 4）
        ws.conditional_format(1, 4, n_rows, 4, {"type": "cell", "criteria": ">", "value": 0, "format": green_fmt})
        ws.conditional_format(1, 4, n_rows, 4, {"type": "cell", "criteria": "<", "value": 0, "format": red_fmt})

        # 分红率（第 5 列，索引 5）
        ws.conditional_format(1, 5, n_rows, 5, {"type": "cell", "criteria": ">", "value": 0, "format": green_fmt})
        ws.conditional_format(1, 5, n_rows, 5, {"type": "cell", "criteria": "<", "value": 0, "format": red_fmt})

        # 总收益率（第 6 列，索引 6）
        ws.conditional_format(1, 6, n_rows, 6, {"type": "cell", "criteria": ">", "value": 0, "format": green_fmt})
        ws.conditional_format(1, 6, n_rows, 6, {"type": "cell", "criteria": "<", "value": 0, "format": red_fmt})

# ========== 主流程 ==========
def _run_single_job(ticker: str, start_year: int, auto_adjust: bool, outfile: Optional[str]):
    """单标的执行一次完整流程（价格/分红/年度表/导出/落盘过程）。"""
    # 价格 & 分红
    if _is_a_share_etf(ticker):
        price = fetch_price_ak_etf(ticker)
        # A股ETF：取“累计分红”序列
        cum_div = fetch_div_ak_etf_cum(ticker)
        div_series_for_calc = cum_div  # 年度分红由累计分红差分得到
        is_ak = True

        # 原始/过程数据落盘（=== 改动点（5）：写入统一工作簿的不同 sheet ===）
        _save_raw_price(ticker, price)
        _save_raw_dividend_series(ticker, {"CumDiv": cum_div})
    else:
        price = fetch_price_yf_etf(ticker, auto_adjust=auto_adjust)
        # yfinance：获得现金分红与累计分红
        divs = fetch_div_yf_cash_and_cum(ticker)
        cash_div, cum_div = divs.get("CashDiv", pd.Series(dtype=float)), divs.get("CumDiv", pd.Series(dtype=float))
        div_series_for_calc = cash_div  # 年度分红用现金分红求和
        is_ak = False

        # 原始/过程数据落盘（=== 改动点（5）：写入统一工作簿的不同 sheet ===）
        _save_raw_price(ticker, price)
        _save_raw_dividend_series(ticker, {"CashDiv": cash_div, "CumDiv": cum_div})

    # 年度表
    annual_df = build_annual_table(price, start_year, div_series_for_calc, is_ak)

    # 过程表（年初开盘 & 日均收盘 & 分红输入）便于核对
    inputs = annual_df[["Year","start_open","avg_close","dividend"]].rename(columns={
        "Year":"年份","start_open":"开始价(年初Open)","avg_close":"当年日均收盘价","dividend":"当年分红"
    })
    _save_process_table(ticker, inputs, sheet="Process")

    # 导出汇总（每个标的各自输出一个 Excel）
    export_annual_to_excel(annual_df, ticker, outfile)

    # 控制台简单回显
    print(f"完成：{ticker}（起始年份 {start_year}）逐年开始价/涨跌幅/分红/分红率/总收益率 已输出到 Excel。")
    if outfile is None:
        print(f"默认路径：output/{ticker.replace('^','').replace('/','_')}_annual.xlsx")
        print(f"原始与过程数据：{_raw_book_path(ticker)} -> [Price, Dividend, Process] 三个sheet")

def main():
    # 若定义了批量任务，则逐一执行；否则执行单标的旧逻辑
    if TICKER_JOBS:
        for tk, sy in TICKER_JOBS:
            try:
                _run_single_job(ticker=str(tk), start_year=int(sy), auto_adjust=bool(AUTO_ADJUST), outfile=None)
            except Exception as e:
                print(f"[WARN] {tk} 起始年份 {sy} 处理失败：{e}")
    else:
        ticker = str(TICKER)
        start_year = int(START_YEAR)
        auto_adjust = bool(AUTO_ADJUST)
        outfile = OUTPUT_FILE
        _run_single_job(ticker=ticker, start_year=start_year, auto_adjust=auto_adjust, outfile=outfile)

if __name__ == "__main__":
    main()
