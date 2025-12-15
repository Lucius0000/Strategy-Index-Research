# -*- coding: utf-8 -*-
"""
沪深300指数 & 其ETF（510300/510310/510330）净值曲线
- 指数数据：ak.stock_zh_index_daily（收盘点位）
- ETF数据：ak.fund_open_fund_info_em(indicator="累计净值走势")
- 归一方式：公共覆盖期第一天 = 1（在“日频”归一；若之后做周频降采样，首个点是基准日后的首个周五）
- 坐标：线性坐标
- 输出图：output/CSI300_ETF_NAV_common_base.png
- 输出表：output/raw_data/CSI300_ETF_NAV_for_plot.xlsx（含原始日频、归一后日频、最终作图频率数据、元信息）

【本脚本为 yfinance 版本】
- 改用 yfinance 获取美股/港股的指数与ETF数据；
- 指数使用 Close；ETF使用 Adj Close 近似累计净值（含分红再投资）；
- 仍按公共基准日=1归一、同样的作图与表格输出；
- 额外：按你的示例，将每个标的的“价格CSV + 分红CSV”保存到 output/raw_data。
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import os
import re
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import yfinance as yf

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# ---------------- 配置区 ----------------
# 说明：此处仍保留原注释，不影响功能；标的改成美股/港股例子
INDEX_TICKER: str = '^GSPC'                         # 指数：标普500 (^GSPC)；也可换成 ^HSI 等
ETF_TICKERS:  List[str] = ['SPY','VOO']   # ETF：美股/港股示例
START_DATE_STR = "1980-01-01"                       # 起始日期（会先裁剪到此日期）
END_DATE_STR   = datetime.now().strftime("%Y-%m-%d")

DIR_OUT = Path("output"); DIR_OUT.mkdir(parents=True, exist_ok=True)
DIR_RAW = DIR_OUT / "raw_data"; DIR_RAW.mkdir(parents=True, exist_ok=True)

# 输出图与表的文件名沿用原命名
FIG_PATH = DIR_OUT / "SP500_ETF_NAV_common_base.png"
TABLE_PATH = DIR_RAW / "SP500_ETF_NAV_for_plot.xlsx"

# 画图风格（保持不变：线性坐标；图例只显示名称）
USE_WEEKLY = True               # 是否使用周频降采样；日频= False
LINE_STYLES = {
    # 图例直接写名称
    "标普500": dict(linewidth=1.6, alpha=0.95, linestyle="-"),
    "SPY":   dict(linewidth=1.0, alpha=0.92, linestyle="-"),
    "VOO":   dict(linewidth=1.0, alpha=0.92, linestyle="--"),
}

# 可读名称映射（用于图例与CSV文件名前缀）
DISPLAY_NAME = {
    '^GSPC': '标普500',
    '^HSI':  '恒生指数',
    'SPY':   'SPY',
    'VOO':   'VOO',
}

# 指数价格：False 用 Close ；True=用 Adj Close
USE_INDEX_ADJ_CLOSE = False

# ---------- 中文字体 ----------
def setup_chinese_font():
    matplotlib.rcParams['axes.unicode_minus'] = False
    matplotlib.rcParams['font.sans-serif'] = [
        'SimHei','Microsoft YaHei','Arial Unicode MS','Noto Sans CJK SC'
    ]
setup_chinese_font()

# ---------- 工具 ----------
def safe_name(s: str) -> str:
    """用于生成文件名的安全前缀"""
    return re.sub(r'[^0-9A-Za-z_\-\.]+', '_', s)
    
def fetch_history_yf(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    使用 yfinance 获取日频历史：
    - 返回 DataFrame，索引为 DatetimeIndex，包含列：Open, High, Low, Close, Adj Close, Volume
    """
    tk = yf.Ticker(ticker)
    df = tk.history(start=start, end=end, interval="1d", auto_adjust=False)
    if df is None or df.empty:
        raise RuntimeError(f"历史价格获取为空：{ticker}")
    df = df.copy()
    # 索引统一为 tz-naive
    idx = pd.to_datetime(df.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    df.index = idx
    df = df[~df.index.duplicated(keep='last')].sort_index()
    return df

def fetch_dividends_yf(ticker: str) -> pd.Series:
    """yfinance 分红序列（若无则返回空Series）"""
    try:
        tk = yf.Ticker(ticker)
        div = tk.dividends
        if div is None:
            return pd.Series(dtype="float64")
        # 统一时间索引
        div.index = pd.to_datetime(div.index)
        div = div.sort_index()
        return div
    except Exception:
        return pd.Series(dtype="float64")

def fetch_index_close_series(index_ticker: str) -> pd.Series:
    """指数：使用 Close（收盘点位）"""
    df = fetch_history_yf(index_ticker, START_DATE_STR, END_DATE_STR)
    col = 'Adj Close' if (USE_INDEX_ADJ_CLOSE and 'Adj Close' in df.columns) else 'Close'
    s = df[col].astype(float)
    s = s[~s.index.duplicated(keep='last')].sort_index()
    return s

def fetch_etf_total_return_series_yf(etf_ticker: str) -> pd.Series:
    """
    ETF 累计净值近似：使用 yfinance 的 Adj Close（包含分红与拆分影响，近似总回报口径）。
    若 Adj Close 缺失则回退到 Close（此时不含分红影响）。
    """
    df = fetch_history_yf(etf_ticker, START_DATE_STR, END_DATE_STR)
    col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
    s = df[col].astype(float)
    s = s[~s.index.duplicated(keep='last')].sort_index()
    return s

def save_price_and_dividends_csv(label: str, ticker: str, price_df: pd.DataFrame, dividends: pd.Series):
    """
    将价格与分红分别保存到 CSV，格式参考你给的示例
    - 价格：完整df（含Open/High/Low/Close/Adj Close/Volume）
    - 分红：两列 Date, Dividend
    """
    prefix = f"{safe_name(label)}_{safe_name(ticker)}"
    price_path = DIR_RAW / f"{prefix}_price.csv"
    div_path   = DIR_RAW / f"{prefix}_dividends.csv"

    price_df.to_csv(price_path, index=True)

    div_df = dividends.reset_index()
    if not div_df.empty:
        div_df.columns = ["Date", "Dividend"]
    else:
        # 保持列结构，空表
        div_df = pd.DataFrame(columns=["Date","Dividend"])
    div_df.to_csv(div_path, index=False)

def clip_period(s: pd.Series, start_str: str, end_str: str) -> pd.Series:
    return s.loc[(s.index >= start_str) & (s.index <= end_str)]

def rebase_to_common_base(series_dict: Dict[str, pd.Series]) -> tuple[Dict[str, pd.Series], pd.Timestamp, Dict[str, float]]:
    """
    在 '日频' 上：
    - 找到所有序列都有数据的第一天（公共覆盖期起点 base_date）
    - 将各序列该日的值缩放为 1（即 s / s.loc[base_date]）
    返回：归一后的字典、基准日、各序列在基准日的原始值（便于审计）
    """
    # 各序列的“首个有效日”
    first_dates = []
    for s in series_dict.values():
        s1 = s.dropna()
        if not s1.empty:
            first_dates.append(s1.index.min())
    if not first_dates:
        raise RuntimeError("没有可用于归一的有效数据。")
    base_date = max(first_dates)  # 公共覆盖期的起点

    # 若当日无值（如周末/假期），取 >= base_date 的第一个有效日
    def first_on_or_after(s: pd.Series, d: pd.Timestamp) -> pd.Timestamp | None:
        idx = s.index[s.index >= d]
        return None if len(idx) == 0 else idx[0]

    out: Dict[str, pd.Series] = {}
    base_values: Dict[str, float] = {}
    used_dates: Dict[str, pd.Timestamp] = {}
    for name, s in series_dict.items():
        d_use = first_on_or_after(s, base_date)
        if d_use is None:
            raise RuntimeError(f"{name} 在公共基准日之后没有数据。")
        used_dates[name] = d_use

    # 可视化起点（仍≥base_date）
    vis_start = min(used_dates.values())

    for name, s in series_dict.items():
        d_use = used_dates[name]
        base_val = float(s.loc[d_use])
        base_values[name] = base_val
        s2 = s.loc[s.index >= vis_start].copy()
        out[name] = s2 / base_val

    return out, base_date, base_values

def strip_tz_index(df: pd.DataFrame) -> pd.DataFrame:
    """把 DataFrame 的索引与所有 datetime 列剥离时区（Excel 不支持 tz-aware）。"""
    out = df.copy()
    # 处理索引
    if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
        out.index = out.index.tz_localize(None)
    # 处理各列（极少数情况下数据列可能是 datetime）
    for c in out.columns:
        if np.issubdtype(out[c].dtype, np.datetime64):
            # pandas 的 datetime64[ns, tz] 会体现在 dtype 的 tz 属性，统一转成 naive
            try:
                out[c] = pd.to_datetime(out[c]).dt.tz_localize(None)
            except Exception:
                pass
    return out

# ---------- 基于配置的动态覆写（修复硬编码与变量顺序问题） ----------
# 1) 动态图/表文件名（保留你原来的定义，这里进行“覆写”）
_IDX_LABEL_FOR_NAME = DISPLAY_NAME.get(INDEX_TICKER, INDEX_TICKER)
_IDX_TAG = _IDX_LABEL_FOR_NAME.replace("/", "_")
FIG_PATH = DIR_OUT / f"{_IDX_TAG}_ETF_NAV_common_base.png"
TABLE_PATH = DIR_RAW / f"{_IDX_TAG}_ETF_NAV_for_plot.xlsx"

# 2) 动态线型映射（先保留你原来的 LINE_STYLES，再用自动生成覆盖同名键）
_auto_styles = { _IDX_LABEL_FOR_NAME: dict(linewidth=1.6, alpha=0.95, linestyle="-") }
_linestyles_pool = ["-", "--", ":", "-."]
for i, tk in enumerate(ETF_TICKERS):
    _auto_styles[DISPLAY_NAME.get(tk, tk)] = dict(linewidth=1.0, alpha=0.92, linestyle=_linestyles_pool[i % len(_linestyles_pool)])
# 用自动生成的键覆盖原有同名键，避免“名称对不上导致取不到样式”
LINE_STYLES = {**LINE_STYLES, **_auto_styles}

# ---------- 主流程 ----------
def main():
    # 1) 拉取原始序列（全历史），并保存“价格CSV + 分红CSV”
    # 指数
    idx_label = DISPLAY_NAME.get(INDEX_TICKER, INDEX_TICKER)
    idx_hist  = fetch_history_yf(INDEX_TICKER, START_DATE_STR, END_DATE_STR)
    idx_div   = fetch_dividends_yf(INDEX_TICKER)  # 多数指数无分红，得到空表也无妨
    save_price_and_dividends_csv(idx_label, INDEX_TICKER, idx_hist, idx_div)
    idx_close = idx_hist['Close'].astype(float)

    # ETFs
    etf_series_map: Dict[str, pd.Series] = {}
    for tk in ETF_TICKERS:
        label = DISPLAY_NAME.get(tk, tk)
        hist = fetch_history_yf(tk, START_DATE_STR, END_DATE_STR)
        div  = fetch_dividends_yf(tk)
        save_price_and_dividends_csv(label, tk, hist, div)
        # 累计净值近似 = Adj Close
        etf_series_map[label] = hist['Adj Close'].astype(float) if 'Adj Close' in hist.columns else hist['Close'].astype(float)

    # 2) 裁剪到 [START_DATE_STR, END_DATE_STR]（fetch 已经按区间，这里再次保证）
    idx_close = clip_period(idx_close, START_DATE_STR, END_DATE_STR)
    for k in list(etf_series_map.keys()):
        etf_series_map[k] = clip_period(etf_series_map[k], START_DATE_STR, END_DATE_STR)

    # 3) 统一到共同基准日 = 1（在“日频”上归一）
    raw_series = {idx_label: idx_close}  # <<< 修复：不再硬编码“标普500”
    raw_series.update(etf_series_map)    # {"指数显示名":Series, "SPY":Series, "VOO":Series, ...}
    series_norm_daily, base_date, base_values = rebase_to_common_base(raw_series)

    # 4) 合成用于保存与作图的数据框
    df_raw_daily = pd.concat(raw_series, axis=1).sort_index()
    df_norm_daily = pd.concat(series_norm_daily, axis=1).sort_index()

    # 5) 若需要：转换为周频（每周最后一个交易日）用于作图
    if USE_WEEKLY:
        df_plot = df_norm_daily.resample("W-FRI").last()
        freq_label = "周频（W-FRI）"
    else:
        df_plot = df_norm_daily.copy()
        freq_label = "日频"

    # 6) 画图（线性坐标 + 观感优化）
    plt.figure(figsize=(12.5, 6.2))
    for col in df_plot.columns:
        style = LINE_STYLES.get(col, dict(linewidth=1.0, alpha=0.9))
        plt.plot(df_plot.index, df_plot[col], label=col, **style)

    ylabel = f"归一化净值（公共基准日=1，作图频率：{freq_label}）"
    title = (
        f"{idx_label} 与其ETF净值曲线（公共基准日：{base_date.date()} 起=1）\n"
        f"{START_DATE_STR} 至 {END_DATE_STR}"
    )
    plt.title(title)
    plt.xlabel("日期"); plt.ylabel(ylabel)
    plt.grid(alpha=0.30, linestyle=":")
    plt.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    plt.tight_layout()
    plt.savefig(FIG_PATH, dpi=150, bbox_inches="tight")
    plt.show()

    # 7) 保存“用于画图的计算/放缩后的过程表格”
    meta_rows = [{"键": "公共基准日(base_date)", "值": str(base_date.date())},
                 {"键": "作图频率", "值": freq_label}]
    for k, v in base_values.items():
        meta_rows.append({"键": f"基准日原值[{k}]", "值": v})
    df_meta = pd.DataFrame(meta_rows)

    # 这里的 df_* 已经是 tz-naive 索引，直接写入
    with pd.ExcelWriter(TABLE_PATH, engine="openpyxl") as writer:
        df_raw_daily.to_excel(writer, sheet_name="原始日频", index=True)
        df_norm_daily.to_excel(writer, sheet_name="归一后(日频)", index=True)
        df_plot.to_excel(writer, sheet_name="用于作图(最终频率)", index=True)
        df_meta.to_excel(writer, sheet_name="元信息", index=False)

if __name__ == "__main__":
    main()
