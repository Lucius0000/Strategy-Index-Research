# -*- coding: utf-8 -*-
"""
沪深300指数 & 其ETF（510300/510310/510330）净值曲线
- 指数数据：ak.stock_zh_index_daily（收盘点位）
- ETF数据：ak.fund_open_fund_info_em(indicator="累计净值走势")
- 归一方式：公共覆盖期第一天 = 1（在“日频”归一；若之后做周频降采样，首个点是基准日后的首个周五）
- 坐标：线性坐标
- 输出图：output/CSI300_ETF_NAV_common_base.png
- 输出表：output/raw_data/CSI300_ETF_NAV_for_plot.xlsx（含原始日频、归一后日频、最终作图频率数据、元信息）
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, List
import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import pandas as pd
import numpy as np
import akshare as ak
import matplotlib
import matplotlib.pyplot as plt

# ---------------- 配置区 ----------------
INDEX_TICKER: str = '000300.SS'                     # 沪深300
ETF_TICKERS:  List[str] = ['510300.SS','510310.SS','510330.SS']
START_DATE_STR = "2012-05-01"                       # 起始日期（会先裁剪到此日期）
END_DATE_STR   = datetime.now().strftime("%Y-%m-%d")

DIR_OUT = Path("output"); DIR_OUT.mkdir(parents=True, exist_ok=True)
DIR_RAW = DIR_OUT / "raw_data"; DIR_RAW.mkdir(parents=True, exist_ok=True)
FIG_PATH = DIR_OUT / "CSI300_ETF_NAV_common_base.png"
TABLE_PATH = DIR_RAW / "CSI300_ETF_NAV_for_plot.xlsx"

# 画图风格
USE_WEEKLY = True               # 是否使用周频降采样；日频= False
LINE_STYLES = {
    "沪深300":   dict(linewidth=1.6, alpha=0.95, linestyle="-"),
    "510300":   dict(linewidth=1.0, alpha=0.92, linestyle="-"),
    "510310":   dict(linewidth=1.0, alpha=0.92, linestyle="--"),
    "510330":   dict(linewidth=1.0, alpha=0.92, linestyle="-."),
}

# ---------- 中文字体 ----------
def setup_chinese_font():
    matplotlib.rcParams['axes.unicode_minus'] = False
    matplotlib.rcParams['font.sans-serif'] = [
        'SimHei','Microsoft YaHei','Arial Unicode MS','Noto Sans CJK SC'
    ]
setup_chinese_font()

# ---------- 工具 ----------
def to_exchange_prefix(ticker: str) -> str:
    return "sh" if ticker.endswith(".SS") else "sz"

def code6(ticker: str) -> str:
    return ticker.split(".")[0]

def ak_symbol_for_index(ticker: str) -> str:
    return f"{to_exchange_prefix(ticker)}{code6(ticker)}"

def fetch_index_close_series(index_ticker: str) -> pd.Series:
    """沪深300：返回收盘点位序列（DatetimeIndex 升序）"""
    sym = ak_symbol_for_index(index_ticker)
    df = ak.stock_zh_index_daily(symbol=sym)
    if df is None or len(df) == 0:
        raise RuntimeError(f"指数数据为空：{index_ticker}")
    df = df.rename(columns={'date':'日期','close':'收盘','日期':'日期','收盘价':'收盘'})
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df = df.dropna(subset=['日期']).sort_values('日期')
    s = pd.Series(df['收盘'].astype(float).values, index=df['日期'])
    s = s[~s.index.duplicated(keep='last')]
    return s

def fetch_etf_cum_nav_series(etf_ticker: str) -> pd.Series:
    """ETF 累计净值（单位净值 + 分红再投），用基金接口返回的‘累计净值走势’"""
    c6 = code6(etf_ticker)
    df = ak.fund_open_fund_info_em(symbol=c6, indicator="累计净值走势")
    if df is None or len(df) == 0:
        raise RuntimeError(f"ETF累计净值为空：{etf_ticker}")
    # 接口字段：['净值日期','累计净值']
    df = df.rename(columns={'净值日期':'日期'})
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df = df.dropna(subset=['日期']).sort_values('日期')
    s = pd.Series(df['累计净值'].astype(float).values, index=df['日期'])
    s = s[~s.index.duplicated(keep='last')]
    return s

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

    # 若当日无值（如周末），取 >= base_date 的第一个有效日
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

# ---------- 主流程 ----------
def main():
    # 1) 拉取原始序列（全历史）
    idx_close = fetch_index_close_series(INDEX_TICKER)
    etf_510300 = fetch_etf_cum_nav_series('510300.SS')
    etf_510310 = fetch_etf_cum_nav_series('510310.SS')
    etf_510330 = fetch_etf_cum_nav_series('510330.SS')

    # 2) 裁剪到 [START_DATE_STR, END_DATE_STR]
    idx_close = clip_period(idx_close, START_DATE_STR, END_DATE_STR)
    etf_510300 = clip_period(etf_510300, START_DATE_STR, END_DATE_STR)
    etf_510310 = clip_period(etf_510310, START_DATE_STR, END_DATE_STR)
    etf_510330 = clip_period(etf_510330, START_DATE_STR, END_DATE_STR)

    # 3) 统一到共同基准日 = 1（在“日频”上归一）
    raw_series = {
        "沪深300": idx_close,
        "510300": etf_510300,
        "510310": etf_510310,
        "510330": etf_510330,
    }
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
        f"沪深300与其ETF净值曲线（公共基准日：{base_date.date()} 起=1）\n"
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

    with pd.ExcelWriter(TABLE_PATH, engine="openpyxl") as writer:
        df_raw_daily.to_excel(writer, sheet_name="原始日频", index=True)
        df_norm_daily.to_excel(writer, sheet_name="归一后(日频)", index=True)
        df_plot.to_excel(writer, sheet_name="用于作图(最终频率)", index=True)
        df_meta.to_excel(writer, sheet_name="元信息", index=False)

if __name__ == "__main__":
    main()
