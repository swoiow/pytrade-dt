import asyncio
import datetime as dt
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests


USER_HOME = Path(os.environ.get("HOME") or os.environ.get("USERPROFILE") or Path.home())
HOLIDAYS_CACHE_PATH = USER_HOME / "pytrade" / "holiday_days.json"
HOLIDAYS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

__UA__ = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)


def _fetch_holiday_days_sync(year: int):
    url = f"https://vaserviece.10jqka.com.cn/mobilecfxf/data/json_{year}.txt"
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": __UA__})
        resp.raise_for_status()
        data = resp.json()
        print(f"[{year}] 成功")
        return {str(year): data}
    except Exception as e:
        print(f"[{year}] 失败: {e}")
        return {}


async def fetch_all_holiday_days(start_year: int = 1990, end_year: int = dt.date.today().year):
    all_holiday_days = {}

    loop = asyncio.get_running_loop()
    years_to_fetch = [year for year in range(start_year, end_year + 1) if str(year) not in all_holiday_days]

    async def fetch_with_executor(year: int):
        return await loop.run_in_executor(executor, _fetch_holiday_days_sync, year)

    results = []
    # 限制最大并发线程数为 3
    with ThreadPoolExecutor(max_workers=3) as executor:
        tasks = [fetch_with_executor(year) for year in years_to_fetch]
        results = await asyncio.gather(*tasks)

    for result in results:
        all_holiday_days.update(result)

    with open(HOLIDAYS_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_holiday_days, f, ensure_ascii=False, indent=2)
        print(f"[OK] 所有交易日数据已保存: {HOLIDAYS_CACHE_PATH}")

    return all_holiday_days


def is_workday(date: dt.date) -> bool:
    """
    判断指定日期是否是中国交易日。
    本地缓存整体为一个 JSON 文件，按年份组织，如无则请求 API 自动保存。
    """

    year = str(date.year)
    date_str = date.strftime("%m%d")
    
    if HOLIDAYS_CACHE_PATH.exists():
        with open(HOLIDAYS_CACHE_PATH, "r", encoding="utf-8") as f:
            all_trading_days = json.load(f)
            if year not in all_trading_days:
                all_trading_days = asyncio.run(fetch_all_holiday_days())
    else:
        all_trading_days = asyncio.run(fetch_all_holiday_days())
    return date_str in set(all_trading_days.get(year, []))


def get_latest_cn_trading_day(base_datetime: Optional[Union[dt.datetime, dt.date]] = None) -> dt.date:
    """ 获取中国最新的交易日。

    规则说明：
    1. 节假日,周末（周六或周日）,非节假日+工作日+当前时间在早上9点前，回退前一天，循环到不用回退。
    2. 支持传入任意 datetime 或 date 类型，默认使用当前时间。

    参数:
        base_datetime: 可选，指定参考时间，若未指定则使用当前系统时间。

    返回:
        最新的交易日（datetime.date 类型）
    """
    now = base_datetime or dt.datetime.now()
    if isinstance(now, dt.date) and not isinstance(now, dt.datetime):
        now = dt.datetime.combine(now, dt.time.min)

    today = now.date()

    # 第一步：先处理节假日和周末
    latest_trading_day = today
    while not is_workday(latest_trading_day):
        latest_trading_day -= dt.timedelta(days=1)

    # 第二步：如果当前时间早于上午9点，且今天本来就是交易日，也要回退
    if now.hour < 9 and latest_trading_day == today:
        latest_trading_day -= dt.timedelta(days=1)
        while not is_workday(latest_trading_day):
            latest_trading_day -= dt.timedelta(days=1)

    return latest_trading_day


# SPECIAL_EVENTS = {
#     # ==== 1990s 早期市场与危机阶段 ====
#     # 沪深股市重启—探索阶段（恢复交易、制度初建）
#     "沪深股市重启初期": ["1990-12-19", "1991-12-31"],
#     # 1992 扭曲牛市（入世预期、政策利好、股民热情高涨）
#     "1992扭曲牛市": ["1992-03-01", "1993-12-31"],
#     # 1994–1995 股市结构调整与熊市（政策收紧、券商改革滞后）
#     "1994–1995熊市": ["1994-01-01", "1995-12-31"],
#     # 1996–1997 “百股跌停”风波（局部监管缺失、内幕交易高发）
#     "百股跌停风波": ["1996-10-01", "1997-06-30"],
#     # 亚洲金融危机（泰铢兑美元贬值 → 地区资本流出 → A 股承压）
#     "亚洲金融危机": ["1997-07-01", "1998-12-31"],
#     # ==== 2000s 全球化与结构性事件 ====
#     # 互联网泡沫破灭（Nasdaq指数暴跌 → 国内“网股”板块大幅下行）
#     "互联网泡沫破灭": ["2000-03-01", "2002-10-31"],
#     # 中国加入WTO（入世后外资流入预期 → 证券市场长期向好预期）
#     "中国加入WTO": ["2001-12-11", "2002-06-30"],
#     # 2003 SARS疫情冲击（经济短期停摆 → 市场抛压明显）
#     "SARS疫情": ["2003-02-01", "2003-07-31"],
#     # 股权分置改革启动（推动流通股与限售股整合 → 改革推进期）
#     "股权分置改革": ["2004-12-01", "2005-12-31"],
#     # 2006–2007 牛市（分置改革见效、政策进一步放松、融资融券试点）
#     "2006–2007牛市": ["2006-01-01", "2007-10-31"],
#     # 全球金融危机（美次贷危机 → 全球资本纷纷撤离）
#     "全球金融危机": ["2008-01-01", "2009-06-30"],
#     # ==== 2010s 国内外再平衡与新兴板块 ====
#     # 欧债危机尾声（欧洲主权债务风险集聚 → 全球避险情绪升温）
#     "欧债危机尾声": ["2012-01-01", "2013-01-31"],
#     # 2014–2015 牛市（降准降息、沪港通、融资盘入场 → 指数大幅拉升）
#     "2014–2015牛市": ["2014-04-01", "2015-06-12"],
#     # 中国股灾（杠杆资金爆仓、熔断机制试点失败 → 市场大幅回落）
#     "中国股灾": ["2015-06-01", "2016-02-29"],
#     # 人民币“8·11”汇改及贬值风波（市场对汇率预期波动加剧）
#     "人民币贬值风波": ["2015-08-11", "2016-01-31"],
#     # 股市熔断试点（短短两轮熔断 → 政策紧急取消 → 市场恐慌加剧）
#     "股市熔断": ["2016-01-04", "2016-02-11"],
#     # 中美贸易摩擦（关税冲突 → 产业链调整预期 → 资金风险偏好下降）
#     "中美贸易摩擦": ["2018-07-06", "2019-12-31"],
#     # ==== 2020s 新经济浪潮与全球扰动 ====
#     # 新冠疫情（封城封锁 → 政策大规模刺激 → 板块轮动极其剧烈）
#     "新冠疫情": ["2020-01-30", "2023-05-31"],
#     # 房地产“三道红线”与恒大危机（房企流动性危机 → 金融板块压力）
#     "房地产调控高压": ["2020-08-01", "2021-12-31"],
#     # 科技企业监管风暴（反垄断与网络安全新规 → 科技板块大幅震荡）
#     "科技监管风暴": ["2021-07-01", "2021-12-31"],
#     # 俄乌冲突（能源与大宗商品供应骤变 → 全球通胀与资本流向扰动）
#     "俄乌冲突": ["2022-02-24", "2022-12-31"],
#     # 全球加息潮与宽松预期切换（美联储加息 → 后期通胀回落 → 宽松政策预期）
#     "全球加息潮/宽松预期": ["2022-11-01", "2023-12-31"],
#     # 政策—共同富裕与碳中和（“双碳”与“共同富裕”导向 → 产业结构切换）
#     "共同富裕/碳中和政策": ["2021-03-01", "2022-12-31"],
# }

SPECIAL_EVENTS = {
    # 一、全球／区域金融危机类（宏观突发型）
    "亚洲金融危机": ["1997-07-01", "1998-12-31"],  # 市场整体下行、成交萎缩
    "互联网泡沫破灭": ["2000-03-01", "2002-10-31"],  # 网股集体暴跌 / 长期阴跌
    "全球金融危机": ["2008-01-01", "2009-06-30"],  # A 股在 08 年 10 月跌幅超 40%
    "欧债危机尾声": ["2012-01-01", "2013-01-31"],  # 全球避险情绪、相对宽松期少

    # 二、制度变革／监管政策冲击（制度突击型）
    "股权分置改革": ["2004-12-01", "2005-12-31"],  # 流通股解禁引发的持续分歧
    "2016年股市熔断": ["2016-01-04", "2016-02-11"],  # 两轮熔断效果干扰日常交易逻辑
    "2015年股灾": ["2015-06-01", "2016-02-29"],  # 杠杆爆仓、熔断失效、救市反复，信号无参考价值
    "熔断机制短暂失效": ["2016-01-04", "2016-02-11"],  # 可与“2016年股市熔断”合并

    # 三、宏观／国际事件（高度同步化冲击）
    "2003年SARS疫情": ["2003-02-01", "2003-07-31"],  # 恐慌且交易量急剧萎缩
    "2020年新冠疫情初期": ["2020-01-30", "2020-03-31"],  # 单日两次熔断 / 交易完全失真

    # 四、外部监管与流动性拐点（政策+宏观双重效应）
    "2015年8·11汇改风波": ["2015-08-11", "2016-01-31"],  # 汇率剧烈波动导致股市恐慌
    "2018年中美贸易摩擦高峰": ["2018-07-06", "2018-12-31"],  # 贸易战第一阶段到谈判僵局，市场避险明显
    "2021年科技监管风暴": ["2021-07-01", "2021-12-31"],  # 反垄断、平台经济监管导致科技股单边下跌

    # 五、极端情绪高点／低点（泡沫与恐慌阶段）
    "2007年A股泡沫顶": ["2006-07-01", "2007-10-31"],  # 指数从 2006 底至 07 年末涨幅近 250%，随后暴跌
    "2020年二次熔断戳顶": ["2020-02-03", "2020-02-12"],  # 短期两次熔断后市场被彻底扰乱

    # 六、“剔除窗口”配置示例（可选更细粒度剔除）
    # 如果你觉得某些单日或单周样本已经对后续分析造成严重干扰，可再添加：
    "2015-06-26熔断日": ["2015-06-26", "2015-06-26"],
    "2016-02-08熔断日": ["2016-02-08", "2016-02-08"],

    # 1. 蚂蚁集团 IPO 叫停（直接导致金融科技板块、沪深两市短线急跌）
    #    2020-11-03 当天，银监会、证监会联合发声，暂停蚂蚁集团 IPO；
    #    2020-11-04 交易连续下跌；2020-11-05 微幅反弹，但行业情绪持续低迷。
    "蚂蚁集团IPO叫停": ["2020-11-03", "2020-11-05"],

    # 2. “双减”政策提出（教育板块遭遇封杀式下挫）
    #    2021-07-24 教育部等四部委联合发布“双减”指导意见，在线教育龙头股当天急跌；
    #    2021-07-25–07-26 连续两个交易日大幅回调 (>20%)，市场情绪极度恐慌。
    "教育行业“双减”政策": ["2021-07-24", "2021-07-26"],

    # 3. 科技反垄断监管表态（平台经济板块巨震）
    #    2021-09-24 中办、国办印发《关于促进平台经济规范健康持续发展的指导意见》，
    #    当日包括 BATJ（百度、阿里、腾讯、京东）在内的大盘科技股集体暴跌；
    #    2021-09-27（隔夜美股大跌+国内跟跌）再度放量下探，次日略有反弹。
    "科技平台反垄断指导意见": ["2021-09-24", "2021-09-27"],

    # 4. 房地产行业“严厉调控”重要信号（高位地产股集体闪崩）
    #    2022-01-05 央行、住建部联合召开会议，明确强调“房住不炒 + 去房地产金融化”；
    #    2022-01-06–01-07 房地产板块、银行地产相关信托类股连续大幅杀跌 (＞15%)。
    "房地产“去地产金融化”表态": ["2022-01-05", "2022-01-07"],

    # 5. 气候目标和“三道红线”政策强化表态（带动环保与碳中和板块剧烈波动）
    #    2022-03-07 中央政治局会议明确：“碳达峰、碳中和目标要更快落实”，
    #    当日多数高耗能、煤炭与有色金属股大跌；碳中和相关龙头（光伏、储能）当天却迎来
    #    短线大幅拉升，但次日（03-08）因获利盘回吐出现剧烈震荡。
    "碳达峰碳中和政策强化": ["2022-03-07", "2022-03-08"],

    # 6. 中共中央、国务院关于稳定资本市场的“回暖拐点”表态
    #    2023-04-16 政治局会议提出“稳定资本市场、稳定外资流入”；
    #    2023-04-17–04-18 A股在连阴后突然放量大涨，全市场暴涨 (>4%)；
    #    2023-04-19 涨幅回落，但市场情绪明显改善。
    "资本市场稳定表态": ["2023-04-16", "2023-04-18"],

    # 7. “房住不炒”进一步升级文件（地产板块再遭速跌）
    #    2023-07-18 政策层发布《关于进一步推进房地产市场平稳健康发展若干措施》，
    #    明确从土地、融资、买房、销售等多个环节加强调控，当日地产板块、银行地产相关股
    #    再度闪崩 (>10%)；次日（07-19）继续大幅杀跌，07-20 微幅反弹。
    "房地产调控升级措施": ["2023-07-18", "2023-07-20"],

    # 8. 2024年全国“两会”政策基调落地（对制造业、科技与消费等行业造成短线调整）
    #    2024-03-05 在政协与人大会议上露面强调“加速产业升级、扩大内需、促进数字经济发展”，
    #    当天新兴产业（半导体、芯片设计、智能制造）板块小幅跳水 (>5%)；但因配套利好落地预期，
    #    次日（03-06）迅速反弹；03-07 市场情绪回归正常。
    "2024“两会”产业政策基调": ["2024-03-05", "2024-03-07"],

    # 9. “降准/降息”超预期操作（引发短线上涨与随后获利回吐）
    #    2024-08-15 PBOC 突然宣布下调存款准备金率 25 个基点（降准），
    #    当日上证指数大幅拉升 (>3%)；次日（08-16）获利盘涌现，出现剧烈震荡；
    #    08-17 小幅回落，市场重回震荡；
    "2024超预期降准": ["2024-08-15", "2024-08-17"],

    # 10. “科技创新板块深度扶持”政策预期落地（短期资金追捧与获利回吐）
    #     2025-03-05 十三届全国人大三次会议中提出“进一步加快科技创新板块融资与注册制改革”，
    #     当日科创板、创业板呈现短线高开 (>4%)；03-06–03-07 出现高位回撤 (>5%)，波动集中在 3 个交易日。
    "2025科创板深度扶持表态": ["2025-03-05", "2025-03-07"],
}


def load_special_events():
    return {
        name: (
            dt.datetime.strptime(start, "%Y-%m-%d").date(),
            dt.datetime.strptime(end, "%Y-%m-%d").date(),
        )
        for name, (start, end) in SPECIAL_EVENTS.items()
    }


def mark_special_events(date: "dt.date") -> Optional[str]:
    """
    根据日期标记特殊金融或社会事件。

    参数:
        date: 要判断的日期 (datetime.date)

    返回:
        如果是特殊事件日，返回事件名；否则返回 None。
    """

    special_events = load_special_events()
    for event_name, (start_date, end_date) in special_events.items():
        if start_date <= date <= end_date:
            return event_name
    return None


# 预处理成 IntervalIndex
def build_event_interval_index(events: dict) -> tuple["pd.IntervalIndex", list[str]]:
    intervals = []
    labels = []
    for name, (start, end) in events.items():
        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
        intervals.append(pd.Interval(start, end, closed="both"))
        labels.append(name)
    return pd.IntervalIndex(intervals), labels


def mark_special_events_faster(trade_dates: "pd.Series") -> "pd.Series":
    intervals, labels = build_event_interval_index(SPECIAL_EVENTS)
    dates = pd.to_datetime(trade_dates)  # 确保为 datetime64[ns]
    result = pd.Series(None, index=trade_dates.index, dtype="string")

    for interval, label in zip(intervals, labels):
        result.loc[dates.between(interval.left, interval.right)] = label

    return result


if __name__ == '__main__':
    print(get_latest_cn_trading_day())
