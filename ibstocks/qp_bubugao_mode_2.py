"""
Mode_2: 步步高策略拆解之早盘偏多追涨策略
V1: pipeline filter + intraday filter(at 21:45, the last ten close prices are above vwap; chose the first one to backtest)
TODOs:
V2: backtest all stocks with above basic intraday filter
V3: strength daily filter with zhuduo + huoxing + valid adjustment
V4: dispatch bubugao into three sub-strategies: mode_2 is long before 22:00
TODO:
1. sync intraday strategy from vnpy
"""
import quantopian.algorithm as algo
from quantopian.pipeline import Pipeline, CustomFactor
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.data.morningstar import Fundamentals
import operator
import numpy as np
import pandas as pd
import time
import talib

# Switch factors

from quantopian.pipeline import factors, filters, classifiers

def QMYUS():
    return filters.make_us_equity_universe(
        target_size=5000,
        rankby=factors.AverageDollarVolume(window_length=20),
        mask=filters.default_us_equity_universe_mask(),
        groupby=classifiers.fundamentals.Sector(),
        max_group_weight=1,
        smoothing_func=lambda f: f.downsample('week_start'),
    )

class BubugaoIndex(CustomFactor):
    """
    Computes the bubugao signal based on the last 20 days k lines

    Pre-declares k lines price as default inputs and `window_length` as
    20.
    """

    inputs = [USEquityPricing.high, USEquityPricing.low, USEquityPricing.close, USEquityPricing.open]
    window_length = 50

    def compute(self, today, assets, out, highs, lows, closes, opens):
        from numpy import nanmin, nanmax
        
        for i in range(-11,0):# last eleven k lines
            pre3HH = nanmax(highs[i-3:i], axis=0) # last three days' highest price
            k_highest = nanmax(highs[-14:i], axis=0) # must be tupo and positive k line
            # long index: tupo + positive
            ma10 = closes[i-10:i].mean(axis=0)
            out[(closes[i]>pre3HH)&(closes[i]>opens[i])&(closes[i] > k_highest)] = i+20

            # filter non positice adjustment by checking last 5-1=4 k lines with ma10 and ma5
            if i<-1 and i >= -5:
                ma10 = closes[i-9:i+1].mean(axis=0)
                out[closes[i]<ma10] = 0
            if i < -1 and i >= -3:
                ma5 = closes[i-4:i+1].mean(axis=0)
                ma10 = closes[i-9:i+1].mean(axis=0)
                out[ma10>ma5] = 0

        #最近10周有中大阳
        isBigKlineAvailable = nanmax(closes[-50:]-opens[-50:]*1.075, axis=0)
        out[isBigKlineAvailable<=0] = 0 
        #这波多有中阳（常见于弩末步步高）
        isMKAvailable = nanmax(closes[-11:]-opens[-11:]*1.045, axis=0)
        out[isMKAvailable<=0] = 0

        #当日为刚好站上5日线的中小阳, 且从10日线起
        #cur_ma5 = closes[-5:].mean(axis=0)
        #cur_ma10 = closes[-10:].mean(axis=0)
        #isBigKline = closes[-1] - opens[-1]*1.055
        #isYang = closes[-1] - opens[-1]
        #isAboveVwap = closes[-1] - cur_ma5
        #isTooAboveVwap = closes[-1] - cur_ma5*1.03
        #isFromMa10_1 = lows[-1] - cur_ma5*0.985
        #isFromMa10_2 = lows[-1] - cur_ma10*1.015
        #out[(isBigKline > 0) | (isAboveVwap < 0) | (isTooAboveVwap > 0) | (isYang < 0) | ((isFromMa10_1 > 0) & (isFromMa10_2 > 0))] = 0
        

        #有近10日线的调整：前4日最低价低于当日ma10
        cur_ma10 = closes[-10:].mean(axis=0)
        isAdjustwithMa10 = nanmin(lows[-5:-1], axis=0) - cur_ma10
        out[isAdjustwithMa10 > 0] = 0# 有接近10日线调整k线
        high_3d = nanmax(closes[-3:], axis=0)
        high_pre_5d = nanmax(highs[-8:-3], axis=0)
        out[(high_3d > high_pre_5d*1.015)] = 0 #排除近期拔高品种
        
        out[(out==15) & (closes[-1] < opens[-1])] = 0 # 趋4非阴线
        # 趋5~8阴线高于5日均线
        out[(out<15) &(out>=11) & ((closes[-1] < opens[-1])&(closes[-1] < closes[-5:].mean(axis=0)))] = 0 
                    
def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    #context.aapl = symbols('AAPL')
    #context.count = 0
    #context.test_len = 10
    algo.schedule_function(
        stock_filter,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(minutes =14),
    )

    # Record tracking variables at the end of each day.
    ##algo.schedule_function(
    ##    record_vars,
    ##    algo.date_rules.every_day(),
    ##    algo.time_rules.market_close(),
    ##)

    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    # Base universe set to the QTradableStocksUS
    #base_universe = QTradableStocksUS()
    #base_universe = QMYUS()
    exchange = Fundamentals.exchange_id.latest
    exch_filter = exchange.element_of(['NAS', 'NYS', 'AME'])
    asset_type = Fundamentals.security_type.latest
    stock_filter = asset_type.notnull()

    base_universe = exch_filter & stock_filter

    price_filter = USEquityPricing.close.latest > 5
    money_filter = USEquityPricing.volume.latest*USEquityPricing.close.latest > 10000000

    sma5 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=5)
    sma10 = SimpleMovingAverage(inputs=[USEquityPricing.close], window_length=10)

    ma_filter = (USEquityPricing.close.latest >= sma10) & (sma5 > sma10)

    universe = base_universe & price_filter & money_filter & ma_filter
    
    bubugao_index = BubugaoIndex(mask=universe)
    
    valid_bubugao = (bubugao_index>=11)&(bubugao_index<=15) #趋8---趋4


    pipe = Pipeline(
        columns={
            'close':USEquityPricing.close.latest,
        },
        #screen=universe
        screen=(universe&valid_bubugao)
    )
    
    return pipe


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    #context.median_start = 1
    context.percent = 0
    context.zz_count_max = 1
    # TODO: 每个stock分别设置该值
    context.zy_threshold = 1.045

    context.median_start = {}
    context.zz_count = {}
    context.zz_1_high = {}
    context.low_first20mins = {}
    context.yestoday_close = {}

    # get current trading date
    context.cur_td = pd.to_datetime(get_datetime('US/Eastern'), utc=True).date() 
    #print(context.cur_td)

    # These are the securities that we are interested in trading each day.
    context.output = algo.pipeline_output('pipeline')
    context.security_list = context.output.index
    context.position_stock = []
    print(context.output)

def handle_data(context, data):
    if len(context.position_stock) == 0:
        return
    for stock in context.position_stock:
        h = data.history(stock, fields=['open', 'high', 'low', 'close', 'price', 'volume'], bar_count=390, frequency="1m")
        h = h.tz_convert('US/Eastern')

        mk = h[h.index.date==context.cur_td]
    
    #rn = pd.to_datetime(get_datetime('US/Eastern'), utc=True).time() 
    
        num_bar = len(mk)

        #context.yestoday_close[stock] = h[len(h) - num_bar - 1]
        # 尾盘强制平仓
        if num_bar >= 375:
            if context.portfolio.positions[stock].amount>0:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                order_target(stock, 0)
                print("qiangzhi pingcang %s at price: %f"%(stock, mk["price"][-1]))
            return
        
        mk = mk.assign(vwap = ((mk["volume"]*mk["price"]).cumsum() / mk["volume"].cumsum()).ffill())

        # 重置参数，重新开始波段计算
        if mk['price'][-1] <= mk['vwap'][-1]:
            context.median_start[stock] = num_bar
            context.zz_count[stock] = 0
            context.zz_1_high[stock] = 0.0
        
        if num_bar <= 30 and num_bar >= 15:
            if num_bar == 20:
                context.low_first20mins[stock] = mk["price"].min()

            # 步步高开仓策略1：21:45 偏多走稳，涨幅低于zy_threshold，则追涨
            if num_bar >= 15 and num_bar <= 18:
                if ((mk["price"][5:] >= mk["vwap"][5:]*0.998).all() or (len(mk[mk["price"] >= mk["vwap"]])>= num_bar-3 and (mk["price"][-3:] >= mk["vwap"][-3:]*0.999).all())) \
                    and mk["price"].max() < mk["price"].min()*context.zy_threshold and mk["price"].min() > h['price'][-num_bar - 1]*0.992:
                    open_order = get_open_orders(stock)
                    if len(open_order) > 0:
                        cancel_order(open_order[0])
                    order_percent(stock, context.percent)
                    print("kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))

            elif num_bar == 19:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                    print("qiangzhi cancel")
            else:
                pass

        elif num_bar > 30:
            #13：45~13:55定点止损
            mk_l20 = mk[-20:]
            if num_bar >= 255 and num_bar < 265 and context.portfolio.positions[stock].amount>0  and len(mk_l20[mk_l20["price"] > mk_l20["vwap"]]) < 16:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                order_target(stock, 0)
                print("dingdian: 13:45 zhishun %s at price: %f"%(stock, mk["price"][-1]))

            #止损：长期低于分时后反弹止损
            if context.portfolio.positions[stock].amount>0 and len(mk[mk["price"] < mk["vwap"]]) > 40 \
                and mk_l20["price"][-1] > (mk['price'][20:].min() + mk['price'].max())/2 and mk_l20["price"][-1] < mk_l20["price"].max():
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                order_target(stock, 0)
                print("changqi_piankong_fantan_zhishun %s at price %.2f"%(stock, mk["price"][-1]))

            if num_bar - context.median_start[stock] > 30:
                median_CH_index, median_CH = max(enumerate(mk["price"][context.median_start[stock]:]), key=operator.itemgetter(1))
                median_h_zz = mk[context.median_start[stock]+median_CH_index:]
                if len(median_h_zz) >= 30 and len(median_h_zz) < 60 and median_CH > median_h_zz["vwap"][0] * 1.015:
                    if len(median_h_zz) == 30:
                        context.zz_count[stock] += 1
                        if context.zz_count[stock] == 1:
                            context.zz_1_high[stock] = median_CH
                        #case1&2: close long while zz 1 or 2 times
                        if context.portfolio.positions[stock].amount>0 and context.zz_count[stock] >= context.zz_count_max \
                            and median_CH > h['price'][len(h) - num_bar - 1]*1.055 and mk["price"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                            open_order = get_open_orders(stock)
                            if len(open_order) > 0:
                                cancel_order(open_order[0])
                            order_target(stock, 0)
                            print("zhiying %s at price: %f"%(stock, mk["price"][-1]))
                    else:
                        if context.portfolio.positions[stock].amount>0 and context.zz_count[stock] >= context.zz_count_max \
                            and median_CH > h['price'][len(h) - num_bar - 1]*1.055 and mk["price"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                            open_order = get_open_orders(stock)
                            if len(open_order) > 0:
                                cancel_order(open_order[0])
                            order_target(stock, 0)
                            print("zhiying %s at price: %f"%(stock, mk["price"][-1]))

def stock_filter(context, data):
    if len(context.security_list) == 0:
        return

    for stock in context.security_list:
        #以下基于周k对评分的前两部分做了初步判定，由于notebook里暂时无法同步该功能，vnpy输入目标股时需要人工核对删选，具体步骤如下：
        #股性（有2跟以上中大阳，0.5分以上） +  近10周收盘高低点的波动幅度在35%以上 + 上周站稳5，10均线 + 近10周创近20周新高（保证至少是反弹多）
        dk = data.history(stock, fields=['price','open','high'], bar_count=5*20, frequency="1d")
        #dk = dk.assign(zhangfu = ((mk["volume"]*mk["price"]).cumsum() / mk["volume"].cumsum()).ffill())
        if (dk['price'] > dk['open']*1.05).sum() < 2 or (dk['price'] > dk['open']*1.09).sum() < 1:
            continue 

        # 主多|准主多判断：最近一周在5均线之上（所以无法发现弩末机会）+ ?
        wk = dk.resample('1W-FRI', closed='right', label='right').last().dropna()
        ma5 = talib.MA(wk['price'], timeperiod=5)
        ma10 = talib.MA(wk['price'], timeperiod=10)

        if len(ma10[~np.isnan(ma10)]) < 11 and wk['price'].max() > wk['price'].min()*1.35: #新股
            pass
        else:
            # 基础判定：最近1周站稳5,10均线 + 最近10周波动大
            if wk['price'][-2] <= ma5[-2] or wk['price'][-2] <= ma10[-2] or wk['price'][-11:-1].max() < wk['price'][-11:-1].min()*1.35:
                continue
            # 条件判定：20周内新高在近10周（至少是止跌反弹多）+ ?
            if wk['price'][-11:-1].max() < wk['price'][:-11].max():
                continue

        #TODOs:条件判定：删除一些趋多信号不佳的标的：中阴，中大阳且高点接近前期高点
        if (dk['price'][-2] > dk['open'][-2]*1.045 and dk['high'][-2] > dk['high'][-11:-2].max()*0.995) \
            or (dk['price'][-2] < dk['open'][-2]*0.955):
            continue
        
        #context.position_stock.append(stock)
        #context.median_start[stock] = 1
        #context.zz_count[stock] = 0
        #context.zz_1_high[stock] = 0.0

        # 之前周k高于均线根数>=3,<7,则只发掘连续创新高步步高；>=7且高点在最近4周则参与弩末机会机会
        #if ((wk['price'][-10:-1]>=ma10[-10:-1]).sum()>=3 and (wk['price'][-10:-1]>=ma10[-10:-1]).sum()<7 \
        #    and wk['price'][-2] >= ma5[-2] and wk['price'][-2] > ma10[-2] and \
        #    (dk['price'].max() == dk['price'][-10:].max() and dk['price'][-1] < dk['high'].max()*0.93)) \
        #    or ((wk['price'][-10:-1]>=ma10[-10:-1]).sum()>=7 and wk['price'][-2] > ma10[-2] \
        #    and dk['price'].max() > dk['price'].min()*1.6 and dk['price'][-1] < dk['high'].max()*0.93) \
        #    and (len(dk[dk['price']>dk['open']*1.075]))>=2 and (len(dk[dk['price']>dk['open']*1.1]))>=1:
        #    pass
        # 周k趋多向上要有空间，或者日k趋多刚突破 + 近期中大阳统计股性较好品种
        #if (wk['price'][-10:-1]>=ma10[-10:-1]).sum()>=8 and wk['price'][-2] >= ma5[-2] and wk['price'][-2] > ma10[-2] \
        #    and ((dk['price'].max() > dk['price'][-25:].max() and dk['price'][-1] < dk['high'].max()*0.9) or \
        #    dk['price'].max() == dk['price'][-10:].max()) \
        #    and (len(dk[dk['price']>dk['open']*1.075]))>=2:
        #    pass
        #else:
        #    continue

        h = data.history(stock, fields=['open', 'high', 'low', 'close', 'price', 'volume'], bar_count=20, frequency="1m")
        h = h.tz_convert('US/Eastern')
        mk = h[h.index.date==context.cur_td]
        mk = mk.assign(vwap = ((mk["volume"]*mk["price"]).cumsum() / mk["volume"].cumsum()).ffill())
        if (mk["price"][-8:] >= mk["vwap"][-8:]*0.9985).all() and mk["price"][-1] < mk["price"][0]*context.zy_threshold:
            context.position_stock.append(stock)
            context.median_start[stock] = 1
            context.zz_count[stock] = 0
            context.zz_1_high[stock] = 0.0
            #context.median_start[stock] = 1

    if len(context.position_stock) > 0:
        context.percent = 1/len(context.position_stock)
    print(context.position_stock)