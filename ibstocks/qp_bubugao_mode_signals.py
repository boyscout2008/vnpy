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
from quantopian.pipeline.filters import QTradableStocksUS, StaticSids
from quantopian.pipeline.factors import SimpleMovingAverage
from quantopian.pipeline.data.morningstar import Fundamentals
import operator
import numpy as np
import pandas as pd
import time
import talib

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
    window_length = 20

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
        
        #out[(out==15) & (closes[-1] < opens[-1])] = 0 # 趋4非阴线
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
    #exchange = Fundamentals.exchange_id.latest
    #exch_filter = exchange.element_of(['NAS', 'NYS', 'AME'])
    #asset_type = Fundamentals.security_type.latest
    #stock_filter = asset_type.notnull()

    #base_universe = exch_filter & stock_filter

    #base_universe = StaticSids([53098, 23710, 53884, 39635]) #3.a
    base_universe = StaticSids([52957, 50683, 8134, 3436,22355,53926,3431,8119,52490,33052])
    #base_universe = StaticSids([53098])
    
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
        #screen=(universe&valid_bubugao)
        screen=(base_universe)
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
    context.zy_threshold = 1.045 #股性极好的放大到0.09，如clsk；股性好则放大到0.07
    context.xiankong_zd_duo = False
    context.pianduo_tiaozheng = False

    context.cc_avg_price = {}
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
    
        num_bar = len(mk)

        #context.yestoday_close[stock] = h[len(h) - num_bar - 1]
        # 尾盘强制平仓
        if num_bar >= 375:
            if context.portfolio.positions[stock].amount>0:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                order_target(stock, 0)
                res = (mk["price"][-1]-context.cc_avg_price[stock])/context.cc_avg_price[stock]*100
                print("qiangzhi pingcang %s at price: %f, with profit %.1f"%(stock, mk["price"][-1], res))
            continue
        
        mk = mk.assign(vwap = ((mk["volume"]*mk["price"]).cumsum() / mk["volume"].cumsum()).ffill())

        # 重置参数，重新开始波段计算
        if mk['price'][-1] <= mk['vwap'][-1]:
            context.median_start[stock] = num_bar
            context.zz_count[stock] = 0
            context.zz_1_high[stock] = 0.0       

        day_CH_index, day_CH = max(enumerate(mk["price"]), key=operator.itemgetter(1))
        if num_bar <= 30 and num_bar >= 15:
            if num_bar <= 20:
                context.low_first20mins[stock] = mk["price"].min()
            #步步高mode_2a
            if num_bar == 15:
                if ((mk["price"][5:] > mk["vwap"][5:]*0.998).all() \
                    or (len(mk[mk["price"] > mk["vwap"]*0.998])>= num_bar-3 and mk["price"][-5:].max() == mk["price"].max())) \
                    and mk["price"].max() < mk["price"].min()*context.zy_threshold and mk["price"].min() > h['price'][-num_bar - 1]*0.992:
                    order_target(stock, 1)
                    context.cc_avg_price[stock] = mk["price"][-1]
                    print("mode2a_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))
                    print("double check whether it's zuliwei or not, like last day's high or recent high")
                #步步高4.b：低开主多
                elif mk["price"][0] < h['price'][-num_bar - 1] * 0.992 \
                    and (mk["price"][3:] >= mk["vwap"][3:]).all() and day_CH > mk["price"][0]*1.015 and day_CH > h['price'][-num_bar - 1]:
                        order(stock, 1)
                        context.cc_avg_price[stock] = mk["price"][-1]
                        print("mode4b_dikai_pianduo_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))
                        print("double check whether duanxian pianduo or not, buy after 22:05 is another good choice!")
                    
            #步步高3.a：偏多信号之后，小空 + 相对低位10mins局部止跌 + 不破相
            if  mk["price"][:5].max() > mk["price"][0]*1.004 and mk["price"].min() > context.low_first20mins[stock]*0.99 \
                and mk["price"].min() < mk["price"][0]*0.991 \
                and (mk['price'][-10:] < mk["price"][:-10].max()).all() and mk['price'][-1] >= mk["price"][day_CH_index:-10].min() \
                and (mk['price'][-10:] >= mk["price"][day_CH_index:-10].min()*0.998).all() and mk["price"].min() > h['price'][-num_bar - 1]*0.998:
                if context.portfolio.positions[stock].amount <= 1:
                    open_order = get_open_orders(stock)
                    if len(open_order) > 0:
                        cancel_order(open_order[0])
                    order_target(stock, 2)
                    if context.cc_avg_price[stock] < 0.1:
                        context.cc_avg_price[stock] = mk["price"][-1]
                    else:
                        context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                    print("mode_3a_xiaodi_10mins_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))
            #步步高4.a：早盘小空止跌转震荡多 - 小空 + 相对低位11mins止跌 + 破相
            elif mk["price"].min() < mk["price"][0]*0.975 and mk["price"].min() < h['price'][-num_bar - 1]*0.985 \
                and (mk['price'][-11:] > mk["price"][:-11].min()*0.998).all() and mk["price"].max() < mk["price"][0]*1.015 \
                and mk['price'][-11:].max() < mk["price"].min() + (mk["price"].max() - mk["price"].min())*0.3:
                if context.portfolio.positions[stock].amount <= 1:
                    context.xiankong_zd_duo = True
                    open_order = get_open_orders(stock)
                    if len(open_order) > 0:
                        cancel_order(open_order[0])
                    order_target(stock, 2)
                    if context.cc_avg_price[stock] < 0.1:
                        context.cc_avg_price[stock] = mk["price"][-1]
                    else:
                        context.cc_avg_price[stock] = (context.cc_avg_pric[stock] + mk["price"][-1])/2
                    print("mode_4a_kong_10mins_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))
                    print("double check whether it's confirming zhicheng. if so, just do it.")


            if num_bar == 30:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                    print("qiangzhi cancel")

        elif num_bar > 30:
            pre30m_CH_index, pre30m_CH = max(enumerate(mk["price"][:30]), key=operator.itemgetter(1))
            #步步高3.a：偏多信号之后，小空 + 相对低位明确止跌 + 且不破相（不有效破开盘20分钟内的低点）
            # TODO: 长期偏多走稳标的，有破分时但不破相
            if num_bar < 45 and mk["price"][:5].max() > mk["price"][0]*1.004 \
                and mk["price"].min() > context.low_first20mins[stock]*0.99 \
                and mk["price"].min() < mk["price"][0]*0.991 \
                and (mk['price'][-30:] < mk["price"][:-30].max()).all() \
                and (mk['close'][-30:] > mk["close"][day_CH_index:-30].min()*0.998).all() and mk["price"].min() >= h['price'][-num_bar - 1]*0.998:
                if context.portfolio.positions[stock].amount <= 2:
                    open_order = get_open_orders(stock)
                    if len(open_order) > 0:
                        cancel_order(open_order[0])
                    order_target(stock, 3)
                    if context.cc_avg_price[stock] < 0.1:
                        context.cc_avg_price[stock] = mk["price"][-1]
                    else:
                        context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                    print("mode_3a_xiaodi_30mins_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent)) 
            #3.b类先多后调整止跌后的续多机会
            #TODOs: 加一个基础判断，30k 高于均线
            #day_CH_index, day_CH = max(enumerate(mk["price"]), key=operator.itemgetter(1))
            elif num_bar>=45 and num_bar<145: # 22:15~23:55
                mk_30 = mk[-30:]
                # 有中阳以上多趋势 + 维持了一段时间  + 不破相 + 小空调整 + 半小时止跌 + 若低点高于开盘价则要有相对止跌过程 + 当前处于相对低点
                if day_CH > mk["price"][0]*1.015 and day_CH > h['price'][-num_bar - 1]*context.zy_threshold \
                    and len(mk[mk["price"] > mk["vwap"]]) > 10 and mk["price"][day_CH_index:-30].min() > h['price'][-num_bar - 1] \
                    and mk["price"].min() > min(mk["price"][0], h['price'][-num_bar - 1])*0.995 \
                    and mk["price"][day_CH_index:-30].min() < day_CH*0.975 \
                    and (mk['price'][-30:] >= mk["price"][day_CH_index:-30].min()*0.998).all() \
                    and (mk['price'][-30:] < day_CH*0.993).all() \
                    and (len(mk_30[mk_30['price'] > mk_30['vwap']*0.998])>25 or (mk['price'][-30:] < (mk["price"][day_CH_index:-30].min() + day_CH)/2).all()):
                    # solve confict between 3a and yiboduo_zz using pianduo_tiaozheng
                    if context.portfolio.positions[stock].amount <= 2 and not context.pianduo_tiaozheng:
                        open_order = get_open_orders(stock)
                        if len(open_order) > 0:
                            cancel_order(open_order[0])
                        order_target(stock, 3)
                        if context.cc_avg_price[stock] < 0.1:
                            context.cc_avg_price[stock] = mk["price"][-1]
                        else:
                            context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                        print("mode_3b_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent)) 
                        print("double check whether it's weizhi zhendang_duo and already above zhongyang or it. if so, just zhiying.")
                        if (mk_30['price'] > mk_30['vwap']).all():
                            context.pianduo_tiaozheng = True
                #2.b 介入3b和2a之间，震荡小多，站稳分时不满足2a，涨幅不满足3b：15分钟之后的突破多 + 突破幅度不大+走稳15分钟开始做多
                elif num_bar < 120 and pre30m_CH*1.002 <  day_CH and day_CH_index - pre30m_CH_index > 15 \
                    and (mk['price'][-30:] > h['price'][-num_bar - 1]).all() and mk['price'][:30].max() < h['price'].min()*context.zy_threshold \
                    and (mk['price'][-5:] >= mk['vwap'][-5:]).all() and (mk['price'][-10:] < day_CH).all() \
                    and num_bar - day_CH_index < 20 and len(mk[mk['price'] > mk['vwap']*0.998]) > num_bar - 12:
                     if context.portfolio.positions[stock].amount <= 2:
                        open_order = get_open_orders(stock)
                        if len(open_order) > 0:
                            cancel_order(open_order[0])
                        order_target(stock, 3)
                        if context.cc_avg_price[stock] < 0.1:
                            context.cc_avg_price[stock] = mk["price"][-1]
                        else:
                            context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                        print("mode_2b_zhendang_duo_tupo_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent)) 
                        print("double check whether it's xiaofu_zhendang_duo or not. if so, just zhiying.")
                        print("double check whether it's 2a already. if so, no long.")                      

            #4.a 空止跌做震荡多
            if num_bar < 185 and mk["price"].min() < mk["price"][0]*0.985 and mk["price"].min() < h['price'][-num_bar - 1]*0.985 \
                and (mk['price'][-30:] > mk["price"][day_CH_index:-30].min()*0.998).all() and mk['price'][-1] < mk["price"][0] \
                and mk['price'][-30:].max() < mk["price"].min() + (day_CH_index - mk["price"].min())*0.45 \
                and mk["price"].max() < mk["price"][0]*1.015:
                if context.portfolio.positions[stock].amount <= 2:
                    context.xiankong_zd_duo = True
                    open_order = get_open_orders(stock)
                    if len(open_order) > 0:
                        cancel_order(open_order[0])
                    order_target(stock, 3)
                    if context.cc_avg_price[stock] < 0.1:
                        context.cc_avg_price[stock] = mk["price"][-1]
                    else:
                        context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                    print("mode_4a_kong_30mins_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent)) 
                    print("double check whether it's confirming zhicheng or it. if so, after 23:00 and second kong is a good choice for long.")


            #3.c类盘中相对低位止跌做多
            if num_bar>=120 and num_bar<230:#23：30~13：20
                # 早盘有偏多信号 + 长期低于早盘价相对低位止跌 + 不破相 + 近期相对低位止跌30mins + 30k偏多走稳
                if mk["price"][:20].max() > mk["price"][0] and day_CH < mk["price"][:20].max()*1.015 \
                    and len(mk[mk["price"] > context.low_first20mins[stock]*0.99])> num_bar-3 and mk["price"].min() > h['price'][-num_bar - 1]*0.995 \
                    and (mk['price'][-30:] >= mk["price"][day_CH_index:-30].min()*0.998).all() \
                    and mk['price'][-30:].max() < (mk["price"][day_CH_index:-30:].min() + day_CH)/2:
                    if context.portfolio.positions[stock].amount <= 3:
                        open_order = get_open_orders(stock)
                        if len(open_order) > 0:
                            cancel_order(open_order[0])
                        order_target(stock, 4)
                        if context.cc_avg_price[stock] < 0.1:
                            context.cc_avg_price[stock] = mk["price"][-1]
                        else:
                            context.cc_avg_price[stock] = (context.cc_avg_price[stock] + mk["price"][-1])/2
                        print("mode_3c_zhidie_kaicang %s at price: %f with percent %f"%(stock, mk["price"][-1], context.percent))

            #13：45~13:55定点止损
            mk_l20 = mk[-20:]
            if num_bar >= 255 and num_bar < 265 and context.portfolio.positions[stock].amount>0  and len(mk_l20[mk_l20["price"] > mk_l20["vwap"]*0.998]) < 16:
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                order_target(stock, 0)
                res = (mk["price"][-1]-context.cc_avg_price[stock])/context.cc_avg_price[stock]*100
                print("dingdian: 13:45 zhishun %s at price: %f with profit %.1f"%(stock, mk["price"][-1], res))
                print("double check whether it's creating xindi. if so, just zhishun; if not, wait until 14:00 and check 30k.")

                context.cc_avg_price[stock] = 0.0

            #止损：破相后反弹止损
            if context.portfolio.positions[stock].amount>0 and context.xiankong_zd_duo == False \
                and (mk["price"].min() < context.low_first20mins[stock]*0.99 and context.low_first20mins[stock] < h['price'][-num_bar - 1]*1.015) \
                and mk_l20["price"][-1] > (mk['price'][20:].min() + mk['price'].max())/2 and mk_l20["price"][-1] < mk_l20["price"].max():
                open_order = get_open_orders(stock)
                if len(open_order) > 0:
                    cancel_order(open_order[0])
                res = (mk["price"][-1]-context.cc_avg_price[stock])/context.cc_avg_price[stock]*100
                order_target(stock, 0)
                print("changqi_piankong_fantan_zhishun %s at price %.2f with profit %.1f"%(stock, mk["price"][-1], res))
                context.cc_avg_price[stock] = 0.0

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
                            res = (mk["price"][-1]-context.cc_avg_price[stock])/context.cc_avg_price[stock]*100
                            order_target(stock, 0)
                            print("zhiying %s at price: %f, with profit %.1f"%(stock, mk["price"][-1], res))
                            context.cc_avg_price[stock] = 0.0
                    else:
                        if context.portfolio.positions[stock].amount>0 and context.zz_count[stock] >= context.zz_count_max \
                            and median_CH > h['price'][len(h) - num_bar - 1]*1.055 and mk["price"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                            open_order = get_open_orders(stock)
                            if len(open_order) > 0:
                                cancel_order(open_order[0])
                            order_target(stock, 0)
                            res = (mk["price"][-1]-context.cc_avg_price[stock])/context.cc_avg_price[stock]*100
                            print("zhiying %s at price: %f with profit %.1f"%(stock, mk["price"][-1], res))
                            context.cc_avg_price[stock] = 0.0

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

        h = data.history(stock, fields=['open', 'high', 'low', 'close', 'price', 'volume'], bar_count=20, frequency="1m")
        h = h.tz_convert('US/Eastern')
        mk = h[h.index.date==context.cur_td]
        mk = mk.assign(vwap = ((mk["volume"]*mk["price"]).cumsum() / mk["volume"].cumsum()).ffill())
        # 有偏多信号;较为初略，不排除震荡标的
        #if (mk["price"] > mk["price"][0]).any() and (mk["price"] > mk["vwap"]).any():
        context.position_stock.append(stock)
        context.median_start[stock] = 1
        context.zz_count[stock] = 0
        context.zz_1_high[stock] = 0.0
        context.cc_avg_price[stock] = 0.0

    if len(context.position_stock) > 0:
        context.percent = 1/len(context.position_stock)
    print(context.position_stock)