"""
Intraday close position strategy without no higher price within 30-mins, two parameters zz_count_max & above_zz_1
control three close modes:
1: close position at first zz( zz_count_max=1&above_zz_1=0)
2: close position at second zz( zz_count_max=2&above_zz_1=0)
3: close position x above first zz price(zz_count_max=1&>above_zz_1 = x)
Another parameter "duobeili_threshold" is used to decide a valid long trend, which is equity dependent.
NOTE: Variable zz_count will reset for new long trend
TODO: limit order with given price
trick: 单向滞涨止盈策略系统支持不好，采用另类方式实现，就是滞涨开空，实际上就跟手动开多的仓位相抵消了。
具体改动涉及：init_pos，和pos的判断
"""

from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager
)

import pandas as pd
import operator
from time import time
import winsound
import bs_vn_base as bs

#partial bubugao strategy with close fuction while zhizhang
class BubugaoZZ_short(CtaTemplate):
    """"""
    author = "boyscout"

    zz_count_max = 1
    above_zz_1 = 0.0#1.001
    duobeili_threshold = 1.015 # 美股个股、取货品种都各不相同
    init_long_pos = 0 #实盘用：手动开仓仓位设定，从而启动单向滞涨止盈策略
    cover_before_close = True
    last_15mins_bar_index = 375
    backtest_flag = False #回测用

    median_start = 1
    zz_count = 0
    zz_1_high = 0.0
    partial_pos_count = 0

    parameters = ["zz_count_max", "above_zz_1", "duobeili_threshold", "init_long_pos", "cover_before_close", "last_15mins_bar_index", "backtest_flag"]
    variables = ["median_start", "zz_count", "zz_1_high", "partial_pos_count"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super(BubugaoZZ_short, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        #Step-1: minute generator creation
        #self.bg = BarGenerator(self.on_bar, 15, self.on_15min_bar)
        self.bg = BarGenerator(self.on_bar)
        # ArrayManager是一个定制化的时间序列管理器，一无时间信息，二固定长度；不符合我的日内策略需求，自定义bars
        #self.am = ArrayManager(391)
        self.bars = pd.DataFrame(columns=('datetime','open','high','low','close','volume'))

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(1) #载入1天的历史数据,实盘中用于中断和盘中启动

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")
        # 为人工开仓设置初始仓位，开启单向滞涨止盈策略

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        #Step-1_1: 聚合呈minute数据
        self.bg.update_tick(tick)
        self.put_event()

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        #self.bg.update_bar(bar) #如有必要再合成5-minute的bar
        #Step-2:策略实体
        # canel未成交订单
        #self.cancel_all()
        #am = self.am
        #am.update_bar(bar)
        if self.inited:
            self.write_log("API_STABILITY_MONITOR: %f, %d, num_bar = %d"%(bar.close_price, bar.volume, num_bar))

        cur_dt = bar.datetime.date()

        #TODOs: get cur_bar 391 using history interface once network breakup or startup among trading
        # internal logic is independent although computation comsuming
        df_bar = pd.DataFrame({'datetime':[bar.datetime], 'open':[bar.open_price], \
            'high':[bar.high_price], 'low':[bar.low_price], 'close':[bar.close_price], 'volume':[bar.volume]})

        self.bars = self.bars.append(df_bar, ignore_index=True)

        mk = self.bars.set_index('datetime')
        mk = mk[mk.index.date==cur_dt]

        num_bar = len(mk)

        # 回测设置：早盘固定时间开仓，滞涨止盈回测用
        if self.backtest_flag and num_bar == 15:
            self.buy(mk["close"][-1], 1, True)
            self.write_log("simulated buy at price %.2f for backtesting zz"%(mk["close"][-1]))

        # 市价单3分钟只是部分成交则提醒人工干预
        if self.pos < 0 and self.pos != -self.init_long_pos:
            self.partial_pos_count += 1
            if self.partial_pos_count > 3:
                self.write_log("市价单3分钟未成交，提醒人工介入!")
                winsound.PlaySound(bs.SOUND_MANUAL_INTERUPT, winsound.SND_FILENAME)
        else:
            self.partial_pos_count = 0

        # 尾盘强制平仓
        if num_bar >= self.last_15mins_bar_index and self.cover_before_close:
            if self.pos != -self.init_long_pos and self.init_long_pos > 0:
                self.cancel_all() # 取消未成交订单，重新下单
                cur_stop_pos = self.init_long_pos + self.pos
                assert(cur_stop_pos > 0)
                self.sell(mk["close"][-1], cur_stop_pos, True)
                self.write_log("weipan pingcang %d at price %.2f"%(cur_stop_pos, mk["close"][-1]))
                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
            return
        
        mk = mk.assign(vwap = ((mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()).ffill())
        # 重置参数，重新开始波段计算
        if num_bar == 1 or mk['close'][-1] <= mk['vwap'][-1]:
            self.median_start = num_bar
            self.zz_count = 0
            self.zz_1_high = 0.0

        if num_bar > 30:
            # case3：首次滞涨后突破止盈（微突破不止盈，可以考虑协同第二波多滞涨止盈一起工作）
            if self.above_zz_1 > 0.00001 and self.zz_count == 1 and mk["close"][-1] > self.zz_1_high*(1.0+self.above_zz_1):
                self.write_log("ZZ %d times from %d, close long at %d with price: %.2f above zz_1_high: %.2f" \
                    %(self.zz_count, self.median_start, num_bar, mk["close"][-1], self.zz_1_high))
                if self.pos == 0 and self.init_long_pos > 0:
                    self.cancel_all()
                    self.sell(mk["close"][-1], abs(self.init_long_pos), True)
                    #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
                self.zz_count= self.zz_count_max + 1 #disable zz modes

            if num_bar - self.median_start > 30:
                median_CH_index, median_CH = max(enumerate(mk["close"][self.median_start:]), key=operator.itemgetter(1))
                median_h_zz = mk[self.median_start+median_CH_index:]
                if len(median_h_zz) >= 30 and len(median_h_zz) < 60 and median_CH > median_h_zz["vwap"][0] * self.duobeili_threshold:
                    if len(median_h_zz) == 30:
                        self.zz_count += 1
                        if self.zz_count == 1:
                            self.zz_1_high = median_CH
                        #case1&2: close long while zz 1 or 2 times
                        if self.zz_count >= self.zz_count_max and (self.above_zz_1 > -0.00001 and self.above_zz_1 < 0.00001):
                            self.write_log("ZZ %d times from %d, suggest PC at %d with price %.2f"%(self.zz_count, self.median_start, num_bar, mk["close"][-1]))
                            if self.pos == 0 and self.init_long_pos > 0 \
                                and mk["close"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                                self.cancel_all()
                                self.sell(mk["close"][-1], abs(self.init_long_pos), True)
                                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
                    elif len(median_h_zz) > 30:
                        #The backup logic for unexpected network breakup while len(median_h_zz) == 30, which lasts for 30 minutes,
                        #Meanwhile it's used for sell with limit price.
                        #case1&2: close long while zz 1 or 2 times
                        if self.zz_count >= self.zz_count_max and (self.above_zz_1 > -0.00001 and self.above_zz_1 < 0.00001):
                            if self.pos == 0 and self.init_long_pos > 0 \
                                and mk["close"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                                self.cancel_all()
                                self.sell(mk["close"][-1], abs(self.init_long_pos), True)
                                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
        
    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        self.put_event()

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        self.put_event()

'''
    def test_market_order(self):
        """"""
        self.buy(self.last_tick.limit_up, 1)
        self.write_log("执行市价单测试")

    def test_limit_order(self):
        """"""
        self.buy(self.last_tick.limit_down, 1)
        self.write_log("执行限价单测试")

    def test_stop_order(self):
        """"""
        self.buy(self.last_tick.ask_price_1, 1, True)
        self.write_log("执行停止单测试")

    def test_cancel_all(self):
        """"""
        self.cancel_all()
        self.write_log("执行全部撤单测试")
'''
