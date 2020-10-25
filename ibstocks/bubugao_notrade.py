"""
Intraday bubugao basic signals with trading option, corresponding parameters and usage:
1. PARAM: zz_count_max & above_zz_1 - control three ZY modes:
    1.1 close position at first zz( zz_count_max=1&above_zz_1=0)
    1.2 close position at second zz( zz_count_max=2&above_zz_1=0)
    1.3 close position x above first zz price(zz_count_max=1&>above_zz_1 = x)
2. Other key PARAMs: duobeili_threshold, dst_long_pos, zy_threshold
    2.1 duobeili_threshold - decide a valid long trend, which is equity dependent.
3. internal VARIABLE: zuliwei - other ZY related parameters：
    2.1 the fourth ZY mode with zuliwei - 根据涨幅、日内波段和背离分时状况选择：+-1%以内，+3%，+5~10%止盈
4. common internal VARIABLES(close prices): first20_low, yestoday_close, (day_low, day_high), (poxiang: day_low < first20_low*0.99)
5. zz & zd related parameters, which are patial values from the latest median_start: 
    5.1 median_start - the latest starting of long strategy
    5.2 zz_count, zz_1_high
6. position related VARIABLEs:
    6.1 actual_long_pos - it's an internal variable for self.pos
    6.2 partial_pos_count - info manual interupt

The Signals: 
1. 2a, 2b --- duo step by step 
2. 3a, 3b, 3c --- duo with small or long zhidie period 
3. 4a 4b --- dikai or xiankong

The trading logic:
1. kaicang
2. zhiying
3. zhishun
NOTE: Variable zz_count will reset for new long trend
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
import time
import winsound
#import bs_vn_base as bs

#partial bubugao strategy with close fuction while zhizhang
class BubugaoSignal(CtaTemplate):
    """"""
    author = "boyscout"

    signal_log = ''
    sh = None

    #Parameters
    zz_count_max = 1
    above_zz_1 = 0.0#1.001
    duobeili_threshold = 1.015 # 美股个股、取货品种都各不相同
    dst_long_pos = 0
    zy_threshold = 1.045 # zhongyang
    cover_before_close = True
    last_15mins_bar_index = 375

    long_avg_price = 0.0
    xiankong_zd_duo = False
    pianduo_tiaozheng = False

    #Variables
    median_start = 1
    zz_count = 0
    zz_1_high = 0.0
    #0：初始化状态，无多空信号； 2: bubugao_mode2； 2<<1: 多转滞涨
    #3: bubugao_mode3a_10min; 3<<2: bubugao_mode3a_30min; 3<<3: bubugao_mode3a_zhizhang
    #-1：背离空破相，偏空趋势；-1<<1: 空止跌（结合30k，潜在买点）
    # internal
    #bar_30m = []
    strategies = {}
    first20_low = 0.0
    yestoday_close = 0.0 
    isready_zd_duo = False
    # position related 
    #partial_pos_count = 0
    #actual_long_pos = 0

    parameters = ["zz_count_max", "above_zz_1", "duobeili_threshold", "dst_long_pos", "zy_threshold", "cover_before_close", "last_15mins_bar_index"]
    variables = ["median_start", "zz_count", "zz_1_high", "trending"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super(BubugaoSignal, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        #Step-1: minute generator creation
        #self.bg = BarGenerator(self.on_bar, 15, self.on_30min_bar)
        self.bg = BarGenerator(self.on_bar)
        self.bars = pd.DataFrame(columns=('datetime','open','high','low','close','volume'))
        # signal file
        #curDay = time.strftime('%Y%m%d', time.localtime(time.time()))
        self.signal_log = 'E://proj-futures/logs/' + strategy_name + '_' + vt_symbol.split('.')[0] + '.log'

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
        cur_dt = bar.datetime.date()

        #TODOs: get cur_bar 391 using history interface once network breakup or startup among trading
        # internal logic is independent although computation comsuming
        df_bar = pd.DataFrame({'datetime':[bar.datetime], 'open':[bar.open_price], \
            'high':[bar.high_price], 'low':[bar.low_price], 'close':[bar.close_price], 'volume':[bar.volume]})

        self.bars = self.bars.append(df_bar, ignore_index=True)

        mk_390 = self.bars.set_index('datetime')
        mk = mk_390[mk_390.index.date==cur_dt]

        num_bar = len(mk)

        if self.inited:
            self.write_log("API_STABILITY_MONITOR: %f, %d, num_bar = %d"%(bar.close_price, bar.volume, num_bar))

        if num_bar == 1 and len(mk_390)>300:
            self.yestoday_close = mk_390['close'][-2]
            self.bar_30m = mk_390['close'][-300:-1:30]

            self.sh = open(self.signal_log, mode='a+')

        # 尾盘强制平仓
        if num_bar == self.last_15mins_bar_index and self.cover_before_close:
            if self.long_avg_price > 0.1 and self.sh:
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                self.sh.write("%s: weipan_pingcang with profit %.1f at price %.2f\n"%(mk.index[-1], res, mk["close"][-1]))
                self.sh.close()
                self.sh = None
            self.long_avg_price = 0.0
            self.strategies.clear()
                #winsound.PlaySound(bs.SOUND_NOTICE_ORDER, winsound.SND_FILENAME)
            return

        #TODO: 30k及其ma5和ma10
        mk = mk.assign(vwap = ((mk["volume"]*mk["close"]).cumsum() / mk["volume"].cumsum()).ffill())
        # 重置参数，重新开始波段计算
        if num_bar == 1 or mk['close'][-1] <= mk['vwap'][-1]:
            self.median_start = num_bar
            self.zz_count = 0
            self.zz_1_high = 0.0

        day_CH_index, day_CH = max(enumerate(mk["close"]), key=operator.itemgetter(1))
        # 早盘：2a|3a|4a|4b - 四种模式具有互斥性，最多执行一种策略
        if num_bar >= 15 and num_bar <= 30:
            if num_bar <= 20:
                self.first20_low = mk["close"].min()
            # Signal：mode_2a
            if num_bar == 15:
                if ((mk["close"][5:] > mk["vwap"][5:]*0.998).all() \
                    or (len(mk[mk["close"] > mk["vwap"]*0.998])>= num_bar-3 and mk["close"][-5:].max() == mk["close"].max())) \
                    and day_CH < mk["close"].min()*self.zy_threshold and mk["close"][3:].min() > self.yestoday_close*0.992:
                    #self.trending = 2
                    self.long_avg_price = mk["close"][-1]
                    self.strategies['2a'] = mk["close"][-1]
                    if self.sh:
                        self.sh.write("%s: SIGNAL_mode2a_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        self.sh.write("Double check whether it's zuliwei or not, like last day's high or recent high.\n")
                    #self.write_log("SIGNAL_buy_bubugao_mode2_2145 at price %.2f"%(mk["close"][-1]))
                # Signal：mode_4b
                elif mk["close"][0] < self.yestoday_close * 0.992 \
                    and (mk["close"][3:] >= mk["vwap"][3:]).all() and day_CH > mk["close"][0]*self.duobeili_threshold and day_CH > self.yestoday_close:
                    self.long_avg_price = mk["close"][-1]
                    self.strategies['4b'] = mk["close"][-1]
                    if self.sh:
                        self.sh.write("%s: SIGNAL_mode4b_dikai_pianduo_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        self.sh.write("Double check whether duanxian pianduo or not, buy after 22:05 is another good choice!\n")

            # Signal：mode_3a_0
            # 小高偏多信号 + 小空局部止跌10分钟 + 最低点约高于昨日收盘
            if  mk["close"][:5].max() > mk["close"][0]*1.004 and mk["close"].min() > self.first20_low*0.99 \
                and mk["close"].min() < mk["close"][0]*0.991 \
                and (mk['close'][-10:] < mk["close"][:-10].max()).all() and mk['close'][-1] >= mk["close"][day_CH_index:-10].min() \
                and (mk['close'][-10:] >= mk["close"][day_CH_index:-10].min()*0.998).all() and mk["close"].min() > self.yestoday_close*0.998:
                self.long_avg_price = mk["close"][-1]
                self.strategies['3a_0'] = mk["close"][-1]
                if self.sh:
                    self.sh.write("%s: SIGNAL_mode3a_xiaodi_10mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                    if '2a' in self.strategies:
                        self.sh.write("WARNING: zhuan_kong_jubu_zhidie_3a after xiaoduo_2a, had better ignore the 3a signal!\n")
            # Signal：mode_4a_0
            # 早盘小空止跌转震荡多 - 小空 + 相对低位11mins止跌 + 破相
            elif mk["close"].min() < mk["close"][0]*0.975 and mk["close"].min() < self.yestoday_close*0.985 \
                and (mk['close'][-11:] > mk["close"][:-11].min()*0.998).all() and day_CH < mk["close"][0]*self.duobeili_threshold \
                and mk['close'][-11:].max() < mk["close"].min() + (day_CH - mk["close"].min())*0.3:
                self.xiankong_zd_duo = True
                self.long_avg_price = mk["close"][-1]
                self.strategies['4a_0'] = mk["close"][-1]
                if self.sh:
                    self.sh.write("%s: SIGNAL_mode4a_kong_10mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))              

        #盘中：开仓-2b;3a,3b,3c;4a; 止损-定点和破相反弹止损；止盈-一波多或两波多滞涨止盈
        elif num_bar > 30:
            pre30m_CH_index, pre30m_CH = max(enumerate(mk["close"][:30]), key=operator.itemgetter(1))
            # Signal：mode_3a_1
            # 偏多信号之后，小空 + 相对低位明确止跌 + 且不破相（不有效破开盘20分钟内的低点）
            if num_bar < 45 and mk["close"][:5].max() > mk["close"][0]*1.004 \
                and mk["close"].min() > self.first20_low*0.99 \
                and mk["close"].min() < mk["close"][0]*0.991 \
                and (mk['close'][-30:] < mk["close"][:-30].max()).all() \
                and (mk['close'][-30:] > mk["close"][day_CH_index:-30].min()*0.998).all() and mk["close"].min() >= self.yestoday_close*0.998:
                if '3a_1' in self.strategies:
                    pass
                else:
                    self.strategies['3a_1'] = mk["close"][-1]
                    if self.long_avg_price < 0.1:
                        self.long_avg_price = mk["close"][-1]
                    else:
                        self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                    if self.sh:
                        self.sh.write("%s: SIGNAL_mode3a_1_xiaodi_30mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        if '3a' in self.strategies:
                            self.sh.write("NOTE: mode_3a_1 after mode_3a, ignore it if necessary!\n")
            #TODOs: 加一个基础判断，30k 高于均线
            elif num_bar>=45 and num_bar<145: # 22:15~23:55
                mk_30 = mk[-30:]
                # Signal：mode_3b
                # 有中阳以上多趋势 + 维持了一段时间  + 不破相 + 小空调整 + 半小时止跌 + 若低点高于开盘价则要有相对止跌过程 + 当前处于相对低点
                if day_CH > mk["close"][0]*1.015 and day_CH > self.yestoday_close*self.zy_threshold \
                    and len(mk[mk["close"] > mk["vwap"]]) > 10 and mk["close"][day_CH_index:-30].min() > self.yestoday_close \
                    and mk["close"].min() > min(mk["close"][0], self.yestoday_close)*0.995 \
                    and mk["close"][day_CH_index:-30].min() < day_CH*0.975 \
                    and (mk['close'][-30:] >= mk["close"][day_CH_index:-30].min()*0.998).all() \
                    and (mk['close'][-30:] < day_CH*0.993).all() \
                    and (len(mk_30[mk_30['close'] > mk_30['vwap']*0.998])>25 or (mk['close'][-30:] < (mk["close"][day_CH_index:-30].min() + day_CH)/2).all()):
                    # solve confict between 3a and yiboduo_zz using pianduo_tiaozheng
                    if '3b' in self.strategies:
                        pass
                    else:
                        self.strategies['3b'] = mk["close"][-1]
                        if self.long_avg_price < 0.1:
                            self.long_avg_price = mk["close"][-1]
                        else:
                            self.long_avg_price = sum(self.strategies.values())/len(self.strategies)

                        if (mk_30['close'] > mk_30['vwap']).all():
                            self.pianduo_tiaozheng = True

                        if self.sh:
                            self.sh.write("%s: SIGNAL_mode3b_duotiaozheng_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                            self.sh.write("Double check whether it's weizhi zhendang_duo and already above zhongyang or it. if so, just zhiying.\n")
                # Signal：mode_2b
                # 介入3b和2a之间，震荡小多，站稳分时不满足2a，涨幅不满足3b：15分钟之后的突破多 + 突破幅度不大+走稳15分钟开始做多
                elif num_bar < 120 and pre30m_CH*1.002 <  day_CH and day_CH_index - pre30m_CH_index > 15 \
                    and (mk['close'][-30:] > self.yestoday_close).all() and mk['close'][:30].max() < mk['close'].min()*self.zy_threshold \
                    and (mk['close'][-5:] >= mk['vwap'][-5:]).all() and (mk['close'][-10:] < day_CH).all() \
                    and num_bar - day_CH_index < 20 and len(mk[mk['close'] > mk['vwap']*0.998]) > num_bar - 12:
                    if '2b' in self.strategies:
                        pass
                    else:
                        self.strategies['2b'] = mk["close"][-1]
                        if self.long_avg_price < 0.1:
                            self.long_avg_price = mk["close"][-1]
                        else:
                            self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                        if self.sh:
                            self.sh.write("%s: SIGNAL_mode2b_zhendang_duo_tupo_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                            self.sh.write("Double check whether it's 2a already; if so, no more buy.\n")
                            if '2a' in self.strategies or '3b' in self.strategies:
                                self.sh.write("NOTE: mode_2b after mode_2a or mode_3b, ignore it if necessary!\n")
            # Signal：mode_4a_1
            # 空止跌做震荡多或反弹
            if num_bar < 185 and mk["close"].min() < mk["close"][0]*0.985 and mk["close"].min() < self.yestoday_close*0.985 \
                and (mk['close'][-30:] > mk["close"][day_CH_index:-30].min()*0.998).all() and mk['close'][-1] < mk["close"][0] \
                and mk['close'][-30:].max() < mk["close"].min() + (day_CH_index - mk["close"].min())*0.45 \
                and day_CH < mk["close"][0]*1.015:
                self.xiankong_zd_duo = True
                if '4a_1' in self.strategies:
                    pass
                else:
                    self.strategies['4a_1'] = mk["close"][-1]
                    if self.long_avg_price < 0.1:
                        self.long_avg_price = mk["close"][-1]
                    else:
                        self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                    if self.sh:
                        self.sh.write("%s: SIGNAL_mode4a_1_kong_30mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                        self.sh.write("Double check whether it's confirming zhicheng or it. if so, after 23:00 and second kong is a good choice for long.\n")
                        if '4a' in self.strategies:
                            self.sh.write("WARNING: mode_4a_1 after mode_4a, ignore it if necessary!\n")
            # Signal：mode_3c
            # 盘中相对低位止跌做多
            if num_bar>=120 and num_bar<230:#23：30~13：20
                # 早盘有偏多信号 + 长期低于早盘价相对低位止跌 + 不破相 + 近期相对低位止跌30mins + 30k偏多走稳
                if mk["close"][:20].max() > mk["close"][0] and day_CH < mk["close"][:20].max()*1.015 \
                    and len(mk[mk["close"] > self.first20_low*0.99])> num_bar-3 and mk["close"].min() > self.yestoday_close*0.995 \
                    and (mk['close'][-30:] >= mk["close"][day_CH_index:-30].min()*0.998).all() \
                    and mk['close'][-30:].max() < (mk["close"][day_CH_index:-30:].min() + day_CH)/2:
                    if '3c' in self.strategies:
                        pass
                    else:
                        self.strategies['3c'] = mk["close"][-1]
                        if self.long_avg_price < 0.1:
                            self.long_avg_price = mk["close"][-1]
                        else:
                            self.long_avg_price = sum(self.strategies.values())/len(self.strategies)
                        if self.sh:
                            self.sh.write("%s: SIGNAL_mode3c_xingduidi_30mins_zhidie_kaicang at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                            self.sh.write("Double check 30k is above MA.\n")

            # Signal：zhishun
            # 13：45~13:55定点止损
            mk_l20 = mk[-20:]
            if num_bar >= 255 and num_bar < 265 and self.long_avg_price >0.1  and len(mk_l20[mk_l20["close"] > mk_l20["vwap"]*0.998]) < 16:
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                if self.sh:
                    self.sh.write("%s: SIGNAL_dingdian_zhishun_1345 at price %.2f with profit: %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                    self.sh.write("Double check whether it's creating xindi. if not, wait until 14:00 and check 30k.\n")
                self.long_avg_price = 0.0
            # 破相后反弹时局部滞涨止损
            if self.long_avg_price >0.1 and self.xiankong_zd_duo == False \
                and (mk["close"].min() < self.first20_low*0.99 and self.first20_low < self.yestoday_close*1.015) \
                and mk_l20["close"][-1] > (mk['close'][20:].min() + day_CH)/2 \
                and mk_l20["close"][-1] < mk_l20["close"].max(): #TODO: 目前只要一分钟滞涨就止损
                res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                if self.sh:
                    self.sh.write("%s: SIGNAL_changqi_piankong_fantan_zhishun at price %.2f with profit %.1f\n"%(mk.index[-1], mk["close"][-1], res))
                self.long_avg_price = 0.0

            # 止盈
            if num_bar - self.median_start > 30:
                median_CH_index, median_CH = max(enumerate(mk["close"][self.median_start:]), key=operator.itemgetter(1))
                median_h_zz = mk[self.median_start+median_CH_index:]
                if len(median_h_zz) >= 30 and len(median_h_zz) < 60 and median_CH > median_h_zz["vwap"][0] * 1.015:
                    if len(median_h_zz) == 30:
                        self.zz_count += 1
                        if self.zz_count == 1:
                            self.zz_1_high = median_CH
                        #case1&2: close long while zz 1 or 2 times
                        if self.long_avg_price > 0.1 and self.zz_count >= self.zz_count_max \
                            and median_CH > self.yestoday_close*1.055 and mk["close"][-1] > (mk["vwap"][-1] + (median_CH - mk["vwap"][-1])*0.5):
                            res = (mk["close"][-1]-self.long_avg_price)/self.long_avg_price*100
                            if self.sh:
                                self.sh.write("%s: SIGNAL_zz_zhiying: %d times, at price %.2f with profit %.1f\n"%(mk.index[-1], \
                                    self.zz_count, mk["close"][-1], res))
                                self.sh.write("Double check whrther it's a strong trending; if so, wait second ZZ signal.\n")
                            if self.zz_count >= 2: # print two level zz signals
                                self.long_avg_price = 0.0

            '''
            # Signal poxiang zhendang or kong
            if self.trending != -1 and mk["close"][-1] <= self.first20_low*0.99:
                self.trending = -1
                #Two sell strategy: 破昨日收盘价则为反弹止跌，14：00之前第二波多不排除分时之下滞涨止损；
                # 若先中大多调整至昨日收盘附近则第二波多不排除日内新高滞涨止盈
                if self.sh:
                    self.sh.write("%s: SIGNAL_sell_kong_1percent_below_first20_low at price %.2f\n"%(mk.index[-1], mk["close"][-1]))
                    self.sh.write("%s: kong_zhishun strategies: pingcang at the second zz, which is possibly below vwap or with new intraday high\n"%(mk.index[-1]))
                self.write_log("SIGNAL_sell_kong_1percent_below_first20_low at price %.2f"%(mk["close"][-1]))
                self.write_log("kong_zhishun strategies: pingcang at the second zz, which is possibly below vwap or with new intraday high")
            '''
        
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
