import json, time, logging
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

APP_KEY     = "YOUR_APP_KEY"
APP_SECRET  = "YOUR_APP_SECRET"
ACCOUNT_NO  = "YOUR_ACCOUNT_NO"
ENV         = "paper"
SEED_CAPITAL = 10_000_000

UNIVERSE     = "volume"
TOP_N        = 50
PORTFOLIO    = 8
MIN_PRICE, MAX_PRICE = 3000, 500000
EXCLUDE_ETF  = True

STRATEGY     = "momentum_ma"  # options: momentum, disparity, golden_cross, consecutive, momentum_ma, pullback_momentum
MOM_PERIOD, MOM_BUY, MOM_SELL = 60, 5.0, -5.0
DISP_PERIOD, DISP_OVERSOLD, DISP_OVERBOUGHT = 20, 90.0, 110.0
MA_FAST, MA_SLOW = 5, 20
CONSEC_BUY, CONSEC_SELL = 5, 5

INVEST_RATIO     = 0.95
MIN_STRENGTH     = 0.5
STOP_LOSS_PCT    = -7.0
TAKE_PROFIT_PCT  = 15.0

LOOP_INTERVAL = 60
MARKET_OPEN, MARKET_CLOSE = "09:00", "15:20"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("autotrader")

DOMAIN = {"paper": "https://openapivts.koreainvestment.com:29443",
          "real": "https://openapi.koreainvestment.com:9443"}
TR = {"paper": {"buy": "VTTC0802U", "sell": "VTTC0801U", "balance": "VTTC8434R",
                "psbl": "VTTC8908R", "ccld": "VTTC8001R", "cancel": "VTTC0803U"},
      "real":  {"buy": "TTTC0802U", "sell": "TTTC0801U", "balance": "TTTC8434R",
                "psbl": "TTTC8908R", "ccld": "TTTC8001R", "cancel": "TTTC0803U"}}
BASE = DOMAIN[ENV]
APP_KEY = APP_KEY.strip()
APP_SECRET = APP_SECRET.strip()
_acc = ACCOUNT_NO.replace("-", "").strip()
CANO, ACNT = _acc[:8], (_acc[8:] or "01")
TOKEN_FILE = Path(__file__).parent / ".token.json"
RECORD_FILE = Path(__file__).parent / "TRADING_LOG.md"
ETF_KW = ("KODEX","TIGER","KBSTAR","ARIRANG","HANARO","KOSEF","SOL ","ACE ","PLUS ","RISE",
          "WON ","FOCUS","히어로즈","ETN","레버리지","인버스","채권","혼합","선물","TR ")

_equity_hist = []
_trades = []
_prev_holdings = {}
_pending_buy_reason = {}
_pending_sell_reason = {}
_baseline_done = False

def get_token():
    if TOKEN_FILE.exists():
        try:
            c = json.loads(TOKEN_FILE.read_text())
            if c.get("env") == ENV and c.get("expire", 0) > time.time():
                return c["token"]
        except Exception:
            pass
    r = requests.post(f"{BASE}/oauth2/tokenP", json={
        "grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(
            f"토큰 발급 실패 [{r.status_code}]: {r.text}\n"
            f"→ ENV='{ENV}' 입니다. paper면 '모의투자' 전용 APP_KEY/SECRET 인지,\n"
            f"   그리고 키에 공백/오타가 없는지 확인하세요. (과다 호출 시 몇 분 뒤 재시도)")
    tok = r.json()["access_token"]
    TOKEN_FILE.write_text(json.dumps({"env": ENV, "token": tok, "expire": time.time() + 23*3600}))
    return tok

TOKEN = None
def headers(tr_id, hkey=None):
    h = {"content-type": "application/json; charset=utf-8", "authorization": f"Bearer {TOKEN}",
         "appkey": APP_KEY, "appsecret": APP_SECRET, "tr_id": tr_id, "custtype": "P"}
    if hkey: h["hashkey"] = hkey
    return h

def hashkey(body):
    r = requests.post(f"{BASE}/uapi/hashkey", headers={
        "content-type": "application/json", "appkey": APP_KEY, "appsecret": APP_SECRET},
        data=json.dumps(body), timeout=10)
    r.raise_for_status(); return r.json()["HASH"]

def api_get(path, tr_id, params, retries=3):
    last = None
    for i in range(retries):
        try:
            r = requests.get(f"{BASE}{path}", headers=headers(tr_id), params=params, timeout=10)
            if r.status_code in (429, 500, 503):
                last = requests.HTTPError(f"{r.status_code} (재시도 {i+1}/{retries})")
                time.sleep(0.5 * (i + 1)); continue
            r.raise_for_status(); return r.json()
        except requests.RequestException as e:
            last = e; time.sleep(0.5 * (i + 1))
    raise last if last else RuntimeError("api_get 실패")

def current_price(code):
    try:
        o = api_get("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                    {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}).get("output", {})
        return int(o.get("stck_prpr", 0) or 0)
    except Exception as e:
        log.warning(f"현재가 실패 {code}: {e}"); return 0

def daily_prices(code, days=120):
    end = datetime.now(); start = end - timedelta(days=days*2+10)
    try:
        d = api_get("/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
            {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
             "FID_INPUT_DATE_1": start.strftime("%Y%m%d"), "FID_INPUT_DATE_2": end.strftime("%Y%m%d"),
             "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})
        rows = [r for r in d.get("output2", []) if r.get("stck_bsop_date")]
        if not rows: return pd.DataFrame()
        df = pd.DataFrame({
            "close": [float(r["stck_clpr"]) for r in rows],
            "high":  [float(r["stck_hgpr"]) for r in rows],
            "low":   [float(r["stck_lwpr"]) for r in rows],
            "date":  [r["stck_bsop_date"] for r in rows]})
        return df.sort_values("date").reset_index(drop=True).tail(days)
    except Exception as e:
        log.warning(f"일봉 실패 {code}: {e}"); return pd.DataFrame()

def rank_list():
    if UNIVERSE == "fluctuation":
        path, tr_id = "/uapi/domestic-stock/v1/ranking/fluctuation", "FHPST01700000"
        params = {"FID_COND_MRKT_DIV_CODE":"J","FID_COND_SCR_DIV_CODE":"20170","FID_INPUT_ISCD":"0000",
                  "FID_RANK_SORT_CLS_CODE":"0","FID_INPUT_CNT_1":str(TOP_N),"FID_PRC_CLS_CODE":"0",
                  "FID_INPUT_PRICE_1":"","FID_INPUT_PRICE_2":"","FID_VOL_CNT":"","FID_TRGT_CLS_CODE":"0",
                  "FID_TRGT_EXLS_CLS_CODE":"0","FID_DIV_CLS_CODE":"0","FID_RSFL_RATE1":"","FID_RSFL_RATE2":""}
    else:
        path, tr_id = "/uapi/domestic-stock/v1/quotations/volume-rank", "FHPST01710000"
        params = {"FID_COND_MRKT_DIV_CODE":"J","FID_COND_SCR_DIV_CODE":"20171","FID_INPUT_ISCD":"0000",
                  "FID_DIV_CLS_CODE":"0","FID_BLNG_CLS_CODE":"0","FID_TRGT_CLS_CODE":"111111111",
                  "FID_TRGT_EXLS_CLS_CODE":"0000000000","FID_INPUT_PRICE_1":"","FID_INPUT_PRICE_2":"",
                  "FID_VOL_CNT":"","FID_INPUT_DATE_1":""}
    try:
        out = api_get(path, tr_id, params).get("output", [])
        res = []
        for r in out[:TOP_N]:
            code = r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd")
            if code:
                res.append({"code": code, "name": r.get("hts_kor_isnm",""),
                            "price": int(r.get("stck_prpr",0) or 0)})
        return res
    except Exception as e:
        log.warning(f"순위 실패: {e}"); return []

def balance():
    try:
        d = api_get("/uapi/domestic-stock/v1/trading/inquire-balance", TR[ENV]["balance"],
            {"CANO":CANO,"ACNT_PRDT_CD":ACNT,"AFHR_FLPR_YN":"N","OFL_YN":"","INQR_DVSN":"02",
             "UNPR_DVSN":"01","FUND_STTL_ICLD_YN":"N","FNCG_AMT_AUTO_RDPT_YN":"N","PRCS_DVSN":"01",
             "CTX_AREA_FK100":"","CTX_AREA_NK100":""})
        hold = [{"code":r["pdno"],"name":r["prdt_name"],"qty":int(r["hldg_qty"]),
                 "cur_price":int(r.get("prpr",0) or 0),"pnl_rate":float(r.get("evlu_pfls_rt",0) or 0),
                 "avg_price":float(r.get("pchs_avg_pric",0) or 0)}
                for r in d.get("output1",[]) if int(r.get("hldg_qty",0) or 0) > 0]
        summ = d.get("output2",[{}])
        s0 = summ[0] if summ else {}
        cash = int(s0.get("dnca_tot_amt",0) or 0)
        total = int(s0.get("tot_evlu_amt",0) or 0)
        return pd.DataFrame(hold), cash, total
    except Exception as e:
        log.warning(f"잔고 실패: {e}"); return pd.DataFrame(), 0, 0

def orderable_cash():
    try:
        d = api_get("/uapi/domestic-stock/v1/trading/inquire-psbl-order", TR[ENV]["psbl"],
            {"CANO":CANO,"ACNT_PRDT_CD":ACNT,"PDNO":"005930","ORD_UNPR":"0","ORD_DVSN":"01",
             "CMA_EVLU_AMT_ICLD_YN":"N","OVRS_ICLD_YN":"N"})
        return int(d.get("output", {}).get("ord_psbl_cash", 0) or 0)
    except Exception as e:
        log.warning(f"주문가능현금 조회 실패: {e}"); return 0

def order(code, qty, side, ord_dvsn="01", price=0):
    body = {"CANO":CANO,"ACNT_PRDT_CD":ACNT,"PDNO":code,"ORD_DVSN":ord_dvsn,
            "ORD_QTY":str(qty),"ORD_UNPR":str(int(price)) if price>0 else "0"}
    try:
        r = requests.post(f"{BASE}/uapi/domestic-stock/v1/trading/order-cash",
                          headers=headers(TR[ENV]["buy"] if side=="buy" else TR[ENV]["sell"], hashkey(body)),
                          data=json.dumps(body), timeout=10)
        r.raise_for_status(); j = r.json()
        ok = j.get("rt_cd") == "0"
        log.info(f"주문 {'접수' if ok else '실패'} [{side}] {code} x{qty}: {j.get('msg1')}")
        return ok
    except Exception as e:
        log.error(f"주문 오류 {code}: {e}"); return False

def open_orders():
    today = datetime.now().strftime("%Y%m%d")
    try:
        d = api_get("/uapi/domestic-stock/v1/trading/inquire-daily-ccld", TR[ENV]["ccld"],
            {"CANO":CANO,"ACNT_PRDT_CD":ACNT,"INQR_STRT_DT":today,"INQR_END_DT":today,
             "SLL_BUY_DVSN_CD":"00","INQR_DVSN":"00","PDNO":"","CCLD_DVSN":"02",
             "ORD_GNO_BRNO":"","ODNO":"","INQR_DVSN_3":"00","INQR_DVSN_1":"",
             "CTX_AREA_FK100":"","CTX_AREA_NK100":""})
        return [r for r in d.get("output1", []) if int(r.get("rmn_qty",0) or 0) > 0]
    except Exception as e:
        log.warning(f"미체결 조회 실패: {e}"); return []

def cancel_open_orders():
    n = 0
    for o in open_orders():
        body = {"CANO":CANO,"ACNT_PRDT_CD":ACNT,
                "KRX_FWDG_ORD_ORGNO":o.get("ord_gno_brno",""),"ORGN_ODNO":o.get("odno",""),
                "ORD_DVSN":"00","RVSE_CNCL_DVSN_CD":"02","ORD_QTY":"0","ORD_UNPR":"0",
                "QTY_ALL_ORD_YN":"Y"}
        try:
            r = requests.post(f"{BASE}/uapi/domestic-stock/v1/trading/order-rvsecncl",
                              headers=headers(TR[ENV]["cancel"], hashkey(body)),
                              data=json.dumps(body), timeout=10)
            r.raise_for_status()
            if r.json().get("rt_cd") == "0": n += 1
        except Exception as e:
            log.warning(f"취소 실패 {o.get('odno')}: {e}")
        time.sleep(0.2)
    if n: log.info(f"미체결 {n}건 취소")

def holdings_dict(df):
    if df.empty: return {}
    return {r["code"]: {"qty":int(r["qty"]), "name":r["name"],
                        "price":int(r["cur_price"]), "avg":float(r.get("avg_price",0) or 0)}
            for _, r in df.iterrows()}

def reconcile_fills(cur):
    for code, c in cur.items():
        d = c["qty"] - _prev_holdings.get(code, {}).get("qty", 0)
        if d > 0:
            reason = _pending_buy_reason.pop(code, "매수 체결")
            price = int(c["avg"]) if c["avg"] else c["price"]
            record(c["name"], "BUY", d, price, reason)
    for code, p in _prev_holdings.items():
        d = p["qty"] - cur.get(code, {}).get("qty", 0)
        if d > 0:
            reason = _pending_sell_reason.pop(code, "매도 체결")
            record(p["name"], "SELL", d, p["price"], reason)

def ma(df, p): return df["close"].rolling(p).mean()

def signal(df, code, name):
    need = {"momentum":MOM_PERIOD+1,
            "disparity":DISP_PERIOD+1,
            "golden_cross":MA_SLOW+1,
            "consecutive":max(CONSEC_BUY,CONSEC_SELL)+1,
            "momentum_ma":max(MOM_PERIOD+1, MA_SLOW+1),
            "pullback_momentum":max(MOM_PERIOD+1, MA_FAST+1, MA_SLOW+1)}.get(STRATEGY,30)
    if df.empty or len(df) < need:
        return (name, "HOLD", 0.0, "데이터 부족")

    if STRATEGY == "momentum":
        ret = (df["close"].iloc[-1]/df["close"].iloc[-(MOM_PERIOD+1)]-1)*100
        if ret >= MOM_BUY:
            return (name,"BUY",min(1.0,0.5+ret/100),
                    f"모멘텀 조건 충족: {MOM_PERIOD}일 수익률 +{ret:.1f}% >= +{MOM_BUY:.1f}%")
        if ret <= MOM_SELL:
            return (name,"SELL",min(1.0,0.5+abs(ret)/100),
                    f"모멘텀 약화: {MOM_PERIOD}일 수익률 {ret:.1f}% <= {MOM_SELL:.1f}%")
        return (name,"HOLD",0.0,
                f"모멘텀 중립: {MOM_PERIOD}일 수익률 {ret:.1f}% / 매수기준 +{MOM_BUY:.1f}%")

    if STRATEGY == "momentum_ma":
        close = df["close"].iloc[-1]
        ret = (close/df["close"].iloc[-(MOM_PERIOD+1)]-1)*100
        slow_ma = ma(df, MA_SLOW).iloc[-1]
        if ret >= MOM_BUY and close > slow_ma:
            return (name,"BUY",min(1.0,0.5+ret/100),
                    f"모멘텀+이동평균 매수: {MOM_PERIOD}일 수익률 +{ret:.1f}% >= +{MOM_BUY:.1f}%, 현재가 {close:,.0f} > {MA_SLOW}일선 {slow_ma:,.0f}")
        if ret <= MOM_SELL or close < slow_ma:
            reason = []
            if ret <= MOM_SELL:
                reason.append(f"{MOM_PERIOD}일 수익률 {ret:.1f}% <= {MOM_SELL:.1f}%")
            if close < slow_ma:
                reason.append(f"현재가 {close:,.0f} < {MA_SLOW}일선 {slow_ma:,.0f}")
            return (name,"SELL",min(1.0,0.5+abs(ret)/100),
                    "모멘텀+이동평균 매도: " + "; ".join(reason))
        return (name,"HOLD",0.0,
                f"모멘텀+이동평균 관망: {MOM_PERIOD}일 수익률 {ret:.1f}%, 현재가 {close:,.0f}, {MA_SLOW}일선 {slow_ma:,.0f}")

    if STRATEGY == "pullback_momentum":
        close = df["close"].iloc[-1]
        ret = (close/df["close"].iloc[-(MOM_PERIOD+1)]-1)*100
        fast_ma = ma(df, MA_FAST).iloc[-1]
        slow_ma = ma(df, MA_SLOW).iloc[-1]
        # 중기 상승 흐름은 유지되지만, 단기선 아래로 살짝 눌린 구간을 매수 후보로 본다.
        if ret >= MOM_BUY and slow_ma < close < fast_ma:
            return (name,"BUY",min(1.0,0.5+ret/100),
                    f"눌림목 모멘텀 매수: {MOM_PERIOD}일 수익률 +{ret:.1f}% >= +{MOM_BUY:.1f}%, {MA_SLOW}일선 {slow_ma:,.0f} < 현재가 {close:,.0f} < {MA_FAST}일선 {fast_ma:,.0f}")
        if ret <= MOM_SELL or close < slow_ma:
            reason = []
            if ret <= MOM_SELL:
                reason.append(f"{MOM_PERIOD}일 수익률 {ret:.1f}% <= {MOM_SELL:.1f}%")
            if close < slow_ma:
                reason.append(f"현재가 {close:,.0f} < {MA_SLOW}일선 {slow_ma:,.0f}")
            return (name,"SELL",min(1.0,0.5+abs(ret)/100),
                    "눌림목 모멘텀 매도: " + "; ".join(reason))
        return (name,"HOLD",0.0,
                f"눌림목 조건 미충족: {MOM_PERIOD}일 수익률 {ret:.1f}%, 현재가 {close:,.0f}, {MA_FAST}일선 {fast_ma:,.0f}, {MA_SLOW}일선 {slow_ma:,.0f}")

    if STRATEGY == "disparity":
        m = ma(df, DISP_PERIOD).iloc[-1]
        d = (df["close"].iloc[-1]/m)*100 if m else 100
        if d < DISP_OVERSOLD:   return (name,"BUY",min(1.0,(DISP_OVERSOLD-d)/20+0.5),f"이격도 {d:.1f}(과매도)")
        if d > DISP_OVERBOUGHT: return (name,"SELL",min(1.0,(d-DISP_OVERBOUGHT)/20+0.5),f"이격도 {d:.1f}(과매수)")
        return (name,"HOLD",0.0,f"이격도 {d:.1f}(중립)")

    if STRATEGY == "golden_cross":
        mf, ms = ma(df,MA_FAST), ma(df,MA_SLOW)
        if mf.iloc[-2]<=ms.iloc[-2] and mf.iloc[-1]>ms.iloc[-1]: return (name,"BUY",0.7,"골든크로스")
        if mf.iloc[-2]>=ms.iloc[-2] and mf.iloc[-1]<ms.iloc[-1]: return (name,"SELL",0.7,"데드크로스")
        return (name,"HOLD",0.0,"크로스 없음")

    if STRATEGY == "consecutive":
        c = df["close"].values; up=down=0
        for i in range(len(c)-1,0,-1):
            if c[i]>c[i-1]: up+=1
            else: break
        for i in range(len(c)-1,0,-1):
            if c[i]<c[i-1]: down+=1
            else: break
        if up>=CONSEC_BUY:    return (name,"BUY",min(0.9,0.5+up*0.08),f"{up}일 연속 상승")
        if down>=CONSEC_SELL: return (name,"SELL",min(0.9,0.5+down*0.08),f"{down}일 연속 하락")
        return (name,"HOLD",0.0,f"연속 미충족(▲{up}/▼{down})")
    return (name,"HOLD",0.0,"알 수 없는 전략")

def load_existing_trades():
    if not RECORD_FILE.exists(): return
    for line in RECORD_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("| 20"):
            cols = [c.strip() for c in line.strip("|").split("|")]
            if len(cols) == 6:
                _trades.append({"time":cols[0],"name":cols[1],"side":cols[2],
                                "qty":cols[3],"price":cols[4],"reason":cols[5]})

def record(name, side, qty, price, reason):
    _trades.append({"time":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "name":name,"side":side,"qty":str(qty),"price":f"{price:,}","reason":reason})
    log.info(f"기록: {side} {name} x{qty} @ {price}")

def strategy_description():
    descriptions = {
        "momentum": f"{MOM_PERIOD}일 수익률이 +{MOM_BUY:.1f}% 이상이면 매수, {MOM_SELL:.1f}% 이하이면 매도 신호를 생성",
        "disparity": f"현재가와 {DISP_PERIOD}일 이동평균의 이격도를 기준으로 과매도/과매수 판단",
        "golden_cross": f"{MA_FAST}일 이동평균과 {MA_SLOW}일 이동평균의 교차를 기준으로 추세 전환 판단",
        "consecutive": f"{CONSEC_BUY}일 연속 상승 또는 {CONSEC_SELL}일 연속 하락 여부로 단기 흐름 판단",
        "momentum_ma": f"{MOM_PERIOD}일 모멘텀 조건과 {MA_SLOW}일 이동평균 조건을 함께 확인",
        "pullback_momentum": f"중기 모멘텀은 유지되지만 단기 조정이 나타난 종목을 매수 후보로 판단",
    }
    return descriptions.get(STRATEGY, "사용자 정의 전략")

def write_report():
    cum = mdd = None
    if _equity_hist:
        final = _equity_hist[-1]
        cum = (final - SEED_CAPITAL)/SEED_CAPITAL*100
        peak = _equity_hist[0]; worst = 0
        for v in _equity_hist:
            peak = max(peak, v)
            worst = min(worst, (v-peak)/peak*100)
        mdd = worst
    sells = [t for t in _trades if t["side"]=="SELL"]
    L = ["# 자동매매 기록 (Trading Log)\n",
         f"\n_갱신: {datetime.now():%Y-%m-%d %H:%M:%S}_  ·  전략: `{STRATEGY}`  ·  환경: `{ENV}`\n",
         f"\n- 시드 자본: {SEED_CAPITAL:,}원\n",
         f"- 총 체결: {len(_trades)}건 (매도 {len(sells)}건)\n"]
    if cum is not None:
        L.append(f"- **누적 수익률: {cum:+.2f}%**  ·  **MDD(이번 실행): {mdd:.2f}%**\n")
    L += ["\n## 전체 체결 로그\n", "| 시각 | 종목 | 구분 | 수량 | 가격 | 사유 |\n|---|---|---|---|---|---|\n"]
    for t in _trades:
        L.append(f"| {t['time']} | {t['name']} | {t['side']} | {t['qty']} | {t['price']} | {t['reason']} |\n")
    RECORD_FILE.write_text("".join(L), encoding="utf-8")

def market_open():
    now = datetime.now()
    if now.weekday() >= 5: return False
    o = datetime.strptime(MARKET_OPEN,"%H:%M").time()
    c = datetime.strptime(MARKET_CLOSE,"%H:%M").time()
    return o <= now.time() <= c

def cycle():
    global _prev_holdings, _baseline_done
    cancel_open_orders()
    time.sleep(2)

    holdings, cash, total = balance()
    cur = holdings_dict(holdings)
    if _baseline_done:
        reconcile_fills(cur)
    else:
        _baseline_done = True
    _prev_holdings = cur

    eval_amt = int((holdings["qty"]*holdings["cur_price"]).sum()) if not holdings.empty else 0
    if total <= 0: total = cash + eval_amt
    _equity_hist.append(total)
    log.info(f"보유 {len(holdings)}종목 | 예수금 {cash:,} | 평가 {eval_amt:,} | 총자산 {total:,}")

    if not holdings.empty:
        for _, h in holdings.iterrows():
            if h["pnl_rate"] <= STOP_LOSS_PCT or h["pnl_rate"] >= TAKE_PROFIT_PCT:
                tag = "손절" if h["pnl_rate"] <= STOP_LOSS_PCT else "익절"
                if order(h["code"], int(h["qty"]), "sell", ord_dvsn="03"):
                    _pending_sell_reason[h["code"]] = f"{tag} ({h['pnl_rate']:.1f}%)"
                continue

            # 보유 종목도 선택 전략의 SELL 신호가 발생하면 매도한다.
            # 기존 손절/익절 조건을 우선 적용하고, 그 외에는 전략 기반 매도 신호를 확인한다.
            s_name, s_action, s_strength, s_reason = signal(daily_prices(h["code"]), h["code"], h["name"])
            if s_action == "SELL":
                if order(h["code"], int(h["qty"]), "sell", ord_dvsn="03"):
                    _pending_sell_reason[h["code"]] = s_reason
                time.sleep(0.2)

    held = set(holdings["code"]) if not holdings.empty else set()
    sigs = []
    for c in rank_list():
        if not c["code"].isdigit(): continue
        if c["price"]<MIN_PRICE or c["price"]>MAX_PRICE: continue
        if EXCLUDE_ETF and any(k.upper() in c["name"].upper() for k in ETF_KW): continue
        if c["code"] in held: continue
        name, action, strength, reason = signal(daily_prices(c["code"]), c["code"], c["name"])
        if action == "BUY" and strength >= MIN_STRENGTH:
            sigs.append((c["code"], name, strength, reason))
            log.info(f"  매수후보 {name}: 강도={strength:.2f} ({reason})")
        time.sleep(0.2)

    holdings, _, _ = balance()
    avail = orderable_cash() * INVEST_RATIO
    slots = PORTFOLIO - (len(holdings) if not holdings.empty else 0)
    if slots > 0 and sigs and avail > 0:
        sigs.sort(key=lambda x: x[2], reverse=True)
        per = avail / slots
        for code, name, strength, reason in sigs[:slots]:
            if avail < MIN_PRICE:
                log.info("주문가능현금 소진 — 이번 사이클 매수 종료"); break
            p = current_price(code)
            if p <= 0: continue
            qty = int(min(per, avail) // p)
            if qty <= 0: continue
            if order(code, qty, "buy", ord_dvsn="00", price=p):
                _pending_buy_reason[code] = reason
                avail -= qty * p
            time.sleep(0.3)

    write_report()

def main():
    global TOKEN
    TOKEN = get_token()
    load_existing_trades()
    log.info(f"=== 자동매매 시작 (env={ENV}, strategy={STRATEGY}) ===")
    log.info(f"전략 설명: {strategy_description()}")
    while True:
        try:
            if market_open(): cycle()
            else: log.info("장 시간 외 — 대기")
            time.sleep(LOOP_INTERVAL)
        except KeyboardInterrupt:
            log.info("중단"); break
        except Exception as e:
            log.error(f"사이클 오류: {e}"); time.sleep(LOOP_INTERVAL)

if __name__ == "__main__":
    main()
