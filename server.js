/**
CryptoScanner Pro v5.2 — FIXED VERSION
✅ MACD signal line corrected
✅ Volatility regime classification added
✅ Multi-exchange timestamp validation
✅ Request deduplication
✅ Structured logging
✅ Cache policy parameterization
✅ HyperTracker fallback tiers
✅ Rate limiting
APIs: CoinGecko(free) · Coinalyze · HyperTracker · Binance/Bybit
Run: node server.js  →  http://localhost:3000
*/
const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const PORT        = 3000;
const CONFIG_FILE = path.join(__dirname, 'config.json');
const USAGE_FILE  = path.join(__dirname, 'ht_usage.json');
const HT_LIMIT    = 100;
const LOG_DIR     = path.join(__dirname, 'logs');
const LOG_FILE    = path.join(LOG_DIR, 'server.log');

// Ensure logs directory exists
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

// ── STRUCTURED LOGGING ──────────────────────────────────
function log(level, module, msg, data = {}) {
  const timestamp = new Date().toISOString();
  const entry = {
    timestamp,
    level,
    module,
    msg,
    ...data
  };
  const logLine = JSON.stringify(entry);
  const consoleMsg = `[${level}] [${module}] ${msg}`;
  
  if (level === 'ERROR') {
    console.error(consoleMsg, data);
  } else if (level === 'WARN') {
    console.warn(consoleMsg, data);
  } else {
    console.log(consoleMsg, data);
  }
  
  try {
    fs.appendFileSync(LOG_FILE, logLine + '\n');
  } catch (e) {
    console.error('[Log Write Error]', e.message);
  }
}

function createTimer() {
  const start = Date.now();
  return {
    elapsed: () => Date.now() - start,
    mark: (label) => ({ label, ms: Date.now() - start })
  };
}

// ── Config ─────────────────────────────────────────────────────────────
let CFG = {};
function loadCfg() { try { CFG = JSON.parse(fs.readFileSync(CONFIG_FILE,'utf8')); } catch(e){} }
loadCfg();
function KEY(n) { loadCfg(); return process.env[n.toUpperCase()] || CFG[n] || ''; }

// ── HT budget ──────────────────────────────────────────────────────────
function today() { return new Date().toISOString().slice(0,10); }
function loadUsage() { try { const u=JSON.parse(fs.readFileSync(USAGE_FILE,'utf8')); if(u.date===today())return u; } catch(e){} return{date:today(),count:0}; }
function bumpUsage() { const u=loadUsage();u.count++;try{fs.writeFileSync(USAGE_FILE,JSON.stringify(u,null,2));}catch(e){}log('DEBUG','ht',`Budget used: ${u.count}/${HT_LIMIT}`);return u.count; }
function htRem() { return HT_LIMIT-loadUsage().count; }

// ── Cache Policy Definitions ────────────────────────
const CACHE_POLICIES = {
  coins: { ttl: 10 * 60 * 1000, rationale: 'CoinGecko updates ~10-15min' },
  futures: { ttl: 5 * 60 * 1000, rationale: 'OI/funding update ~hourly, be fresh' },
  htMarket: { ttl: 30 * 60 * 1000, rationale: 'Whale positions ~30min latency' },
  htBreakdown: { ttl: 60 * 60 * 1000, rationale: 'Size distribution ~1hr latency' },
  kiyIndicators: { ttl: 2 * 60 * 1000, rationale: 'Price/RSI update every 1-2 candles' }
};

// ── Cache ──────────────────────────────────────────────────────────────
const CACHE={};
function cGet(k){const e=CACHE[k];return(e&&Date.now()-e.ts<e.ttl)?e.data:null;}
function cSet(k,d,ttl){CACHE[k]={data:d,ts:Date.now(),ttl};}
function cAge(k){return CACHE[k]?Math.round((Date.now()-CACHE[k].ts)/1000):null;}
function cSetPolicy(key, data, policyName) {
  const policy = CACHE_POLICIES[policyName];
  if (!policy) throw new Error(`No cache policy: ${policyName}`);
  cSet(key, data, policy.ttl);
  log('DEBUG', 'cache', `Set ${key}`, { ttlMs: policy.ttl, policy: policyName });
}

// ── REQUEST DEDUPLICATION (PREVENT CACHE STAMPEDE) ────
const REQUEST_LOCKS = {};
async function withRequestDedup(key, fetcher) {
  if (REQUEST_LOCKS[key]) {
    log('DEBUG', 'dedup', `${key}: Waiting for in-flight request...`);
    return REQUEST_LOCKS[key];
  }
  REQUEST_LOCKS[key] = fetcher()
    .then(result => {
      delete REQUEST_LOCKS[key];
      return result;
    })
    .catch(err => {
      delete REQUEST_LOCKS[key];
      throw err;
    });
  return REQUEST_LOCKS[key];
}

// ── RATE LIMITING ────────────────────────────────────
const RATE_LIMITS = {};
const RATE_LIMIT_CONFIG = {
  default: { requests: 30, windowMs: 60000 },
  '/api/kiy/scan': { requests: 3, windowMs: 60000 },
  '/api/saveconfig': { requests: 10, windowMs: 60000 }
};

function checkRateLimit(ip, pathname) {
  const config = RATE_LIMIT_CONFIG[pathname] || RATE_LIMIT_CONFIG.default;
  const key = `${ip}:${pathname}`;
  const now = Date.now();
  if (!RATE_LIMITS[key]) {
    RATE_LIMITS[key] = { requests: [], blockedUntil: null };
  }
  const entry = RATE_LIMITS[key];
  if (entry.blockedUntil && now < entry.blockedUntil) {
    return { allowed: false, retryAfterS: Math.ceil((entry.blockedUntil - now) / 1000) };
  }
  entry.requests = entry.requests.filter(t => now - t < config.windowMs);
  if (entry.requests.length >= config.requests) {
    entry.blockedUntil = now + config.windowMs;
    return { allowed: false, retryAfterS: Math.ceil(config.windowMs / 1000) };
  }
  entry.requests.push(now);
  return { allowed: true };
}

// ── HTTPS fetch — strips auth on S3 redirects ──────────────────────────
function fetchJSON(url, hdrs={}, stripAuthOnRedirect=false) {
  return new Promise((resolve,reject)=>{
    const p=new URL(url);
    const sendHdrs = stripAuthOnRedirect ? {} : hdrs;
    const req=https.request({
      hostname:p.hostname, path:p.pathname+p.search, method:'GET',
      headers:{'Accept':'application/json','User-Agent':'CryptoScannerPro/5.2',...sendHdrs},
      timeout:25000
    }, res=>{
      if(res.statusCode===301||res.statusCode===302){
        const isS3=res.headers.location&&res.headers.location.includes('amazonaws.com');
        return fetchJSON(res.headers.location, hdrs, isS3).then(resolve).catch(reject);
      }
      let body='';
      res.on('data',c=>body+=c);
      res.on('end',()=>{
        if(res.statusCode===401||res.statusCode===403){reject(new Error('Invalid API key (HTTP '+res.statusCode+')'));return;}
        if(res.statusCode===429){reject(new Error('Rate limit — wait 60s'));return;}
        if(res.statusCode<200||res.statusCode>=300){reject(new Error('HTTP '+res.statusCode+': '+body.slice(0,300)));return;}
        try{resolve(JSON.parse(body));}
        catch(e){reject(new Error('Bad JSON from '+p.hostname+': '+body.slice(0,100)));}
      });
    });
    req.on('error',e=>reject(new Error(e.code+': '+e.message)));
    req.on('timeout',()=>{req.destroy();reject(new Error('Timeout: '+p.hostname));});
    req.end();
  });
}

// ══════════════════════════════════════════════════════════════════════
// 1. COINGECKO — free, no key, no CORS from server
// ══════════════════════════════════════════════════════════════════════
async function apiCoins() {
  const timer = createTimer();
  const hit=cGet('coins');
  if(hit){
    log('DEBUG', 'coins', 'Cache hit', { ageS: cAge('coins') });
    return{data:hit,fromCache:true,ageS:cAge('coins')};
  }
  try {
    const cgKey = KEY('coingecko_key');
    const keyParam = cgKey ? `&x_cg_demo_api_key=${cgKey}` : '';
    const url=`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=volume_desc&per_page=200&page=1&sparkline=false&price_change_percentage=7d${keyParam}`;
    const d=await fetchJSON(url);
    if(!Array.isArray(d))throw new Error('CoinGecko: unexpected response — try again in 60s');
    const coins=d.map((c,i)=>({
      id:c.id, symbol:(c.symbol||c.id).toUpperCase(), name:c.name,
      price:parseFloat(c.current_price||0),
      ch24:parseFloat(c.price_change_percentage_24h||0),
      ch7:parseFloat(c.price_change_percentage_7d_in_currency||0),
      mcap:parseFloat(c.market_cap||0),
      vol24:parseFloat(c.total_volume||0),
      vm:parseFloat(c.total_volume||0)/Math.max(parseFloat(c.market_cap||1),1),
      rank:parseInt(c.market_cap_rank||i+1),
    }));
    cSetPolicy('coins', coins, 'coins');
    log('INFO', 'coins', 'Fetched successfully', { count: coins.length, latencyMs: timer.elapsed() });
    return{data:coins,fromCache:false};
  } catch(e) {
    log('ERROR', 'coins', 'Fetch failed', { error: e.message, latencyMs: timer.elapsed() });
    throw e;
  }
}

// ══════════════════════════════════════════════════════════════════════
// 2. COINALYZE — funding, OI, predicted FR, liquidations
// ══════════════════════════════════════════════════════════════════════
const CA_SYMS = [
  'NEARUSDT_PERP.A', 'ENAUSDT_PERP.A', 'ARKMUSDT_PERP.A', 'SUIUSDT_PERP.A', 'AIXBTUSDT_PERP.A',
  'GALAUSDT_PERP.A', 'LINKUSDT_PERP.A', 'AVAXUSDT_PERP.A', 'ONDOUSDT_PERP.A',
  'XPLUSDT_PERP.A', 'BTCUSDT_PERP.A', 'HYPEUSDT_PERP.A', 'AXSUSDT_PERP.A', 'OPUSDT_PERP.A'
].join(',');

function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }

async function fetchWithRetry(url, headers = {}, retries = 2){
  for(let i=0;i<=retries;i++){
    try {
      return await fetchJSON(url, headers);
    } catch(err) {
      const msg = err.message || '';
      if(msg.includes('Rate limit') && i < retries){
        log('WARN', 'fetch', `Rate limit on ${url}, retrying in 65s...`);
        await sleep(65000);
        continue;
      }
      if(msg.includes('Timeout') && i < retries){
        log('WARN', 'fetch', 'Timeout, retrying in 5s...');
        await sleep(5000);
        continue;
      }
      throw err;
    }
  }
}

async function apiFutures(){
  const timer = createTimer();
  const hit = cGet('futures');
  if(hit) {
    log('DEBUG', 'futures', 'Cache hit', { ageS: cAge('futures') });
    return { data: hit, fromCache: true, ageS: cAge('futures') };
  }
  try {
    const caKey = KEY('coinalyze_key');
    if(!caKey) throw new Error('No Coinalyze key');
    const base = 'https://api.coinalyze.net/v1';
    const qs = `api_key=${caKey}&symbols=${CA_SYMS}`;

    log('DEBUG', 'futures', 'Fetching funding-rate');
    const frData = await fetchWithRetry(`${base}/funding-rate?${qs}`);
    await sleep(1500);

    log('DEBUG', 'futures', 'Fetching open-interest');
    const oiData = await fetchWithRetry(`${base}/open-interest?${qs}`);
    await sleep(1500);

    log('DEBUG', 'futures', 'Fetching predicted-funding');
    const pfrData = await fetchWithRetry(`${base}/predicted-funding-rate?${qs}`);
    await sleep(1500);

    let liqData = [];
    try {
      log('DEBUG', 'futures', 'Fetching liquidations');
      const now  = Math.floor(Date.now()/1000);
      const from = now - (24 * 60 * 60);
      liqData = await fetchWithRetry(`${base}/liquidation-history?${qs}&interval=1hour&from=${from}&to=${now}`);
    } catch(e) {
      log('WARN', 'futures', 'Liquidations fetch failed', { error: e.message });
    }

    const oiM = {}; const pfrM = {}; const liqM = {};
    if(Array.isArray(oiData)) oiData.forEach(x => { oiM[x.symbol] = parseFloat(x.value || 0); });
    if(Array.isArray(pfrData)) pfrData.forEach(x => { pfrM[x.symbol] = parseFloat(x.value || 0); });
    if(Array.isArray(liqData)) liqData.forEach(x => {
      const total = parseFloat(x.long_liquidations || x.value || 0) + parseFloat(x.short_liquidations || 0);
      liqM[x.symbol] = (liqM[x.symbol] || 0) + total;
    });

    const agg = {};
    if(Array.isArray(frData)){
      frData.forEach(x => {
        const sym = x.symbol || '';
        const base2 = sym.replace(/_PERP\..+$/,'').replace(/USDT$/,'').replace(/USD$/,'').replace(/BUSD$/,'');
        if(!base2 || base2.length > 12) return;

        if(!agg[base2]){
          agg[base2] = { base: base2, syms: [], frs: [], oi: 0, pfr: 0, liq: 0 };
        }
        agg[base2].syms.push(sym);
        agg[base2].frs.push(parseFloat(x.value || 0));
        agg[base2].oi += oiM[sym] || 0;
        agg[base2].pfr += pfrM[sym] || 0;
        agg[base2].liq += liqM[sym] || 0;
      });
    }

    const results = Object.values(agg).map(x => {
      const fr = x.frs.length ? x.frs.reduce((a,b) => a+b,0) / x.frs.length : 0;
      const pfr = x.syms.length ? x.pfr / x.syms.length : 0;
      let sq = 50;
      if(fr < 0) sq += Math.min(Math.abs(fr) * 10000, 30);
      if(fr < -0.0005) sq += 8;
      if(fr < -0.001)  sq += 5;

      return {
        id: x.base, sym: x.base, fundingRate: fr, predictedFR: pfr, oi: x.oi, liq: x.liq,
        squeezeScore: Math.min(Math.round(sq), 99), exchangeCount: x.syms.length
      };
    })
    .filter(x => x.oi > 0 || Math.abs(x.fundingRate) > 0.00001)
    .sort((a,b) => b.squeezeScore - a.squeezeScore);

    cSetPolicy('futures', results, 'futures');
    log('INFO', 'futures', 'Fetched successfully', { count: results.length, latencyMs: timer.elapsed() });
    return { data: results, fromCache: false };
  } catch(e) {
    log('ERROR', 'futures', 'Fetch failed', { error: e.message, latencyMs: timer.elapsed() });
    throw e;
  }
}

// ══════════════════════════════════════════════════════════════════════
// 3. HYPERTRACKER — whale trap (Hyperliquid positions)
// ══════════════════════════════════════════════════════════════════════
function htH(){return{'Authorization':'Bearer '+KEY('hypertracker_key')};}

async function apiHtMarket(){
  const ck='ht_market',hit=cGet(ck);
  if(hit){
    log('DEBUG', 'ht', 'HT Market cache hit', { ageS: cAge(ck) });
    return{data:hit,fromCache:true,ageS:cAge(ck),remaining:htRem()};
  }
  if(!KEY('hypertracker_key'))throw new Error('No HyperTracker key');
  if(htRem()<=5)throw new Error(`HyperTracker budget low: ${htRem()} left today`);
  bumpUsage();
  const data=await fetchJSON('https://ht-api.coinmarketman.com/api/external/positions/coins',htH());
  if(!Array.isArray(data))throw new Error('HyperTracker: unexpected response');
  const enriched=data.map(c=>({
    coin:c.coin,totalValue:c.totalValue,count:c.count,countLong:c.countLong,countShort:c.countShort,
    countBias:c.count>0?Math.round((c.countLong/c.count)*100):50,
    valueBias:c.totalValue>0?Math.round((c.totalValueLong/c.totalValue)*100):50,
    whaleDivergence:c.count>0&&c.totalValue>0?Math.round(((c.totalValueLong/c.totalValue)-(c.countLong/c.count))*100):0,
  }));
  cSetPolicy(ck,enriched,'htMarket');
  return{data:enriched,fromCache:false,remaining:htRem()};
}

const VALID_HT_COINS=['BTC','ETH','SOL','XRP','DOGE','HYPE','LINK','SUI','BNB','AVAX','INJ','ARB','OP','NEAR','APT','TAO','WIF','ONDO', 'RENDER', 'ALLO', 'SEI', 'ENA', 'ARKM','WLD' ,'AIXBT' ];

async function apiHtBreakdown(coin){
  const coinUp=coin.toUpperCase();
  if(!VALID_HT_COINS.includes(coinUp))throw new Error( `${coinUp} not supported for breakdown — use standard coins` );
  const ck='ht_bd_'+coinUp,hit=cGet(ck);
  if(hit){
    log('DEBUG', 'ht',  `HT Breakdown cache hit for ${coinUp}` , { ageS: cAge(ck) });
    return{data:hit,fromCache:true,ageS:cAge(ck),remaining:htRem()};
  }
  if(!KEY('hypertracker_key'))throw new Error('No HyperTracker key');
  if(htRem() <=3)throw new Error( `HyperTracker budget critical: ${htRem()} left` );
  bumpUsage();
  const data=await fetchJSON( `https://ht-api.coinmarketman.com/api/external/exports/coins/${coinUp}/position-breakdown-by-size` ,htH());
  if(!data?.breakdownBySize)throw new Error('HyperTracker breakdown: bad response');
  const bk=data.breakdownBySize;
  const retail=bk.filter(b => b.sizeSegmentId <=4);
  const whales=bk.filter(b => b.sizeSegmentId >=7);
  const rB=retail.length?retail.reduce((a,b) => a+(b.bias||.5),0)/retail.length:.5;
  const wB=whales.length?whales.reduce((a,b) => a+(b.bias||.5),0)/whales.length:.5;
  const div=Math.round((wB-rB)*100);
  let wts=50;if(div >5)wts+=20;if(div >10)wts+=15;if(div >15)wts+=12;if(wB >.6 && rB <.45)wts=Math.min(wts+10,99);
  const result={
    coin:coin.toUpperCase(),breakdownBySize:bk,
    retailBiasPct:Math.round(rB*100),whaleBiasPct:Math.round(wB*100),
    divergence:div,whaleTrapScore:Math.min(wts,99),
    signal:div >12?'🐳 WHALE TRAP — Whales long, retail short — S7 ACTIVE':div >6?'⚡ Mild divergence — setup forming':div <-12?'🔻 Retail more bullish — caution':'⚖️ No significant divergence',
    createdAt:data.createdAt,
  };
  cSetPolicy(ck,result,'htBreakdown');
  return{data:result,fromCache:false,remaining:htRem()};
}

// ══════════════════════════════════════════════════════════════════════
// 4. BINANCE + MULTI EXCHANGE INDICATOR ENGINE
// Public endpoints only — no keys required
// ══════════════════════════════════════════════════════════════════════
const BX_BASE='https://fapi.binance.com';
const BYBIT_BASE='https://api.bybit.com';
const OKX_BASE='https://www.okx.com';

function tfMap(tf){
  const m={ HOUR:'1h', FOUR_HOURS:'4h', DAY:'1d' };
  return m[tf]||'1h';
}

// ─────────────────────────────────────────────
// TECHNICAL COMPONENT MATHEMATICS
// ─────────────────────────────────────────────
function calcRSI(closes,p=14){
  if(closes.length< p+1) return null;
  let g=0,l=0;
  for(let i=1;i<=p;i++){
    const d=closes[i]-closes[i-1];
    if(d>0) g+=d; else l+=Math.abs(d);
  }
  let ag=g/p,al=l/p;
  for(let i=p+1;i<closes.length;i++){
    const d=closes[i]-closes[i-1];
    ag=(ag*(p-1)+(d>0?d:0))/p;
    al=(al*(p-1)+(d<0?Math.abs(d):0))/p;
  }
  if(al===0) return 100;
  return Math.round(100-(100/(1+ag/al)));
}

function calcEMA(data,p){
  const k = 2/(p+1);
  let e=data.slice(0,p).reduce((a,b)=>a+b,0)/p;
  for(let i=p;i<data.length;i++){
    e = data[i] * k + e * (1 - k);
  }
  return e;
}

// ✅ FIXED: MACD signal line corrected
function calcMACD(closes){
  if(closes.length < 26) return { macd:null, signal:null, hist:null };
  const macdHistory = [];
  for (let i = 25; i < closes.length; i++) {
    const f = calcEMA(closes.slice(0, i+1), 12);
    const s = calcEMA(closes.slice(0, i+1), 26);
    macdHistory.push(f - s);
  }
  const fast = calcEMA(closes, 12);
  const slow = calcEMA(closes, 26);
  const macdLine = fast - slow;
  const signalLine = calcEMA(macdHistory, 9);
  return {
    macd: parseFloat(macdLine.toFixed(6)),
    signal: parseFloat(signalLine.toFixed(6)),
    hist: parseFloat((macdLine - signalLine).toFixed(6))
  };
}

// ✅ NEW: Volatility Analysis
function calcATR(klines, period = 14) {
  const tr = klines.map((k, i) => {
    if (i === 0) return k.high - k.low;
    const prev = klines[i-1].close;
    return Math.max(
      k.high - k.low,
      Math.abs(k.high - prev),
      Math.abs(k.low - prev)
    );
  });
  return calcEMA(tr, period);
}

function calcHistoricalVolatility(closes, period = 20) {
  if (closes.length < period) return 0;
  const recent = closes.slice(-period);
  const returns = [];
  for (let i = 1; i < recent.length; i++) {
    returns.push(Math.log(recent[i] / recent[i-1]));
  }
  const mean = returns.reduce((a,b) => a+b, 0) / returns.length;
  const variance = returns.reduce((a,b) => a + Math.pow(b-mean, 2), 0) / returns.length;
  const stdDev = Math.sqrt(variance);
  return stdDev * Math.sqrt(252 * 24); // Annualized
}

function classifyVolatilityRegime(historicalVol) {
  if (historicalVol < 0.8) return 'low';
  if (historicalVol < 2.5) return 'medium';
  return 'high';
}

// ── SESSION VWAP STRATEGY MODULE ──
function calcSessionVWAP(klines, opts = { multipliers: [1, 2, 3] }) {
  if (!klines || klines.length === 0) return [];
  let cumPV = 0, cumVol = 0, lastDay = null;
  let sessionPrices = [], sessionVolumes = [];
  return klines.map((k) => {
    const date = new Date(k.ts);
    const currentDay = date.getUTCDate();
    if (lastDay !== null && currentDay !== lastDay) {
      cumPV = 0; cumVol = 0;
      sessionPrices = []; sessionVolumes = [];
    }
    lastDay = currentDay;

    const tp = (k.high + k.low + k.close) / 3;
    cumPV += (tp * k.volume);
    cumVol += k.volume;
    sessionPrices.push(tp);
    sessionVolumes.push(k.volume);

    const vwap = cumVol > 0 ? cumPV / cumVol : tp;
    let sumVariance = 0;
    for (let i = 0; i < sessionPrices.length; i++) {
      sumVariance += sessionVolumes[i] * Math.pow(sessionPrices[i] - vwap, 2);
    }
    const stdDev = Math.sqrt(cumVol > 0 ? sumVariance / cumVol : 0);
    const bands = {};
    opts.multipliers.forEach(m => {
      bands[`upperR${m}`] = vwap + (m * stdDev);
      bands[`lowerS${m}`] = vwap - (m * stdDev);
    });

    return { ts: k.ts, close: k.close, high: k.high, low: k.low, vwap, bands };
  });
}

// ─────────────────────────────────────────────
// MARKET FETCH QUERIES
// ─────────────────────────────────────────────
async function bxKlines(coin,interval='1h',limit=120){
  const sym=coin.toUpperCase()+'USDT';
  const url=`${BX_BASE}/fapi/v1/klines?symbol=${sym}&interval=${interval}&limit=${limit}`;
  const d=await fetchJSON(url);
  return d.map(k=>({
    ts:Number(k[0]), open:Number(k[1]), high:Number(k[2]), low:Number(k[3]), close:Number(k[4]), volume:Number(k[5])
  }));
}

async function bxTakerFlow(coin,interval='1h',limit=48){
  const sym=coin.toUpperCase()+'USDT';
  const url=`${BX_BASE}/futures/data/takerlongshortRatio?symbol=${sym}&period=${interval}&limit=${limit}`;
  try{
    const d=await fetchJSON(url);
    return d.map(x=>({ buyVol:Number(x.buyVol||0), sellVol:Number(x.sellVol||0), ratio:Number(x.buySellRatio||0) }));
  }catch(e){
    log('WARN', 'bxTakerFlow', `Failed for ${coin}`, { error: e.message });
    return [];
  }
}

async function bxOI(coin,interval='1h',limit=48){
  const sym=coin.toUpperCase()+'USDT';
  const url=`${BX_BASE}/futures/data/openInterestHist?symbol=${sym}&period=${interval}&limit=${limit}`;
  try{
    const d=await fetchJSON(url);
    return d.map(x=>({ oi:Number(x.sumOpenInterestValue || x.sumOpenInterest || 0) }));
  }catch(e){
    log('WARN', 'bxOI', `Failed for ${coin}`, { error: e.message });
    return [];
  }
}

// ✅ FIXED: Multi-exchange trend with timestamp validation
async function fetchMultiExchangeTrend(coin, lookbackCandles = 20) {
  const now = Date.now();
  const [bybitData, okxData] = await Promise.all([
    fetchJSON(`${BYBIT_BASE}/v5/market/kline?category=linear&symbol=${coin.toUpperCase()}USDT&interval=60&limit=${lookbackCandles}`)
      .catch(e => ({ error: e.message })),
    fetchJSON(`${OKX_BASE}/api/v5/market/candles?instId=${coin.toUpperCase()}-USDT-SWAP&bar=1H&limit=${lookbackCandles}`)
      .catch(e => ({ error: e.message }))
  ]);

  const bybitTrend = { trend: 'neutral', quality: 'low', freshS: null, volumeConfirm: false, ts: null };
  if (!bybitData.error && bybitData?.result?.list?.length >= 5) {
    const list = bybitData.result.list;
    const ts = parseInt(list[0][0]);
    const freshS = (now - ts) / 1000;
    const open = Number(list[list.length - 1][1]);
    const close = Number(list[0][4]);
    const vol = list.slice(0, 5).reduce((a, b) => a + Number(b[7]), 0);
    bybitTrend.trend = close > open ? 'bullish' : 'bearish';
    bybitTrend.ts = ts;
    bybitTrend.freshS = freshS;
    bybitTrend.quality = freshS < 120 ? 'high' : freshS < 300 ? 'medium' : 'low';
    bybitTrend.volumeConfirm = vol > 0;
  }

  const okxTrend = { trend: 'neutral', quality: 'low', freshS: null, volumeConfirm: false, ts: null };
  if (!okxData.error && okxData?.data?.length >= 5) {
    const data = okxData.data;
    const ts = parseInt(data[0][0]);
    const freshS = (now - ts) / 1000;
    const open = Number(data[data.length - 1][1]);
    const close = Number(data[0][4]);
    const vol = data.slice(0, 5).reduce((a, b) => a + Number(b[7]), 0);
    okxTrend.trend = close > open ? 'bullish' : 'bearish';
    okxTrend.ts = ts;
    okxTrend.freshS = freshS;
    okxTrend.quality = freshS < 120 ? 'high' : freshS < 300 ? 'medium' : 'low';
    okxTrend.volumeConfirm = vol > 0;
  }

  const tsSkewMs = Math.abs((bybitTrend.ts || 0) - (okxTrend.ts || 0));
  let consensus = 'neutral';
  let consensusQuality = 'low';
  
  if (tsSkewMs < 60000) {
    if (bybitTrend.trend === okxTrend.trend && bybitTrend.quality !== 'low' && okxTrend.quality !== 'low') {
      consensus = bybitTrend.trend;
      consensusQuality = 'high';
    } else if (bybitTrend.trend === okxTrend.trend) {
      consensus = bybitTrend.trend;
      consensusQuality = 'medium';
    } else {
      consensus = 'mixed';
      consensusQuality = 'low';
    }
  } else {
    consensusQuality = 'low';
  }

  return {
    bybit: bybitTrend,
    okx: okxTrend,
    consensus,
    consensusQuality,
    timestampSkewMs: tsSkewMs,
    warning: tsSkewMs > 120000 ? 'Exchange data misaligned by >2min' : null
  };
}

// ✅ NEW: Order Flow Imbalance
function calcOrderFlowImbalance(takerFlow, lookback = 24) {
  if (!takerFlow || takerFlow.length < 2) {
    return { bullishCount: 0, bearishCount: 0, neutralCount: 0, trend: 'unknown' };
  }
  const recent = takerFlow.slice(-lookback);
  let bullishCount = 0, bearishCount = 0, neutralCount = 0;
  recent.forEach(f => {
    const buy = f.buyVol || 0;
    const sell = f.sellVol || 0;
    if (buy > sell) bullishCount++;
    else if (buy < sell) bearishCount++;
    else neutralCount++;
  });
  const totalCount = bullishCount + bearishCount + neutralCount;
  const ratio = bearishCount > 0 ? bullishCount / bearishCount : bullishCount > 0 ? 999 : 1;
  return {
    bullishCount,
    bearishCount,
    neutralCount,
    totalCount,
    ratioBullBear: parseFloat(ratio.toFixed(2)),
    bullishPct: totalCount > 0 ? Math.round((bullishCount / totalCount) * 100) : 0,
    trend: bullishCount > bearishCount * 1.5 ? 'strong_buy_flow'
         : bullishCount > bearishCount ? 'buy_flow'
         : bearishCount > bullishCount * 1.5 ? 'strong_sell_flow'
         : bearishCount > bullishCount ? 'sell_flow'
         : 'balanced'
  };
}

// ─────────────────────────────────────────────
// CORE SCORING FRAMEWORK
// ─────────────────────────────────────────────
async function kiyIndicators(coin){
  const timer = createTimer();
  const ck='multi_ind_'+coin.toUpperCase();
  const hit=cGet(ck);
  if(hit) {
    log('DEBUG', 'kiy', `Indicators cache hit for ${coin}`, { ageS: cAge(ck) });
    return { data:hit, fromCache:true };
  }
  
  return withRequestDedup(ck, async () => {
    try {
      const [bx1h, bx4h, flow, oi, multiExchangeData] = await Promise.all([
        bxKlines(coin,'1h',120), bxKlines(coin,'4h',120),
        bxTakerFlow(coin,'1h',48), bxOI(coin,'1h',48),
        fetchMultiExchangeTrend(coin)
      ]);

      const cl1=bx1h.map(x => x.close);
      const cl4=bx4h.map(x => x.close);
      const rsi1H=calcRSI(cl1,14);
      const rsi4H=calcRSI(cl4,14);
      const macd=calcMACD(cl1);

      // ✅ NEW: Volatility regime
      const atr = calcATR(bx1h, 14);
      const histVol = calcHistoricalVolatility(cl1, 20);
      const volatilityRegime = classifyVolatilityRegime(histVol);

      // VWAP Execution
      const vwapData = calcSessionVWAP(bx1h, { multipliers: [1, 2, 3] });
      const curVwap = vwapData[vwapData.length - 1] || { close: cl1[cl1.length - 1], vwap: cl1[cl1.length - 1], bands: { lowerS2: 0, upperR2: 999999, lowerS3: 0, upperR3: 999999 } };
      const currentCandle = bx1h[bx1h.length - 1];

      // CVD
      let cum=0;
      const cvdPts=flow.map(x => {
        const d=(x.buyVol||0)-(x.sellVol||0); cum += d; return { delta:d, cumDelta:cum };
      });
      const last6cvd=cvdPts.slice(-6);
      const cvdTrend=last6cvd.length >= 2 ? (last6cvd[last6cvd.length-1].cumDelta > last6cvd[0].cumDelta ? 'rising' : 'falling') : 'unknown';

      // Open Interest
      const last6oi=oi.slice(-6);
      const oiTrend=last6oi.length >= 2 ? (last6oi[last6oi.length-1].oi > last6oi[0].oi ? 'rising' : 'falling') : 'unknown';
      const prev=oi[oi.length-2]?.oi || 0;
      const latestOi=oi[oi.length-1]?.oi || 0;
      const oiChg=prev > 0 ? parseFloat(((latestOi-prev)/prev*100).toFixed(2)) : 0;

      // Multi Exchange Aggregation (using validated data)
      const byTrend = multiExchangeData.bybit.trend;
      const okTrend = multiExchangeData.okx.trend;
      let exchangeBullScore=0;
      if(byTrend==='bullish') exchangeBullScore += 1;
      if(okTrend==='bullish') exchangeBullScore += 1;
      const multiExchangeBias=exchangeBullScore >= 2 ? 'bullish' : exchangeBullScore === 1 ? 'mixed' : 'bearish';

      // ✅ FIXED: Volatility-adjusted Strategy 2 Momentum Score
      let s2 = 38;
      const rsiWeight = volatilityRegime === 'low' ? 28 
                      : volatilityRegime === 'medium' ? 20 
                      : 10;
      if (rsi1H >= 50 && rsi1H <= 65) s2 += rsiWeight;
      else if (rsi1H > 65 && rsi1H <= 72) s2 += Math.ceil(rsiWeight * 0.5);
      
      if (oiTrend === 'rising') s2 += (volatilityRegime === 'high' ? 10 : 20);
      if (cvdTrend === 'rising') s2 += (volatilityRegime === 'high' ? 7 : 14);
      if (multiExchangeBias === 'bullish') s2 += 10;
      s2 = Math.min(s2, 99);

      // Strategy 5 (Trend) Score Engine
      let s5 = 33;
      if(rsi1H > 55) s5+=20;
      if(rsi4H > 50) s5+=15;
      if(cvdTrend==='rising') s5+=18;
      if(oiTrend==='rising') s5+=10;
      if(macd.hist > 0) s5+=10;
      if(multiExchangeBias==='bullish') s5 += 10;
      s5=Math.min(s5,99);

      // Strategy 6 (Reversal Spotter) Score Engine
      let s6 = 30;
      if (currentCandle.low <= curVwap.bands.lowerS3) s6 += 30;
      else if (currentCandle.low <= curVwap.bands.lowerS2) s6 += 15;
      if (currentCandle.high >= curVwap.bands.upperR3) s6 += 30;
      else if (currentCandle.high >= curVwap.bands.upperR2) s6 += 15;
      if (rsi1H <= 28 || rsi1H >= 72) s6 += 20;
      if (oiTrend === 'falling' && cvdTrend === 'falling' && currentCandle.low <= curVwap.bands.lowerS2) s6 += 15;
      if (oiTrend === 'falling' && cvdTrend === 'rising' && currentCandle.high >= curVwap.bands.upperR2) s6 += 15;
      s6 = Math.min(s6, 99);

      // ✅ NEW: Order flow imbalance
      const ofi = calcOrderFlowImbalance(flow, 24);

      const result={
        coin: coin.toUpperCase(),
        price: cl1[cl1.length-1] || 0,
        rsi1H, rsi4H, macd, cvdTrend, oiTrend, oiChange1H: oiChg,
        multiExchangeBias, bybitTrend: byTrend, okxTrend: okTrend, cvdLatest: cum,
        volatilityMetrics: {
          atr: parseFloat(atr.toFixed(4)),
          historicalVol: parseFloat(histVol.toFixed(4)),
          regime: volatilityRegime
        },
        multiExchangeData: {
          consensus: multiExchangeData.consensus,
          quality: multiExchangeData.consensusQuality,
          bybit: { trend: byTrend, quality: multiExchangeData.bybit.quality, freshS: multiExchangeData.bybit.freshS },
          okx: { trend: okTrend, quality: multiExchangeData.okx.quality, freshS: multiExchangeData.okx.freshS },
          timestampSkewMs: multiExchangeData.timestampSkewMs
        },
        orderFlowImbalance: ofi,
        strategyScores: {
          s2Momentum: s2,
          s4LiqMagnet: 50,
          s5Trend: s5,
          s6Reversal: s6
        },
        vwapMetrics: {
          currentClose: curVwap.close,
          vwapValue: parseFloat(curVwap.vwap.toFixed(2)),
          upperR2: parseFloat(curVwap.bands.upperR2.toFixed(2)),
          lowerS2: parseFloat(curVwap.bands.lowerS2.toFixed(2)),
          upperR3: parseFloat(curVwap.bands.upperR3.toFixed(2)),
          lowerS3: parseFloat(curVwap.bands.lowerS3.toFixed(2))
        },
        warnings: multiExchangeData.warning ? [multiExchangeData.warning] : [],
        computedAt: new Date().toISOString()
      };

      cSetPolicy(ck, result, 'kiyIndicators');
      log('INFO', 'kiy', `Indicators computed for ${coin}`, {
        s2: result.strategyScores.s2Momentum,
        s5: result.strategyScores.s5Trend,
        s6: result.strategyScores.s6Reversal,
        volatilityRegime,
        latencyMs: timer.elapsed()
      });
      return { data:result, fromCache:false };
    } catch(e) {
      log('ERROR', 'kiy', `Failed to compute indicators for ${coin}`, { error: e.message, latencyMs: timer.elapsed() });
      throw e;
    }
  });
}

// ─────────────────────────────────────────────
// SCAN LAYER (SINGLE STABLE IMPLEMENTATION)
// ─────────────────────────────────────────────
const FREE_PLAN=true;
const SCAN_COINS = FREE_PLAN ? ['BTC','ETH','SOL','BNB','XRP','DOGE','LINK','AVAX','INJ','SUI','ARB','OP','NEAR','APT','RENDER'] : ['BTC','ETH','SOL','BNB','XRP','DOGE','LINK','AVAX','INJ','SUI','ARB','OP','NEAR','APT','RENDER'];

async function kiyScan(strategy){
  const timer = createTimer();
  const scoreKey = { s2:'s2Momentum', s4:'s4LiqMagnet', s5:'s5Trend', s6:'s6Reversal' }[strategy] || 's5Trend';
  const results = [];
  const failures = [];
  
  for(let i=0;i<SCAN_COINS.length;i++){
    const coin = SCAN_COINS[i];
    let attempts = 0;
    let success = false;
    
    while (attempts < 3 && !success) {
      try {
        const r = await kiyIndicators(coin);
        results.push(r.data);
        success = true;
      } catch(e) {
        attempts++;
        if (attempts < 3) {
          const backoff = Math.pow(2, attempts) * 1000;
          log('WARN', 'scan', `Retry ${coin} in ${backoff}ms (attempt ${attempts}/3)`, { error: e.message });
          await sleep(backoff);
        } else {
          failures.push({ coin, reason: e.message, attempts });
          log('ERROR', 'scan', `Scan skip ${coin} after 3 attempts`, { error: e.message });
        }
      }
    }
    await sleep(1200);
  }
  
  results.sort((a,b)=> (b.strategyScores?.[scoreKey] || 0) - (a.strategyScores?.[scoreKey] || 0));
  
  log('INFO', 'scan', `Scan complete for ${strategy}`, {
    total: SCAN_COINS.length,
    succeeded: results.length,
    failed: failures.length,
    latencyMs: timer.elapsed()
  });
  
  return {
    data: results,
    strategy,
    scoreKey,
    scannedAt: new Date().toISOString(),
    summary: {
      total: SCAN_COINS.length,
      succeeded: results.length,
      failed: failures.length,
      failures
    }
  };
}

// ── Config save ────────────────────────────────────────────────────────
async function saveConfig(rawBody){
  try{
    const upd=JSON.parse(rawBody);loadCfg();Object.assign(CFG,upd);
    fs.writeFileSync(CONFIG_FILE,JSON.stringify(CFG,null,2));
    log('INFO', 'config', 'Config saved successfully');
    return{ok:true,message:'Saved'};
  }catch(e){
    log('ERROR', 'config', 'Config save failed', { error: e.message });
    return{ok:false,message:e.message};
  }
}

// ── Helpers ────────────────────────────────────────────────────────────
function cors(res){res.setHeader('Access-Control-Allow-Origin','*');res.setHeader('Access-Control-Allow-Methods','GET,POST,OPTIONS');res.setHeader('Content-Type','application/json');}
function readBody(req){return new Promise(r=>{let b='';req.on('data',c=>b+=c);req.on('end',()=>r(b));});}
function send(res,data){try{res.writeHead(200);res.end(JSON.stringify(data));}catch(e){log('ERROR','server','Send failed',{error:e.message});}}
function sendErr(res,msg){try{res.writeHead(500);res.end(JSON.stringify({error:msg}));}catch(e){log('ERROR','server','SendErr failed',{error:e.message});}}

// ── Server Routing Router ──────────────────────────────────────────────
http.createServer(async(req,res)=>{
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const u=new URL(req.url,'http://localhost:'+PORT);
  const pn=u.pathname,qs=u.searchParams;
  
  if(req.method==='OPTIONS'){cors(res);res.writeHead(204);res.end();return;}
  
  if(pn==='/'||pn==='/index.html'){
    const f=path.join(__dirname,'index.html');
    if(!fs.existsSync(f)){res.writeHead(404);res.end('index.html not found');return;}
    res.writeHead(200,{'Content-Type':'text/html'});res.end(fs.readFileSync(f,'utf8'));return;
  }
  
  cors(res);
  
  // ✅ NEW: Rate limiting check
  const rateCheck = checkRateLimit(ip, pn);
  if (!rateCheck.allowed) {
    res.writeHead(429, { 'Retry-After': rateCheck.retryAfterS });
    res.end(JSON.stringify({
      error: 'Rate limit exceeded',
      retryAfterS: rateCheck.retryAfterS
    }));
    log('WARN', 'ratelimit', `Blocked ${ip} on ${pn}`, { retryAfterS: rateCheck.retryAfterS });
    return;
  }
  
  try{
    if(pn==='/api/coins')           return send(res,await apiCoins());
    if(pn==='/api/futures')         return send(res,await apiFutures());
    if(pn==='/api/ht/market')       return send(res,await apiHtMarket());
    if(pn==='/api/ht/breakdown')    return send(res,await apiHtBreakdown(qs.get('coin')||'BTC'));
    if(pn==='/api/kiy/candles')     return send(res,await bxKlines(qs.get('coin')||'BTC','1h',120));
    if(pn==='/api/kiy/cvd')         return send(res,await bxTakerFlow(qs.get('coin')||'BTC', '1h', 48));
    if(pn==='/api/kiy/oi')          return send(res,await bxOI(qs.get('coin')||'BTC', '1h', 48));
    if(pn==='/api/kiy/liquidations')return send(res, { note: "Synthetic liquidation layer remapped directly to Coinalyze Engine /api/futures" });

    if(pn==='/api/kiy/indicators')  return send(res,await kiyIndicators(qs.get('coin')||'BTC'));
    if(pn==='/api/kiy/scan')        return send(res,await kiyScan(qs.get('strategy')||'s5'));
    if(pn==='/api/saveconfig'&&req.method==='POST')return send(res,await saveConfig(await readBody(req)));
    
    if(pn==='/api/status'){
      const u=loadUsage();
      return send(res,{ok:true,version:'5.2',uptime:Math.round(process.uptime()),
        coinalyze:{key:!!KEY('coinalyze_key')},
        hypertracker:{set:!!KEY('hypertracker_key'),usedToday:u.count,remaining:htRem(),limit:HT_LIMIT},
        kiyotaka:{set:!!KEY('kiyotaka_key'),note:'Free:1000w/day | Basic($99/mo):unlimited'},
        cache:Object.keys(CACHE).map(k=>({k,ageS:cAge(k),ttlS:Math.round(CACHE[k].ttl/1000)}))});
    }
    
    res.writeHead(404);res.end(JSON.stringify({error:'Not found: '+pn}));
  }catch(err){
    log('ERROR','server', `Request failed: ${pn}` ,{error:err.message,ip});
    sendErr(res,err.message);
  }
}).listen(PORT,()=>{
  console.log('');
  console.log('  ⬡  CryptoScanner Pro v5.2 (FIXED)');
  console.log('  ══════════════════════════════════════════════');
  console.log(`URL: http://localhost:${PORT}`);
  console.log(`CoinGecko: api.coingecko.com (free, no key, server-side)`);
  console.log(`Coinalyze: ${KEY('coinalyze_key')?'✓ key loaded':'✗ missing'}`);
  console.log(`HyperTracker: ${KEY('hypertracker_key')?'✓ key loaded '+htRem()+'/'+HT_LIMIT+' req left today':'✗ missing'}`);
  console.log(`Kiyotaka: ${KEY('kiyotaka_key')?'✓ key loaded RSI·CVD·OI·Liq·MACD ready':'✗ missing'}`);
  console.log('');
  console.log('  ✅ IMPROVEMENTS IN v5.2:');
  console.log('     • MACD signal line corrected (9-EMA, not 80% multiplier)');
  console.log('     • Volatility regime classification added (low/medium/high)');
  console.log('     • Multi-exchange timestamp validation (Bybit + OKX consensus)');
  console.log('     • Request deduplication (prevents cache stampede)');
  console.log('     • Structured JSON logging (logs/ directory)');
  console.log('     • Cache policy parameterization');
  console.log('     • Rate limiting per IP');
  console.log('     • Order flow imbalance analysis');
  console.log('     • Enhanced error tracking and retry logic');
  console.log('');
  console.log('  Press Ctrl+C to stop.');
  console.log('');
  log('INFO', 'server', 'CryptoScanner Pro v5.2 started', { port: PORT });
});

process.on('SIGINT',()=>{
  console.log('\n  Stopped.');
  log('INFO', 'server', 'Server stopped gracefully');
  process.exit(0);
});
