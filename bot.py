import os
import time
import random
import json
import logging
from datetime import datetime
from decimal import Decimal

import requests
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account

# ===========================
# Logging
# ===========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("volbot")

# ===========================
# Env
# ===========================
load_dotenv()

BSC_RPC_URL = os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org/")
MIN_GWEI = float(os.getenv("MIN_GWEI", "1.2"))          # gás baixo, mas realista na BSC
GAS_BUFFER = float(os.getenv("GAS_BUFFER", "1.05"))     # 5% buffer em estimativas
TOKEN_ADDRESS = os.getenv("TOKEN_ADDRESS", "0xe08b716fffcc0410da0392500c6a88fe0accd819")

# Viés de tamanho das operações
POSITION_PCT_MIN = float(os.getenv("POSITION_PCT_MIN", "0.02"))   # 2%
POSITION_PCT_MAX = float(os.getenv("POSITION_PCT_MAX", "0.03"))   # 3%

# Slippage (para minOut)
SLIPPAGE_TOLERANCE = float(os.getenv("SLIPPAGE_TOLERANCE", "0.70"))  # 30% (0.70 => aceita 70% do esperado)

# Intervalos/ciclos
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "2"))  # frequência do loop (s)
# janela do hold (min/max) em segundos – curtinha para day-trade/volume
HOLD_MIN_LO = int(os.getenv("HOLD_MIN_LO", "60"))
HOLD_MIN_HI = int(os.getenv("HOLD_MIN_HI", "120"))
# timeout de posição para forçar uma venda parcial se não bateu alvo
TIMEOUT_LO = int(os.getenv("TIMEOUT_LO", "70"))
TIMEOUT_HI = int(os.getenv("TIMEOUT_HI", "120"))
# defasagens entre operações
INTER_WALLET_GAP_LO = int(os.getenv("INTER_WALLET_GAP_LO", "60"))
INTER_WALLET_GAP_HI = int(os.getenv("INTER_WALLET_GAP_HI", "120"))
GLOBAL_COOLDOWN = int(os.getenv("GLOBAL_COOLDOWN", "15"))

# Alvo de lucro
PROFIT_TARGET = float(os.getenv("PROFIT_TARGET", "1.15"))  # +15%

# Endereços (checksum)
def to_cs(addr: str) -> str:
    return Web3.to_checksum_address(addr)

ROUTER_V2 = to_cs(os.getenv("PANCAKE_ROUTER_V2", "0x10ED43C718714eb63d5aA57B78B54704E256024E"))
WBNB      = to_cs(os.getenv("WBNB_ADDRESS",        "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"))
USDT      = to_cs(os.getenv("USDT_ADDRESS",        "0x55d398326f99059fF775485246999027B3197955"))
TOKEN     = to_cs(TOKEN_ADDRESS)

# ===========================
# Web3
# ===========================
w3 = Web3(Web3.HTTPProvider(BSC_RPC_URL))
if not w3.is_connected():
    log.error("Falha ao conectar à BSC mainnet.")
    raise SystemExit(1)
log.info("Conectado à BSC mainnet")

# ===========================
# ABIs
# ===========================
PancakeSwapRouterABI = json.loads("""
[
  {
    "type":"function","stateMutability":"payable","name":"swapExactETHForTokensSupportingFeeOnTransferTokens",
    "inputs":[
      {"name":"amountOutMin","type":"uint256"},
      {"name":"path","type":"address[]"},
      {"name":"to","type":"address"},
      {"name":"deadline","type":"uint256"}
    ],"outputs":[]
  },
  {
    "type":"function","stateMutability":"nonpayable","name":"swapExactTokensForETHSupportingFeeOnTransferTokens",
    "inputs":[
      {"name":"amountIn","type":"uint256"},
      {"name":"amountOutMin","type":"uint256"},
      {"name":"path","type":"address[]"},
      {"name":"to","type":"address"},
      {"name":"deadline","type":"uint256"}
    ],"outputs":[]
  },
  {
    "type":"function","stateMutability":"view","name":"getAmountsOut",
    "inputs":[{"name":"amountIn","type":"uint256"},{"name":"path","type":"address[]"}],
    "outputs":[{"name":"amounts","type":"uint256[]"}]
  }
]
""")

ERC20_ABI = json.loads("""
[
  {"constant":true,"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"},
  {"constant":false,"inputs":[{"name":"spender","type":"address"},{"name":"amount","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"type":"function"},
  {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
  {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
  {"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"spender","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"type":"function"}
]
""")

router = w3.eth.contract(address=ROUTER_V2, abi=PancakeSwapRouterABI)
token  = w3.eth.contract(address=TOKEN,      abi=ERC20_ABI)

# Token meta
try:
    TOKEN_DECIMALS = token.functions.decimals().call()
except Exception:
    TOKEN_DECIMALS = 18
try:
    TOKEN_SYMBOL = token.functions.symbol().call()
except Exception:
    TOKEN_SYMBOL = "TOKEN"
DECIMAL_FACTOR = 10 ** TOKEN_DECIMALS

# ===========================
# DexScreener (preço apenas indicativo)
# ===========================
DEXSCREENER_API = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN}"
_price_cache = {"t": 0, "v": None, "ttl": 8}

def get_token_price_usd():
    now = time.time()
    if now - _price_cache["t"] <= _price_cache["ttl"] and _price_cache["v"] is not None:
        return _price_cache["v"]
    try:
        r = requests.get(DEXSCREENER_API, timeout=8)
        r.raise_for_status()
        data = r.json()
        pair = data.get('pairs', [{}])[0]
        price = float(pair.get('priceUsd', 0))
        _price_cache.update({"t": now, "v": price if price > 0 else None})
        return _price_cache["v"]
    except Exception as e:
        log.error(f"Erro DexScreener: {e}")
        return _price_cache["v"]

# ===========================
# Carteiras
# ===========================
wallets = []
for i in range(1, 10 + 1):
    pk = os.getenv(f"WALLET{i}_PRIVATE_KEY")
    if not pk:
        continue
    try:
        acct = Account.from_key(pk)
        wallets.append({
            "private_key": pk,
            "address": acct.address,
            "nonce": w3.eth.get_transaction_count(acct.address),
        })
        log.info(f"Carteira {i} carregada: {acct.address}")
    except Exception as e:
        log.warning(f"Chave privada inválida para WALLET{i}_PRIVATE_KEY: {e}")

if not wallets:
    log.error("Nenhuma carteira válida no .env.")
    raise SystemExit(1)

# ===========================
# Estado por carteira
# ===========================
def now() -> float:
    return time.time()

def jitter(a: int, pct: float = 0.10) -> int:
    d = int(a * pct)
    return max(a + random.randint(-d, d), 0)

def gwei_to_wei(g: float) -> int:
    return Web3.to_wei(g, "gwei")

def get_gas_price() -> int:
    try:
        # força gas baixo, mas fixo/configurável
        return gwei_to_wei(MIN_GWEI)
    except Exception:
        return gwei_to_wei(1.2)

def bnb_balance(addr: str) -> int:
    return w3.eth.get_balance(addr)

def token_balance(addr: str) -> int:
    try:
        return token.functions.balanceOf(addr).call()
    except Exception:
        return 0

def fmt_bnb(wei: int) -> str:
    return f"{(wei/1e18):.6f}"

def fmt_tok(amt: int) -> str:
    return f"{(amt/DECIMAL_FACTOR):.6f}"

def approve_if_needed(wallet: dict, needed: int) -> bool:
    try:
        allowance = token.functions.allowance(wallet["address"], ROUTER_V2).call()
        if allowance >= needed:
            return True
        # aprova "infinito" para não travar volume
        max_uint = (1 << 256) - 1
        tx = token.functions.approve(ROUTER_V2, max_uint).build_transaction({
            "from": wallet["address"],
            "gas": 60000,
            "gasPrice": get_gas_price(),
            "nonce": wallet["nonce"],
            "chainId": 56
        })
        signed = w3.eth.account.sign_transaction(tx, wallet["private_key"])
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)
        rc = w3.eth.wait_for_transaction_receipt(txh)
        if rc.status != 1:
            log.error(f"Aprovação revertida: {txh.hex()}")
            return False
        wallet["nonce"] += 1
        return True
    except Exception as e:
        log.error(f"approve falhou: {e}")
        return False

def best_buy_quote(amount_bnb: int):
    best = 0
    best_path = [WBNB, TOKEN]
    try:
        a1 = router.functions.getAmountsOut(amount_bnb, [WBNB, TOKEN]).call()[-1]
        if a1 > best:
            best = a1
            best_path = [WBNB, TOKEN]
    except Exception:
        pass
    try:
        a2 = router.functions.getAmountsOut(amount_bnb, [WBNB, USDT, TOKEN]).call()[-1]
        if a2 > best:
            best = a2
            best_path = [WBNB, USDT, TOKEN]
    except Exception:
        pass
    return best, best_path

def best_sell_quote(amount_token: int):
    best = 0
    best_path = [TOKEN, WBNB]
    try:
        a1 = router.functions.getAmountsOut(amount_token, [TOKEN, WBNB]).call()[-1]
        if a1 > best:
            best = a1
            best_path = [TOKEN, WBNB]
    except Exception:
        pass
    try:
        a2 = router.functions.getAmountsOut(amount_token, [TOKEN, USDT, WBNB]).call()[-1]
        if a2 > best:
            best = a2
            best_path = [TOKEN, USDT, WBNB]
    except Exception:
        pass
    return best, best_path

def buy(wallet: dict, bnb_amount: int) -> int:
    try:
        exp_tokens, path = best_buy_quote(bnb_amount)
        if exp_tokens <= 0:
            log.error("Sem rota/liquidez para compra.")
            return 0
        min_out = int(exp_tokens * SLIPPAGE_TOLERANCE)
        deadline = int(time.time()) + 600
        bal_before = token_balance(wallet["address"])
        tx = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
            min_out, path, wallet["address"], deadline
        ).build_transaction({
            "from": wallet["address"],
            "value": bnb_amount,
            "gas": 150000,
            "gasPrice": get_gas_price(),
            "nonce": wallet["nonce"],
            "chainId": 56
        })
        signed = w3.eth.account.sign_transaction(tx, wallet["private_key"])
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)
        rc = w3.eth.wait_for_transaction_receipt(txh)
        if rc.status != 1:
            log.error(f"Compra revertida: {txh.hex()}")
            return 0
        wallet["nonce"] += 1
        got = token_balance(wallet["address"]) - bal_before
        if got <= 0:
            log.error("Compra executou, mas saldo não aumentou (fee/antibot?).")
            return 0
        log.info(f"BUY {wallet['address']}: {fmt_bnb(bnb_amount)} BNB -> {fmt_tok(got)} {TOKEN_SYMBOL} | Tx: {txh.hex()}")
        return got
    except Exception as e:
        log.error(f"Erro buy: {e}")
        return 0

def sell(wallet: dict, token_amount: int, reason: str = "") -> int:
    if token_amount <= 0:
        return 0
    try:
        if not approve_if_needed(wallet, token_amount):
            return 0
        exp_bnb, path = best_sell_quote(token_amount)
        if exp_bnb <= 0:
            log.error("Sem rota/liquidez para venda.")
            return 0
        min_out = int(exp_bnb * SLIPPAGE_TOLERANCE)
        deadline = int(time.time()) + 600
        bnb_before = bnb_balance(wallet["address"])
        tx = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
            token_amount, min_out, path, wallet["address"], deadline
        ).build_transaction({
            "from": wallet["address"],
            "gas": 180000,
            "gasPrice": get_gas_price(),
            "nonce": wallet["nonce"],
            "chainId": 56
        })
        signed = w3.eth.account.sign_transaction(tx, wallet["private_key"])
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)
        rc = w3.eth.wait_for_transaction_receipt(txh)
        if rc.status != 1:
            log.error(f"Venda revertida: {txh.hex()}")
            return 0
        wallet["nonce"] += 1
        bnb_after = bnb_balance(wallet["address"])
        gross = bnb_after - bnb_before
        log.info(f"SELL {wallet['address']}: {fmt_tok(token_amount)} {TOKEN_SYMBOL} -> gross ~ {fmt_bnb(gross)} BNB | {reason} | Tx: {txh.hex()}")
        return gross
    except Exception as e:
        log.error(f"Erro sell: {e}")
        return 0

# Estado por carteira
wallet_state = {}
for w in wallets:
    addr = w["address"]
    tbal = token_balance(addr)
    bbnb = bnb_balance(addr)
    log.info(f"Saldo {addr}: {fmt_bnb(bbnb)} BNB | {TOKEN_SYMBOL}: {fmt_tok(tbal)}")
    wallet_state[addr] = {
        "avg_buy_price": None,       # preço médio em USD (indicativo)
        "last_buy_ts": 0.0,
        "last_sell_ts": 0.0,
        "min_hold_secs": random.randint(HOLD_MIN_LO, HOLD_MIN_HI),
        "timeout_secs": random.randint(TIMEOUT_LO, TIMEOUT_HI),
        "sell_streak": 0,
        "buy_streak": 0,
        "next_allowed_ts": 0.0,
    }
    # Se já tem posição de token, inicia com um tempo para simular hold
    if tbal > 0:
        log.info(f"[BOOTSTRAP] {addr} tem posição inicial ({fmt_tok(tbal)} {TOKEN_SYMBOL}). Hold min ~{wallet_state[addr]['min_hold_secs']}s, timeout ~{wallet_state[addr]['timeout_secs']}s.")

# Cooldown global
global_next_ts = 0.0

def can_act(addr: str) -> bool:
    return now() >= wallet_state[addr]["next_allowed_ts"] and now() >= global_next_ts

def schedule_after(addr: str, lo: int, hi: int):
    delay = random.randint(lo, hi)
    wallet_state[addr]["next_allowed_ts"] = now() + delay
    return delay

def schedule_global(cool: int):
    global global_next_ts
    global_next_ts = now() + cool

def pick_buy_size(addr: str) -> int:
    bal = bnb_balance(addr)
    # reserva ~ 2 * 200k gas * gasPrice
    gas_reserve = int(200000 * get_gas_price() * 2)
    if bal <= gas_reserve:
        return 0
    frac = random.uniform(POSITION_PCT_MIN, POSITION_PCT_MAX)
    amt = int((bal - gas_reserve) * frac)
    # mínimo absoluto
    min_abs = Web3.to_wei(0.00005, "ether")
    if amt < min_abs:
        amt = min_abs if bal - gas_reserve > min_abs else 0
    return amt

def try_buy(addr: str, w: dict) -> bool:
    if not can_act(addr):
        return False
    bnb_amt = pick_buy_size(addr)
    if bnb_amt <= 0:
        return False
    got = buy(w, bnb_amt)
    if got > 0:
        # atualiza preço médio indicativo
        px = get_token_price_usd()
        st = wallet_state[addr]
        if px:
            if st["avg_buy_price"] is None:
                st["avg_buy_price"] = px
            else:
                # média móvel simples (peso 0.5)
                st["avg_buy_price"] = (st["avg_buy_price"] + px) / 2.0
        st["last_buy_ts"] = now()
        st["buy_streak"] += 1
        st["sell_streak"] = 0
        # agenda próximos
        schedule_after(addr, INTER_WALLET_GAP_LO, INTER_WALLET_GAP_HI)
        schedule_global(GLOBAL_COOLDOWN)
        return True
    return False

def try_sell(addr: str, w: dict, reason: str) -> bool:
    if not can_act(addr):
        return False
    tbal = token_balance(addr)
    if tbal <= 0:
        return False
    # vende parcial (ondas) 75–95%
    frac = random.uniform(0.75, 0.95)
    amount = max(int(tbal * frac), 1)
    gross = sell(w, amount, reason=reason)
    if gross > 0:
        st = wallet_state[addr]
        st["last_sell_ts"] = now()
        st["sell_streak"] += 1
        # reduzir buy_streak (não zera totalmente p/ manter viés pró-compra quando possível)
        st["buy_streak"] = max(st["buy_streak"] - 1, 0)
        # agenda próximos
        schedule_after(addr, INTER_WALLET_GAP_LO, INTER_WALLET_GAP_HI)
        schedule_global(GLOBAL_COOLDOWN)
        # mantém resto de saldo pra próxima onda
        rem = token_balance(addr)
        log.info(f"[WAVE] Venda PARCIAL {addr}: frac={frac:.3f}, remanescente ~ {fmt_tok(rem)} {TOKEN_SYMBOL}")
        return True
    return False

def must_buy_bias(addr: str) -> bool:
    """
    Regras de viés pró-compra:
      - Se sell_streak >= 2 => deve comprar até buy_streak >= 2.
      - Se não vendeu recentemente, pode comprar mesmo tendo pequenos restos de saldo.
    """
    st = wallet_state[addr]
    if st["sell_streak"] >= 2 and st["buy_streak"] < 2:
        return True
    return False

def price_target_hit(addr: str) -> bool:
    px = get_token_price_usd()
    st = wallet_state[addr]
    return (px is not None) and (st["avg_buy_price"] is not None) and (px >= st["avg_buy_price"] * PROFIT_TARGET)

def hold_timeout(addr: str) -> bool:
    st = wallet_state[addr]
    last_buy = st["last_buy_ts"]
    if last_buy == 0:
        return False
    return (now() - last_buy) >= st["timeout_secs"]

def hold_min_elapsed(addr: str) -> bool:
    st = wallet_state[addr]
    last_buy = st["last_buy_ts"]
    if last_buy == 0:
        # se nunca comprou nesta sessão, pode vender se tiver saldo inicial (mas o viés pró-compra segura isso)
        return True
    return (now() - last_buy) >= st["min_hold_secs"]

def maybe_reset_random_timers(addr: str):
    # Aleatoriza novamente as janelas para evitar sincronizar
    wallet_state[addr]["min_hold_secs"] = random.randint(HOLD_MIN_LO, HOLD_MIN_HI)
    wallet_state[addr]["timeout_secs"]  = random.randint(TIMEOUT_LO, TIMEOUT_HI)

# ===========================
# Loop principal
# ===========================
rr = 0
while True:
    price = get_token_price_usd()
    if price:
        log.info(f"Preço aprox (USD) ~ {price:.8f}")
    # percorre apenas UMA carteira por iteração para manter defasagem
    w = wallets[rr % len(wallets)]
    rr += 1
    addr = w["address"]
    st = wallet_state[addr]
    tbal = token_balance(addr)

    # 1) Se viés manda comprar (após 2 vendas seguidas), prioriza BUY mesmo tendo resto de token
    if must_buy_bias(addr):
        acted = try_buy(addr, w)
        if acted:
            maybe_reset_random_timers(addr)
    else:
        # 2) Caso tenha token, verifica alvo/timeout, mas só vende se já passou hold mínimo
        acted = False
        if tbal > 0 and hold_min_elapsed(addr):
            if price_target_hit(addr):
                acted = try_sell(addr, w, reason="alvo atingido")
                if acted:
                    maybe_reset_random_timers(addr)
            elif hold_timeout(addr):
                acted = try_sell(addr, w, reason="timeout")
                if acted:
                    maybe_reset_random_timers(addr)

        # 3) Se não vendeu (ou se não tinha saldo), tenta comprar
        if not acted:
            acted = try_buy(addr, w)
            if acted:
                maybe_reset_random_timers(addr)

    time.sleep(MONITOR_INTERVAL)
