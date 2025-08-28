import os
import time
import random
import logging
from typing import Optional, List, Dict
from collections import deque

from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account.signers.local import LocalAccount

# Load .env variables
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

###############################################
# LOGGING
###############################################
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger.setLevel(LOG_LEVEL)
VERBOSE = os.getenv("VERBOSE", "0") in ("1", "true", "TRUE")

def vinfo(msg: str):
    if VERBOSE:
        logger.info(msg)
    else:
        logger.debug(msg)

###############################################
# ENV / CONFIG
###############################################
BSC_RPC_URL = os.getenv("BSC_RPC_URL", "https://bsc-dataseed2.defibit.io/")
TOKEN_ADDRESS = os.getenv("TOKEN_ADDRESS", "0x0000000000000000000000000000000000000000")
ROUTER = Web3.to_checksum_address(os.getenv("PANCAKE_ROUTER_V2", "0x10ED43C718714eb63d5aA57B78B54704E256024E"))
WBNB = Web3.to_checksum_address(os.getenv("WBNB_ADDRESS", "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"))
USDT = Web3.to_checksum_address(os.getenv("USDT_ADDRESS", "0x55d398326f99059fF775485246999027B3197955"))

# Timing (in seconds)
STAGGER_MIN = float(os.getenv("STAGGER_MIN", 3))
STAGGER_MAX = float(os.getenv("STAGGER_MAX", 6))
HOLD_MIN = float(os.getenv("HOLD_MIN", 10))
HOLD_MAX = float(os.getenv("HOLD_MAX", 18))

# Trading
SLIPPAGE_TOLERANCE = float(os.getenv("SLIPPAGE_TOLERANCE", 0.70))  # 70% for taxed tokens
POSITION_PCT_MIN = float(os.getenv("POSITION_PCT_MIN", 0.01))  # 1%
POSITION_PCT_MAX = float(os.getenv("POSITION_PCT_MAX", 0.3))  # Max 30%
PROFIT_TARGET = 1.05  # 5% profit
VOLUME_MODE = os.getenv("VOLUME_MODE", "0") in ("1", "true", "TRUE")

# Gas
MIN_GWEI = float(os.getenv("MIN_GWEI", 1.2))
GAS_BUFFER = float(os.getenv("GAS_BUFFER", 1.05))
GAS_RESERVE_BNB = float(os.getenv("GAS_RESERVE_BNB", 0.002))

# Price cache
PRICE_TTL = int(float(os.getenv("PRICE_TTL", 10)))

# Global for tracking last sale amount
last_received_bnb = 0

###############################################
# WEB3
###############################################
web3 = Web3(Web3.HTTPProvider(BSC_RPC_URL, request_kwargs={"timeout": 20}))
try:
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    logger.info("Middleware PoA aplicado com sucesso")
except Exception as e:
    logger.warning(f"Não foi possível aplicar geth_poa_middleware: {e}")

if not web3.is_connected():
    raise RuntimeError(f"Falha ao conectar no RPC BSC: {BSC_RPC_URL}")

# ABIs
ERC20_ABI = [
    {"name": "decimals", "outputs": [{"type": "uint8"}], "inputs": [], "stateMutability": "view", "type": "function"},
    {"name": "balanceOf", "outputs": [{"type": "uint256"}], "inputs": [{"name": "owner", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "allowance", "outputs": [{"type": "uint256"}], "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"name": "approve", "outputs": [{"type": "bool"}], "inputs": [{"name": "spender", "type": "address"}, {"name": "value", "type": "uint256"}], "stateMutability": "nonpayable", "type": "function"},
]

ROUTER_ABI = [
    {
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "path", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "swapExactETHForTokensSupportingFeeOnTransferTokens",
        "outputs": [],
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "stateMutability": "payable",
        "type": "function"
    },
    {
        "name": "swapExactTokensForETHSupportingFeeOnTransferTokens",
        "outputs": [],
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"}
        ],
        "stateMutability": "nonpayable",
        "type": "function"
    },
]

# Validate TOKEN_ADDRESS
try:
    token_contract = web3.eth.contract(address=Web3.to_checksum_address(TOKEN_ADDRESS), abi=ERC20_ABI)
    # Test contract by calling decimals
    token_contract.functions.decimals().call()
except Exception as e:
    logger.error(f"TOKEN_ADDRESS inválido ({TOKEN_ADDRESS}): {e}")
    raise ValueError(f"TOKEN_ADDRESS inválido. Configure corretamente no .env")

router = web3.eth.contract(address=ROUTER, abi=ROUTER_ABI)

# Token decimals
try:
    TOKEN_DECIMALS = token_contract.functions.decimals().call()
except Exception:
    TOKEN_DECIMALS = 18
try:
    USDT_DECIMALS = web3.eth.contract(address=USDT, abi=ERC20_ABI).functions.decimals().call()
except Exception:
    USDT_DECIMALS = 18

###############################################
# PRICE
###############################################
_TOKENUSD_CACHE = {"t": 0.0, "v": None}

def _now() -> float:
    return time.time()

def get_token_price_usd() -> Optional[float]:
    now = _now()
    if _TOKENUSD_CACHE["v"] is not None and now - _TOKENUSD_CACHE["t"] <= PRICE_TTL:
        return _TOKENUSD_CACHE["v"]
    try:
        one = 10 ** TOKEN_DECIMALS
        usd_out = 0
        try:
            usd_out = router.functions.getAmountsOut(one, [Web3.to_checksum_address(TOKEN_ADDRESS), USDT]).call()[-1]
        except Exception:
            try:
                usd_out = router.functions.getAmountsOut(one, [Web3.to_checksum_address(TOKEN_ADDRESS), WBNB, USDT]).call()[-1]
            except Exception:
                usd_out = 0
        if usd_out == 0:
            logger.warning("Falha ao obter preço do token; usando cache")
            return _TOKENUSD_CACHE["v"]
        usd = float(usd_out) / (10 ** USDT_DECIMALS)
        _TOKENUSD_CACHE.update({"t": now, "v": usd})
        vinfo(f"Preço do token atualizado: ${usd:.6f}")
        return usd
    except Exception as e:
        logger.error(f"Erro ao obter preço do token: {e}")
        return _TOKENUSD_CACHE["v"]

###############################################
# WALLETS
###############################################
def load_wallets_from_env() -> List[LocalAccount]:
    accs = []
    for i in range(1, 11):
        pk = os.getenv(f"WALLET{i}_PRIVATE_KEY")
        if pk and "your_private_key" not in pk:
            try:
                accs.append(web3.eth.account.from_key(pk))
            except Exception as e:
                logger.error(f"Chave inválida WALLET{i}: {e}")
    if not accs:
        raise RuntimeError("Nenhuma WALLET*_PRIVATE_KEY válida no .env")
    return accs

###############################################
# TX HELPERS
###############################################
def _get_max_priority_fee() -> int:
    try:
        return web3.eth.max_priority_fee
    except Exception:
        return int(1 * 1e9)  # Fallback to 1 Gwei

def _get_base_fee() -> int:
    try:
        return web3.eth.get_block('pending')['baseFeePerGas']
    except Exception:
        return int(5 * 1e9)  # Fallback to 5 Gwei

def send_tx(tx: dict, account: LocalAccount) -> dict:
    tx["nonce"] = web3.eth.get_transaction_count(account.address)
    try:
        # Estimate gas first
        gas_est = web3.eth.estimate_gas({**tx, "from": account.address})
        tx["gas"] = int(gas_est * 1.2)
    except Exception as e:
        logger.warning(f"Falha ao estimar gás: {e}. Usando gás padrão de 600000.")
        tx["gas"] = 600000

    # Use EIP-1559 parameters
    max_priority_fee = _get_max_priority_fee()
    base_fee = _get_base_fee()
    tx["maxPriorityFeePerGas"] = int(max_priority_fee * GAS_BUFFER)
    tx["maxFeePerGas"] = int((base_fee + max_priority_fee) * GAS_BUFFER)
    tx["type"] = 2  # EIP-1559 transaction type

    try:
        signed = account.sign_transaction(tx)
        txh = web3.eth.send_raw_transaction(signed.rawTransaction)
        receipt = web3.eth.wait_for_transaction_receipt(txh, timeout=120)
        if receipt.status != 1:
            raise ValueError(f"Transação falhou: {txh.hex()}")
        return receipt
    except Exception as e:
        logger.error(f"Erro ao enviar transação para {account.address[:8]}…: {e}")
        raise

###############################################
# TRADES
###############################################
def approve_if_needed(account: LocalAccount, spender: str, amount: int) -> Optional[str]:
    try:
        allowance = token_contract.functions.allowance(account.address, spender).call()
        if allowance >= amount:
            return None
        logger.info(f"Aprovando {amount/(10**TOKEN_DECIMALS):.4f} tokens para {spender}...")
        tx = token_contract.functions.approve(spender, 2**256 - 1).build_transaction({
            "from": account.address,
        })
        receipt = send_tx(tx, account)
        return receipt.transactionHash.hex()
    except Exception as e:
        logger.error(f"Erro ao aprovar tokens para {account.address[:8]}…: {e}")
        return None

def buy_tokens(account: LocalAccount, bnb_wei: int) -> Optional[str]:
    if bnb_wei <= 0:
        return None
    path = [WBNB, Web3.to_checksum_address(TOKEN_ADDRESS)]
    amount_out_min = 0
    deadline = int(time.time()) + 60 * 3
    fn = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
        amount_out_min,
        path,
        account.address,
        deadline,
    )
    tx = fn.build_transaction({
        "from": account.address,
        "value": bnb_wei,
    })
    receipt = send_tx(tx, account)
    return receipt.transactionHash.hex()

def sell_tokens(account: LocalAccount, amount_in: int) -> Optional[str]:
    global last_received_bnb
    if amount_in <= 0:
        return None
    approve_txh = approve_if_needed(account, ROUTER, amount_in)
    if approve_txh:
        logger.info(f"Approve tx={approve_txh}")
    old_bnb = web3.eth.get_balance(account.address)
    path = [Web3.to_checksum_address(TOKEN_ADDRESS), WBNB]
    amount_out_min = 0
    deadline = int(time.time()) + 60 * 3
    fn = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
        amount_in,
        amount_out_min,
        path,
        account.address,
        deadline,
    )
    tx = fn.build_transaction({
        "from": account.address,
    })
    receipt = send_tx(tx, account)
    new_bnb = web3.eth.get_balance(account.address)
    gas_cost = receipt.gasUsed * receipt.effectiveGasPrice
    received = new_bnb - old_bnb + gas_cost
    last_received_bnb = received
    vinfo(f"Received {received / 1e18:.5f} BNB from sell")
    return receipt.transactionHash.hex()

def token_balance_of(addr: str) -> int:
    try:
        return token_contract.functions.balanceOf(addr).call()
    except Exception as e:
        logger.error(f"Erro ao obter saldo de tokens para {addr[:8]}…: {e}")
        return 0

###############################################
# FIBONACCI
###############################################
def fib_gen():
    a, b = 0, 1
    while True:
        yield b
        a, b = b, a + b

###############################################
# MAIN LOOP
###############################################
def _pick_position_fraction() -> float:
    lo, hi = sorted((POSITION_PCT_MIN, POSITION_PCT_MAX))
    return random.uniform(lo, hi)

def run():
    global last_received_bnb
    accounts = load_wallets_from_env()
    logger.info(f"Rodando com {len(accounts)} carteira(s)")

    # Initialize wallet state with sell count
    state: Dict[str, Dict] = {
        a.address: {
            "entry_time": None,
            "entry_price": None,
            "sell_count": 0,  # Track number of sells for no-BNB case
        } for a in accounts
    }

    # Set initial state for wallets with tokens
    for acc in accounts:
        tbal = token_balance_of(acc.address)
        if tbal > 0:
            state[acc.address]["entry_time"] = _now()  # Assume tokens were just acquired
            price = get_token_price_usd()
            state[acc.address]["entry_price"] = price if price else 0.0
            vinfo(f"Inicializando {acc.address[:8]}… com {tbal/(10**TOKEN_DECIMALS):.4f} tokens, preço=${price or 'n/a'}")

    # Action queue
    action_queue = deque()
    buy_fib = fib_gen()
    for _ in range(2):
        next(buy_fib)  # Start at 2 buys
    sell_fib = fib_gen()
    next(sell_fib)  # Start at 1 sell

    def refill_queue():
        buy_count = next(buy_fib)
        sell_count = next(sell_fib)
        # Create a mixed action list to avoid strict buy/sell sequences
        actions = ['buy'] * buy_count + ['sell'] * sell_count
        random.shuffle(actions)  # Shuffle to ensure non-sequential pattern
        action_queue.extend(actions)

    refill_queue()

    while True:
        if not action_queue:
            refill_queue()

        action = action_queue.popleft()

        # Find eligible wallets
        candidates = []
        for acc in accounts:
            addr = acc.address
            tbal = token_balance_of(addr)
            bnb_bal = web3.eth.get_balance(addr)
            spendable = max(0, bnb_bal - int(GAS_RESERVE_BNB * 1e18))
            entry_time = state[addr].get("entry_time")
            age = _now() - entry_time if entry_time else 0
            if action == 'buy' and spendable > 0:
                candidates.append((acc, spendable))
            elif action == 'sell' and tbal > 0:
                candidates.append((acc, tbal))

        if not candidates and action == 'buy':
            # No BNB available, try to sell tokens
            sell_candidates = [(acc, token_balance_of(acc.address)) for acc in accounts if token_balance_of(acc.address) > 0]
            if sell_candidates:
                acc, tbal = random.choice(sell_candidates)
                addr = acc.address
                sell_percentage = 0.4 if state[addr]["sell_count"] == 0 else 0.3  # 40% first, then 30%
                amount_in = int(tbal * sell_percentage)
                if amount_in > 0:
                    vinfo(f"{addr[:8]}… vendendo {sell_percentage*100:.0f}% ({amount_in/(10**TOKEN_DECIMALS):.4f}) tokens por falta de BNB")
                    try:
                        txh = sell_tokens(acc, amount_in)
                        if txh:
                            logger.info(f"SELL {addr[:8]}… tx={txh}")
                            state[addr]["entry_time"] = None
                            state[addr]["entry_price"] = None
                            state[addr]["sell_count"] += 1
                            if state[addr]["sell_count"] < 2:
                                action_queue.appendleft('sell')  # Queue another sell if not done
                            performed = True
                    except Exception as e:
                        logger.error(f"Erro ao vender para {addr[:8]}…: {e}")
                    time.sleep(random.uniform(STAGGER_MIN, STAGGER_MAX))
                    continue
            logger.warning(f"Sem carteira disponível para {action} e sem tokens para vender:")
            for acc in accounts:
                addr = acc.address
                tbal = token_balance_of(addr)
                bnb_bal = web3.eth.get_balance(addr)
                spendable = max(0, bnb_bal - int(GAS_RESERVE_BNB * 1e18))
                age = (_now() - state[addr].get("entry_time", _now())) if state[addr].get("entry_time") else 0
                logger.warning(f"  {addr[:8]}… BNB={bnb_bal/1e18:.5f}, Tokens={tbal/(10**TOKEN_DECIMALS):.4f}, Age={int(age)}s")
            time.sleep(5)
            continue

        if not candidates:
            logger.warning(f"Sem carteira disponível para {action}:")
            for acc in accounts:
                addr = acc.address
                tbal = token_balance_of(addr)
                bnb_bal = web3.eth.get_balance(addr)
                spendable = max(0, bnb_bal - int(GAS_RESERVE_BNB * 1e18))
                age = (_now() - state[addr].get("entry_time", _now())) if state[addr].get("entry_time") else 0
                logger.warning(f"  {addr[:8]}… BNB={bnb_bal/1e18:.5f}, Tokens={tbal/(10**TOKEN_DECIMALS):.4f}, Age={int(age)}s")
            time.sleep(5)
            continue

        # Pick random wallet
        acc, amount = random.choice(candidates)
        addr = acc.address
        performed = False

        try:
            price = get_token_price_usd() if not VOLUME_MODE else None

            if action == 'buy':
                to_spend = int(last_received_bnb * 1.05) if last_received_bnb > 0 else int(amount * _pick_position_fraction())
                to_spend = min(to_spend, int(amount * 0.3))  # Max 30% of BNB
                if to_spend <= 0:
                    logger.warning(f"{addr[:8]}… sem BNB suficiente para comprar")
                    continue
                vinfo(f"{addr[:8]}… comprando com {to_spend/1e18:.5f} BNB")
                txh = buy_tokens(acc, to_spend)
                if txh:
                    logger.info(f"BUY {addr[:8]}… tx={txh}")
                    state[addr]["entry_time"] = _now()
                    state[addr]["entry_price"] = price if price else 0.0
                    state[addr]["sell_count"] = 0  # Reset sell count after buy
                    performed = True

            elif action == 'sell':
                amount_in = int(amount * random.uniform(0.3, 0.4))  # Random 30-40%
                if amount_in <= 0:
                    logger.warning(f"{addr[:8]}… sem tokens suficientes para vender")
                    continue
                age = _now() - state[addr]["entry_time"] if state[addr]["entry_time"] else 0
                sell_now = age >= HOLD_MIN if VOLUME_MODE else age >= HOLD_MAX
                if not VOLUME_MODE and price and state[addr]["entry_price"] and state[addr]["entry_price"] > 0:
                    if price >= state[addr]["entry_price"] * PROFIT_TARGET:
                        sell_now = True
                if not sell_now:
                    vinfo(f"{addr[:8]}… segurando; preço_atual={'{:.6f}'.format(price) if price else 'n/a'}, entry={'{:.6f}'.format(state[addr]['entry_price']) if state[addr]['entry_price'] else 'n/a'}, age={int(age)}s")
                    continue
                vinfo(f"{addr[:8]}… vendendo {amount_in/(10**TOKEN_DECIMALS):.4f} tokens")
                txh = sell_tokens(acc, amount_in)
                if txh:
                    logger.info(f"SELL {addr[:8]}… tx={txh}")
                    state[addr]["entry_time"] = None
                    state[addr]["entry_price"] = None
                    state[addr]["sell_count"] += 1
                    performed = True

        except Exception as e:
            logger.error(f"Erro em {action} para {addr[:8]}…: {e}")

        if performed:
            sleep_time = random.uniform(STAGGER_MIN, STAGGER_MAX)
            vinfo(f"Dormindo {int(sleep_time)}s após transação")
            time.sleep(sleep_time)
        else:
            time.sleep(5)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Encerrado pelo usuário")