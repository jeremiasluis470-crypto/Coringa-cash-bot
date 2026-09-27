"""
streamlit_app.py
=================

Bot "Coringa Cash" para a Deriv — Streamlit.

*** ATUALIZAÇÃO IMPORTANTE (jul/2026) ***
A Deriv trocou a forma de autenticar na API de Opções. O fluxo antigo
(conectar direto no WebSocket e mandar {"authorize": token}) não
funciona mais para essa API e devolve HTTP 401. O fluxo novo é:

  1) REST: lista as contas do usuário
        GET https://api.derivws.com/trading/v1/options/accounts
  2) REST: pede um OTP (senha de uso único) para a conta escolhida
        POST https://api.derivws.com/trading/v1/options/accounts/{account_id}/otp
     A resposta já vem com a URL do WebSocket pronta, ex:
        wss://api.derivws.com/trading/v1/options/ws/demo?otp=xxxxx   (conta demo)
        wss://api.derivws.com/trading/v1/options/ws/real?otp=xxxxx   (conta real)
  3) WebSocket: conecta direto nessa URL. NÃO precisa mais mandar
     "authorize" — o otp na própria URL já autentica a sessão.

Todas as chamadas REST precisam do header "Deriv-App-ID" e de um
Bearer token — que pode ser:
  - um Personal Access Token (PAT), colado manualmente; ou
  - um access_token obtido via OAuth2 + PKCE ("Entrar com a Deriv").

*** CORREÇÃO (jul/2026) — chamada "proposal" ***
A Deriv também mudou o formato da chamada "proposal" dentro do
WebSocket: o campo "symbol" foi renomeado para "underlying_symbol".
Mandar "symbol" agora causa o erro:
    Input validation failed: Properties not allowed: symbol
Além disso, a resposta do "proposal" passou a garantir só o campo
"id" — os demais (como "ask_price") podem não vir, então o código
trata isso com .get() em vez de acesso direto, e cai de volta pro
próprio "id" como preço-limite se "ask_price" não estiver presente.

*** CORREÇÃO (jul/2026) — contract_type inválido ***
O valor "DIGITUND" usado antes NÃO existe na API da Deriv — o valor
correto para contratos de dígito "Under" é "DIGITUNDER". Esse erro de
digitação era a causa do "Input validation failed: Properties not
allowed: contract_type".

*** CORREÇÃO (jul/2026) — campos numéricos vindo como string ***
Na mesma atualização, vários campos numéricos da resposta (ask_price,
payout, buy_price, etc.) passaram a poder vir como STRING em vez de
número ("type: number | string" no schema oficial da Deriv). Isso
quebrava o cálculo do resultado do trade com:

    TypeError: unsupported operand type(s) for -: 'str' and 'str'

porque o código fazia `payout - buy_price` assumindo que os dois já
eram float. A correção é converter TODO valor numérico vindo da API
com a função utilitária `to_float()` antes de qualquer conta.

*** NOVO — seleção de ativo e valor a investir ***
O ativo (symbol) agora é escolhido na sidebar em vez de fixo em
"R_100", e o valor a investir (stake) já existia mas foi renomeado na
tela para deixar mais claro. Só é oferecida a lista de índices
sintéticos, porque esse tipo de contrato de dígitos não existe para
ações individuais na Deriv.

*** NOVO (set/2026) — saldo real da conta ***
O app agora subscreve o saldo da conta ("balance", subscribe=1) assim
que conecta ao WebSocket, e mostra o saldo real (não apenas o P&L
acumulado da sessão) no painel de status. Isso resolve o problema de
a interface não mostrar quanto realmente existe na conta.

*** NOVO (set/2026) — reconciliação com o extrato real da Deriv ***
Ao terminar a sessão (parar o bot ou atingir o máximo de trades), o
app busca o extrato real de contratos da conta via REST
(/trading/v1/options/accounts/{account_id}/statement) e compara,
contrato a contrato, com o que o bot registou localmente. Qualquer
contrato que o extrato real mostra mas o bot não registou (ou com
resultado/pnl diferente) aparece destacado na secção "Reconciliação"
— isso existe porque já houve casos em que a interface só mostrava
vitórias enquanto a conta real na Deriv also tinha derrotas.

As mensagens de trading dentro do WebSocket (ticks, proposal, buy,
proposal_open_contract, forget) continuam no mesmo formato de sempre
(exceto pela troca symbol -> underlying_symbol acima). Se a Deriv
mudar esse formato de novo, o app vai mostrar o erro cru no
"Log de eventos" para facilitar o ajuste.

USO:
  pip install -r requirements.txt
  streamlit run streamlit_app.py

IMPORTANTE — DINHEIRO REAL:
  - O app agora suporta conta REAL além da DEMO. Ao escolher REAL,
    é preciso marcar uma confirmação explícita antes de iniciar.
  - Nunca faça commit de tokens no GitHub. No Streamlit Community
    Cloud, use "Secrets" (Settings > Secrets) para guardar
    DERIV_TOKEN, DERIV_APP_ID e DERIV_CLIENT_ID/SECRET se usar OAuth.
"""

import asyncio
import base64
import hashlib
import json
import secrets
import threading
import time
from collections import deque
from datetime import datetime

import pandas as pd
import requests
import streamlit as st
import websockets

# ----------------------------------------------------------------------
# CONFIGURAÇÃO PADRÃO (ajustável na sidebar)
# ----------------------------------------------------------------------

DEFAULT_APP_ID = "1089"
CONTRACT_TYPE = "DIGITUNDER"  # CORRIGIDO: era "DIGITUND" (inválido) -> "DIGITUNDER"
BARRIER = "9"
DURATION = 1

# Ativos que suportam contratos de dígitos (Over/Under). A Deriv não
# oferece ações individuais para esse tipo de contrato — apenas índices
# sintéticos. Formato: (rótulo exibido, símbolo da API)
AVAILABLE_SYMBOLS = [
    ("Volatility 100 Index", "R_100"),
    ("Volatility 75 Index", "R_75"),
    ("Volatility 50 Index", "R_50"),
    ("Volatility 25 Index", "R_25"),
    ("Volatility 10 Index", "R_10"),
    ("Volatility 100 (1s) Index", "1HZ100V"),
    ("Volatility 75 (1s) Index", "1HZ75V"),
    ("Volatility 50 (1s) Index", "1HZ50V"),
]
DEFAULT_SYMBOL = "R_100"

DEFAULT_STAKE = 0.35
DEFAULT_WINDOW_SIZE = 25
DEFAULT_HIGH_PCT_THRESHOLD = 0.75
NINE_LOW_PCTS = {0.00, 0.01, 0.04, 0.08}
NINE_LOW_TOLERANCE = 0.015
DEFAULT_MAX_TRADES = 200
DEFAULT_COOLDOWN_AFTER_WINS = 3
DEFAULT_COOLDOWN_TICKS = 10

REST_BASE_URL = "https://api.derivws.com"
OAUTH_AUTHORIZE_URL = "https://auth.deriv.com/oauth2/auth"
OAUTH_TOKEN_URL = "https://auth.deriv.com/oauth2/token"

# ----------------------------------------------------------------------
# UTILITÁRIO — CORREÇÃO DO BUG "str - str"
# ----------------------------------------------------------------------


def to_float(value, default=0.0):
    """Converte com segurança um valor vindo da API (pode ser str, número
    ou None) para float.

    A Deriv passou a permitir 'number | string' em vários campos
    numéricos da API de Opções (ex: ask_price, payout, buy_price) na
    atualização de jul/2026. Sem essa conversão explícita, contas como
    `payout - buy_price` quebram com:
        TypeError: unsupported operand type(s) for -: 'str' and 'str'
    sempre que a API decidir devolver esses campos como texto.
    """
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ----------------------------------------------------------------------
# ESTADO GLOBAL COMPARTILHADO ENTRE A THREAD DO BOT E O STREAMLIT
# ----------------------------------------------------------------------

if "bot_state" not in st.session_state:
    st.session_state.bot_state = {
        "running": False,
        "stop_requested": False,
        "trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "total_pnl": 0.0,
        "trades": [],
        "log": [],
        "account": None,
        "account_id": None,
        "symbol": None,
        "error": None,
        "balance": None,
        "currency": None,
        "reconciliation": None,
    }

state = st.session_state.bot_state
state_lock = threading.Lock()


def push_log(msg):
    with state_lock:
        state["log"].append(f"{datetime.now().strftime('%H:%M:%S')} - {msg}")
        state["log"] = state["log"][-200:]


def digit_distribution(window):
    if not window:
        return {}
    counts = {d: 0 for d in range(10)}
    for d in window:
        counts[d] += 1
    n = len(window)
    return {d: counts[d] / n for d in range(10)}


def should_enter(window, window_size, high_pct_threshold):
    if len(window) < window_size:
        return False
    dist = digit_distribution(window)
    pct_1_to_8 = sum(dist[d] for d in range(1, 9))
    pct_9 = dist[9]
    cond_1_to_8_high = pct_1_to_8 >= high_pct_threshold
    cond_9_low = any(abs(pct_9 - target) <= NINE_LOW_TOLERANCE for target in NINE_LOW_PCTS)
    return cond_1_to_8_high and cond_9_low


# ----------------------------------------------------------------------
# FLUXO REST (LISTA DE CONTAS + OTP) — substitui o "authorize" antigo
# ----------------------------------------------------------------------

class DerivRestError(Exception):
    pass


def rest_headers(app_id, bearer_token):
    return {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {bearer_token}",
    }


def rest_list_accounts(app_id, bearer_token):
    """GET /trading/v1/options/accounts -> lista de contas do usuário.

    Cada conta tem (entre outros) account_id, account_type ("demo"/"real"),
    currency, balance. Se o formato vier diferente do esperado, mostramos
    o JSON cru no log para facilitar o ajuste.
    """
    url = f"{REST_BASE_URL}/trading/v1/options/accounts"
    resp = requests.get(url, headers=rest_headers(app_id, bearer_token), timeout=15)
    try:
        payload = resp.json()
    except ValueError:
        raise DerivRestError(f"Resposta não-JSON ao listar contas (HTTP {resp.status_code}): {resp.text[:300]}")

    if resp.status_code >= 400:
        err = payload.get("errors", [{}])[0] if isinstance(payload, dict) else {}
        raise DerivRestError(
            f"HTTP {resp.status_code} ao listar contas: {err.get('code', '?')} - "
            f"{err.get('message', payload)}"
        )

    data = payload.get("data", payload)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise DerivRestError(f"Formato inesperado na lista de contas: {payload}")
    return data


def rest_get_otp_ws_url(app_id, bearer_token, account_id):
    """POST /trading/v1/options/accounts/{account_id}/otp -> URL de WS pronta."""
    url = f"{REST_BASE_URL}/trading/v1/options/accounts/{account_id}/otp"
    resp = requests.post(url, headers=rest_headers(app_id, bearer_token), timeout=15)
    try:
        payload = resp.json()
    except ValueError:
        raise DerivRestError(f"Resposta não-JSON ao pedir OTP (HTTP {resp.status_code}): {resp.text[:300]}")

    if resp.status_code >= 400:
        err = payload.get("errors", [{}])[0] if isinstance(payload, dict) else {}
        raise DerivRestError(
            f"HTTP {resp.status_code} ao pedir OTP: {err.get('code', '?')} - "
            f"{err.get('message', payload)}"
        )

    data = payload.get("data", payload)
    ws_url = data.get("url") if isinstance(data, dict) else None
    if not ws_url:
        raise DerivRestError(f"Resposta do OTP não trouxe 'url': {payload}")
    return ws_url


def rest_get_statement(app_id, bearer_token, account_id, limit=200):
    """GET /trading/v1/options/accounts/{account_id}/statement -> extrato real.

    Usado só no FIM da sessão para reconciliação: busca os últimos
    contratos realmente registados na conta (segundo a própria Deriv,
    não segundo o que o bot viu pelo WebSocket) e devolve uma lista de
    dicts com pelo menos contract_id, profit/pnl e resultado. Se o
    endpoint devolver um formato diferente do esperado, isso é
    reportado no log em vez de travar a app.
    """
    url = f"{REST_BASE_URL}/trading/v1/options/accounts/{account_id}/statement"
    resp = requests.get(
        url, headers=rest_headers(app_id, bearer_token),
        params={"limit": limit}, timeout=15,
    )
    try:
        payload = resp.json()
    except ValueError:
        raise DerivRestError(f"Resposta não-JSON ao buscar extrato (HTTP {resp.status_code}): {resp.text[:300]}")

    if resp.status_code >= 400:
        err = payload.get("errors", [{}])[0] if isinstance(payload, dict) else {}
        raise DerivRestError(
            f"HTTP {resp.status_code} ao buscar extrato: {err.get('code', '?')} - "
            f"{err.get('message', payload)}"
        )

    data = payload.get("data", payload)
    if isinstance(data, dict):
        data = data.get("transactions") or data.get("items") or [data]
    if not isinstance(data, list):
        raise DerivRestError(f"Formato inesperado no extrato: {payload}")
    return data


def reconcile_trades(local_trades, real_statement):
    """Compara os trades registados localmente pelo bot com o extrato
    real da Deriv (rest_get_statement) e devolve um relatório de
    divergências.

    Isto existe especificamente porque já houve sessões em que a
    interface só mostrava vitórias enquanto a conta real tinha
    derrotas — ou seja, o registo local pode estar incompleto/errado
    mesmo depois da correção do bug de contract_id.
    """
    local_by_id = {str(t["contract_id"]): t for t in local_trades}

    real_by_id = {}
    for tx in real_statement:
        cid = tx.get("contract_id") or tx.get("id")
        if cid is None:
            continue
        real_by_id[str(cid)] = tx

    missing_locally = []   # a Deriv tem, o bot não registou
    mismatched = []        # os dois têm, mas pnl/resultado não bate
    real_total_pnl = 0.0

    for cid, tx in real_by_id.items():
        real_pnl = to_float(tx.get("profit", tx.get("pnl")), default=0.0)
        real_total_pnl += real_pnl
        real_result = "GANHO" if real_pnl > 0 else ("PERDA" if real_pnl < 0 else "NEUTRO")

        local = local_by_id.get(cid)
        if local is None:
            missing_locally.append({
                "contract_id": cid,
                "resultado_real": real_result,
                "pnl_real": round(real_pnl, 2),
            })
        else:
            local_pnl = to_float(local.get("pnl"), default=0.0)
            if abs(local_pnl - real_pnl) > 0.01 or local.get("resultado") != real_result:
                mismatched.append({
                    "contract_id": cid,
                    "resultado_local": local.get("resultado"),
                    "pnl_local": local_pnl,
                    "resultado_real": real_result,
                    "pnl_real": round(real_pnl, 2),
                })

    local_total_pnl = sum(to_float(t.get("pnl"), default=0.0) for t in local_trades)

    return {
        "n_local": len(local_trades),
        "n_real": len(real_by_id),
        "local_total_pnl": round(local_total_pnl, 2),
        "real_total_pnl": round(real_total_pnl, 2),
        "missing_locally": missing_locally,
        "mismatched": mismatched,
        "ok": not missing_locally and not mismatched,
    }


# ----------------------------------------------------------------------
# LOGIN OAUTH2 + PKCE ("Entrar com a Deriv")
# ----------------------------------------------------------------------

def pkce_pair():
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode("ascii")
    return verifier, challenge


def build_oauth_authorize_url(app_id, redirect_uri, state_token, code_challenge):
    return (
        f"{OAUTH_AUTHORIZE_URL}?response_type=code&client_id={app_id}"
        f"&redirect_uri={redirect_uri}&scope=trade+account_manage"
        f"&state={state_token}&code_challenge={code_challenge}&code_challenge_method=S256"
    )


def exchange_oauth_code(app_id, redirect_uri, code, code_verifier):
    resp = requests.post(
        OAUTH_TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "client_id": app_id,
            "code": code,
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
        },
        timeout=15,
    )
    try:
        payload = resp.json()
    except ValueError:
        raise DerivRestError(f"Resposta não-JSON ao trocar code por token: {resp.text[:300]}")
    if resp.status_code >= 400 or "access_token" not in payload:
        raise DerivRestError(f"Falha ao trocar code por token: {payload}")
    return payload["access_token"]


# ----------------------------------------------------------------------
# WEBSOCKET DE TRADING
# ----------------------------------------------------------------------

async def buy_contract(ws, last_digit_dist, params, digit_window=None):
    # CORRIGIDO: "symbol" -> "underlying_symbol" (a Deriv renomeou esse
    # campo na chamada "proposal"; usar "symbol" agora causa o erro
    # "Input validation failed: Properties not allowed: symbol").
    proposal_req = {
        "proposal": 1,
        "amount": params["stake"],
        "basis": "stake",
        "contract_type": CONTRACT_TYPE,
        "currency": "USD",
        "duration": DURATION,
        "duration_unit": "t",
        "underlying_symbol": params["symbol"],
        "barrier": BARRIER,
    }
    await ws.send(json.dumps(proposal_req))
    proposal_resp = json.loads(await ws.recv())
    if "error" in proposal_resp:
        push_log(f"Erro na proposta: {proposal_resp['error']['message']}")
        return None

    proposal = proposal_resp.get("proposal", {})
    proposal_id = proposal.get("id")
    if not proposal_id:
        push_log(f"Resposta da proposta sem 'id': {proposal_resp}")
        return None

    # CORRIGIDO: só o campo "id" é garantido na resposta agora.
    # "ask_price" pode não vir, e quando vem pode ser string ou número
    # ("type: number | string" no schema novo) -> usamos to_float().
    ask_price_raw = proposal.get("ask_price")
    if ask_price_raw is None:
        push_log(f"Aviso: 'ask_price' não veio na proposta, resposta crua: {proposal_resp}")
        return None
    ask_price = to_float(ask_price_raw, default=None)
    if ask_price is None:
        push_log(f"Aviso: 'ask_price' veio num formato inválido: {ask_price_raw!r}")
        return None

    await ws.send(json.dumps({"buy": proposal_id, "price": ask_price}))
    buy_resp = json.loads(await ws.recv())
    if "error" in buy_resp:
        push_log(f"Erro na compra: {buy_resp['error']['message']}")
        return None

    contract_id = buy_resp["buy"]["contract_id"]

    await ws.send(json.dumps({
        "proposal_open_contract": 1,
        "contract_id": contract_id,
        "subscribe": 1,
    }))

    payout = 0.0
    pnl = 0.0
    result_label = "PENDENTE"
    subscription_id = None

    # CORRIGIDO (bug de vitórias "fantasma"): o loop antigo aceitava a
    # PRIMEIRA mensagem proposal_open_contract com is_sold=true que chegasse,
    # sem checar se ela pertencia a ESTE contract_id. Quando os trades
    # disparam rápido (tick a tick), o "forget" do trade anterior às vezes
    # ainda não tinha sido processado pela Deriv, e uma mensagem presa na
    # fila de um contrato ANTERIOR (já vencedor) era capturada pelo trade
    # novo — por isso o log só mostrava vitórias, mesmo quando o extrato
    # real da Deriv tinha derrotas. Agora filtramos explicitamente por
    # contract_id antes de aceitar o resultado como sendo deste trade.
    #
    # NOTA (set/2026): esse filtro reduz muito o problema mas não prova
    # sozinho que o registo local bate 100% com a Deriv — por isso a
    # reconciliação no fim da sessão (reconcile_trades) existe como
    # segunda camada de verificação, contra o extrato real da conta.
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("msg_type") == "balance":
            with state_lock:
                state["balance"] = to_float(msg["balance"].get("balance"), default=state.get("balance"))
                state["currency"] = msg["balance"].get("currency", state.get("currency"))
            continue
        if msg.get("msg_type") != "proposal_open_contract":
            continue
        contract = msg["proposal_open_contract"]
        if contract.get("contract_id") != contract_id:
            continue  # mensagem de um contrato diferente (residual/atrasada) - ignora
        if contract.get("is_sold"):
            # payout e buy_price podem vir como string ("10.50") em vez de
            # número -> to_float() evita o TypeError da subtração.
            payout = to_float(contract.get("payout"), default=0.0)
            buy_price = to_float(contract.get("buy_price"), default=params["stake"])
            pnl = payout - buy_price
            result_label = "GANHO" if pnl > 0 else "PERDA"
            subscription_id = msg.get("subscription", {}).get("id")
            break

    # Manda o forget e ESPERA a confirmação antes de liberar o próximo
    # trade, pra garantir que essa subscrição pare de fato de enviar
    # mensagens e não vaze pro loop do próximo contrato.
    if subscription_id:
        await ws.send(json.dumps({"forget": subscription_id}))
        try:
            for _ in range(5):
                ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                if ack.get("msg_type") == "forget":
                    break
                # não descarta ticks de mercado que chegarem nessa espera -
                # continuam alimentando a janela usada pela regra de entrada
                if ack.get("msg_type") == "tick" and digit_window is not None:
                    quote = ack["tick"]["quote"]
                    digit_window.append(int(str(quote).replace(".", "")[-1]))
                if ack.get("msg_type") == "balance":
                    with state_lock:
                        state["balance"] = to_float(ack["balance"].get("balance"), default=state.get("balance"))
                        state["currency"] = ack["balance"].get("currency", state.get("currency"))
        except asyncio.TimeoutError:
            push_log(f"Aviso: não recebi confirmação de 'forget' para contrato {contract_id} a tempo.")

    pct_1_8 = sum(last_digit_dist.get(d, 0) for d in range(1, 9))
    pct_9 = last_digit_dist.get(9, 0)

    trade_row = {
        "timestamp": datetime.utcnow().isoformat(),
        "contract_id": contract_id,
        "resultado": result_label,
        "payout": round(payout, 2),
        "pnl": round(pnl, 2),
        "pct_1_a_8": f"{pct_1_8:.1%}",
        "pct_9": f"{pct_9:.1%}",
    }
    return trade_row


async def bot_loop(ws_url, account_label, account_id, params, app_id, bearer_token):
    digit_window = deque(maxlen=params["window_size"])
    consecutive_wins = 0
    cooldown_remaining = 0
    trade_in_flight = False

    try:
        async with websockets.connect(ws_url, ping_interval=20) as ws:
            with state_lock:
                state["account"] = account_label
                state["account_id"] = account_id
            push_log(f"Conectado ({account_label}). Simulação/operação iniciada. "
                     f"Ativo={params['symbol']} | Stake={params['stake']} | "
                     f"Janela={params['window_size']} | Meta={params['max_trades']} trades")

            # NOVO: subscreve o saldo real da conta assim que conecta, para
            # a interface deixar de mostrar só o P&L da sessão e passar a
            # mostrar quanto existe de facto na conta.
            await ws.send(json.dumps({"balance": 1, "subscribe": 1}))
            await ws.send(json.dumps({"ticks": params["symbol"], "subscribe": 1}))

            while True:
                with state_lock:
                    if state["stop_requested"] or state["trade_count"] >= params["max_trades"]:
                        break

                msg = json.loads(await ws.recv())

                if msg.get("error"):
                    push_log(f"ERRO da API: {msg['error'].get('message')}")
                    continue

                if msg.get("msg_type") == "balance":
                    with state_lock:
                        state["balance"] = to_float(msg["balance"].get("balance"), default=state.get("balance"))
                        state["currency"] = msg["balance"].get("currency", state.get("currency"))
                    continue

                if msg.get("msg_type") != "tick":
                    continue

                quote = msg["tick"]["quote"]
                last_digit = int(str(quote).replace(".", "")[-1])
                digit_window.append(last_digit)

                if cooldown_remaining > 0:
                    cooldown_remaining -= 1
                    continue

                if trade_in_flight:
                    continue

                if should_enter(digit_window, params["window_size"], params["high_pct_threshold"]):
                    trade_in_flight = True
                    dist_snapshot = digit_distribution(digit_window)
                    trade_row = await buy_contract(ws, dist_snapshot, params, digit_window=digit_window)
                    trade_in_flight = False

                    if trade_row is not None:
                        with state_lock:
                            state["trade_count"] += 1
                            state["total_pnl"] += trade_row["pnl"]
                            if trade_row["resultado"] == "GANHO":
                                state["win_count"] += 1
                                consecutive_wins += 1
                            else:
                                state["loss_count"] += 1
                                consecutive_wins = 0
                            state["trades"].append(trade_row)

                        if consecutive_wins >= params["cooldown_after_wins"]:
                            cooldown_remaining = params["cooldown_ticks"]
                            consecutive_wins = 0
                            push_log(f"{params['cooldown_after_wins']} ganhos seguidos: "
                                     f"pausa de {params['cooldown_ticks']} ticks.")

            await ws.send(json.dumps({"forget_all": "ticks"}))
            await ws.send(json.dumps({"forget_all": "balance"}))

    except Exception as e:
        with state_lock:
            state["error"] = str(e)
        push_log(f"ERRO: {e}")
    finally:
        # NOVO: reconciliação contra o extrato real da Deriv ao terminar a
        # sessão (por stop manual, max_trades ou erro). Isto é o que
        # confirma se o que a interface mostrou bate com o que a Deriv
        # realmente registou.
        try:
            with state_lock:
                local_trades = list(state["trades"])
            if account_id and local_trades:
                statement = rest_get_statement(app_id, bearer_token, account_id, limit=max(200, len(local_trades) * 2))
                report = reconcile_trades(local_trades, statement)
                with state_lock:
                    state["reconciliation"] = report
                if report["ok"]:
                    push_log("Reconciliação com a Deriv: OK, tudo bate.")
                else:
                    push_log(
                        f"Reconciliação com a Deriv: DIVERGÊNCIA — "
                        f"{len(report['missing_locally'])} contrato(s) não registados localmente, "
                        f"{len(report['mismatched'])} com resultado diferente. "
                        f"P&L local={report['local_total_pnl']} vs P&L real={report['real_total_pnl']}."
                    )
        except DerivRestError as e:
            push_log(f"Aviso: não foi possível reconciliar com o extrato real: {e}")
        except Exception as e:
            push_log(f"Aviso: erro inesperado na reconciliação: {e}")

        with state_lock:
            state["running"] = False
        push_log("Encerrado.")


def start_bot_thread(ws_url, account_label, account_id, params, app_id, bearer_token):
    def runner():
        asyncio.run(bot_loop(ws_url, account_label, account_id, params, app_id, bearer_token))

    t = threading.Thread(target=runner, daemon=True)
    t.start()


# ----------------------------------------------------------------------
# INTERFACE STREAMLIT
# ----------------------------------------------------------------------

st.set_page_config(page_title="Coringa Cash - Deriv", layout="wide")
st.title("🃏 Coringa Cash — Deriv (Demo/Real)")
st.caption(
    "Ferramenta de teste/estudo: mede o EV real observado da regra de entrada "
    "contra um RNG i.i.d. Suporta conta DEMO e conta REAL."
)

# ---- captura de retorno do OAuth (?code=...&state=...) ----
query_params = st.query_params
if "oauth_flow" not in st.session_state:
    st.session_state.oauth_flow = {"verifier": None, "state": None, "access_token": None}

with st.sidebar:
    st.header("Configuração")

    auth_method = st.radio("Autenticação", ["Token (PAT)", "Entrar com a Deriv (OAuth)"])
    app_id = st.text_input("App ID", value=DEFAULT_APP_ID)
    account_choice = st.radio("Tipo de conta", ["Demo", "Real"], horizontal=True)

    bearer_token = None

    if auth_method == "Token (PAT)":
        bearer_token = st.text_input(
            "Personal Access Token (PAT) da Deriv", type="password",
            value=st.secrets.get("DERIV_TOKEN", "") if hasattr(st, "secrets") else "",
        )
    else:
        redirect_uri = st.text_input(
            "Redirect URI (a URL deste app, cadastrada no painel da Deriv)",
            value=st.secrets.get("DERIV_REDIRECT_URI", "") if hasattr(st, "secrets") else "",
        )

        if st.session_state.oauth_flow["access_token"]:
            st.success("Login com a Deriv concluído.")
            bearer_token = st.session_state.oauth_flow["access_token"]
        elif "code" in query_params and "state" in query_params:
            if query_params["state"] == st.session_state.oauth_flow.get("state"):
                try:
                    token = exchange_oauth_code(
                        app_id, redirect_uri, query_params["code"],
                        st.session_state.oauth_flow["verifier"],
                    )
                    st.session_state.oauth_flow["access_token"] = token
                    bearer_token = token
                    st.query_params.clear()
                    st.success("Login com a Deriv concluído.")
                except DerivRestError as e:
                    st.error(f"Falha no login OAuth: {e}")
            else:
                st.error("State do OAuth não confere. Tente novamente.")
        else:
            if redirect_uri and st.button("🔑 Entrar com a Deriv"):
                verifier, challenge = pkce_pair()
                state_token = secrets.token_urlsafe(16)
                st.session_state.oauth_flow["verifier"] = verifier
                st.session_state.oauth_flow["state"] = state_token
                auth_url = build_oauth_authorize_url(app_id, redirect_uri, state_token, challenge)
                st.markdown(f"[Clique aqui para entrar com sua conta Deriv]({auth_url})")
            elif not redirect_uri:
                st.info("Preencha o Redirect URI (precisa estar cadastrado no painel de apps da Deriv) para habilitar o login.")

    symbol_labels = [label for label, _ in AVAILABLE_SYMBOLS]
    default_index = next(
        (i for i, (_, sym) in enumerate(AVAILABLE_SYMBOLS) if sym == DEFAULT_SYMBOL), 0
    )
    symbol_label = st.selectbox("Ativo", symbol_labels, index=default_index)
    symbol = dict(AVAILABLE_SYMBOLS)[symbol_label]

    stake = st.number_input("Valor a investir (stake)", min_value=0.35, value=DEFAULT_STAKE, step=0.05)
    window_size = st.number_input("Janela (ticks)", min_value=5, value=DEFAULT_WINDOW_SIZE, step=1)
    high_pct_threshold = st.slider("Limiar % dígitos 1-8", 0.5, 1.0, DEFAULT_HIGH_PCT_THRESHOLD, 0.01)
    max_trades = st.number_input("Máximo de trades", min_value=1, value=DEFAULT_MAX_TRADES, step=10)
    cooldown_after_wins = st.number_input("Pausa após N vitórias seguidas",
                                           min_value=1, value=DEFAULT_COOLDOWN_AFTER_WINS, step=1)
    cooldown_ticks = st.number_input("Ticks de pausa", min_value=1, value=DEFAULT_COOLDOWN_TICKS, step=1)

    real_confirm = True
    if account_choice == "Real":
        st.warning("Conta REAL selecionada: as ordens usam dinheiro de verdade.")
        real_confirm = st.checkbox("Confirmo que quero operar com dinheiro real (conta REAL)")

    col_a, col_b = st.columns(2)
    start_clicked = col_a.button("▶ Iniciar", disabled=state["running"])
    stop_clicked = col_b.button("⏹ Parar", disabled=not state["running"])

if start_clicked:
    if not bearer_token:
        st.sidebar.error("Autentique-se (token ou login com a Deriv) antes de iniciar.")
    elif account_choice == "Real" and not real_confirm:
        st.sidebar.error("Marque a confirmação de conta REAL antes de iniciar.")
    else:
        with state_lock:
            state.update({
                "running": True,
                "stop_requested": False,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "total_pnl": 0.0,
                "trades": [],
                "log": [],
                "error": None,
                "balance": None,
                "currency": None,
                "reconciliation": None,
            })
        try:
            accounts = rest_list_accounts(app_id, bearer_token)
            wanted_type = "demo" if account_choice == "Demo" else "real"
            match = next(
                (a for a in accounts if str(a.get("account_type", "")).lower() == wanted_type),
                None,
            )
            if match is None and len(accounts) == 1:
                match = accounts[0]  # só uma conta disponível: usa ela mesmo
            if match is None:
                raise DerivRestError(
                    f"Nenhuma conta do tipo '{wanted_type}' encontrada. Contas disponíveis: {accounts}"
                )

            account_id = match.get("account_id") or match.get("loginid") or match.get("id")
            ws_url = rest_get_otp_ws_url(app_id, bearer_token, account_id)
            with state_lock:
                state["symbol"] = symbol
            params = dict(
                symbol=symbol, stake=stake, window_size=int(window_size),
                high_pct_threshold=high_pct_threshold, max_trades=int(max_trades),
                cooldown_after_wins=int(cooldown_after_wins), cooldown_ticks=int(cooldown_ticks),
            )
            start_bot_thread(ws_url, f"{account_id} ({wanted_type})", account_id, params, app_id, bearer_token)
        except DerivRestError as e:
            with state_lock:
                state["running"] = False
                state["error"] = str(e)
            push_log(f"ERRO ao preparar conexão: {e}")
            st.sidebar.error(str(e))

if stop_clicked:
    with state_lock:
        state["stop_requested"] = True

# ---- painel de status ----
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Trades", state["trade_count"])
col2.metric("Vitórias", state["win_count"])
col3.metric("Derrotas", state["loss_count"])
win_rate = (state["win_count"] / state["trade_count"] * 100) if state["trade_count"] else 0
col4.metric("Win rate", f"{win_rate:.1f}%")
saldo_txt = f"{state['balance']:.2f} {state['currency']}" if state.get("balance") is not None else "—"
col5.metric("Saldo da conta", saldo_txt)

st.metric("P&L acumulado (sessão)", f"{state['total_pnl']:+.2f}")

if state["account"]:
    ativo_txt = f" | Ativo: {state['symbol']}" if state.get("symbol") else ""
    st.success(f"Conta: {state['account']}{ativo_txt}")
if state["error"]:
    st.error(state["error"])

# ---- reconciliação com o extrato real da Deriv ----
recon = state.get("reconciliation")
if recon is not None:
    st.subheader("Reconciliação com a Deriv")
    if recon["ok"]:
        st.success(
            f"Bate certo: {recon['n_local']} trade(s) locais = {recon['n_real']} no extrato real. "
            f"P&L local {recon['local_total_pnl']:+.2f} = P&L real {recon['real_total_pnl']:+.2f}."
        )
    else:
        st.error(
            f"Divergência entre o app e o extrato real da Deriv! "
            f"P&L local {recon['local_total_pnl']:+.2f} vs P&L real {recon['real_total_pnl']:+.2f}."
        )
        if recon["missing_locally"]:
            st.write("Contratos que a Deriv registou e o app NÃO mostrou:")
            st.dataframe(pd.DataFrame(recon["missing_locally"]), use_container_width=True)
        if recon["mismatched"]:
            st.write("Contratos com resultado diferente entre o app e a Deriv:")
            st.dataframe(pd.DataFrame(recon["mismatched"]), use_container_width=True)

st.subheader("Trades")
if state["trades"]:
    df_trades = pd.DataFrame(list(reversed(state["trades"])))
    st.dataframe(df_trades, use_container_width=True, height=300)
    st.download_button(
        "⬇ Baixar CSV desta sessão",
        data=df_trades.to_csv(index=False).encode("utf-8"),
        file_name=f"coringa_cash_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )
else:
    st.info("Nenhum trade registrado ainda.")

with st.expander("Log de eventos"):
    st.code("\n".join(reversed(state["log"])) or "—")

if state["running"]:
    time.sleep(2)
    st.rerun()
