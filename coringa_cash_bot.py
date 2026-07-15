"""
streamlit_app.py
=================

Versão Streamlit do bot de SIMULAÇÃO "Coringa Cash" (conta DEMO da Deriv).

Como o Streamlit re-executa o script a cada interação e não lida bem com
loops assíncronos infinitos bloqueando a thread principal, a estratégia
roda numa THREAD separada em background. A interface só lê o estado
(trades, saldo, win rate) desse background e se atualiza sozinha.

USO:
  pip install -r requirements.txt
  streamlit run streamlit_app.py

Depois, na sidebar, cole o token DEMO da Deriv e clique em "Iniciar".

IMPORTANTE:
  - Continua sendo só para conta DEMO. O script trava sozinho se detectar
    um login que não pareça demo (VRTC/VRT).
  - Nunca commit o token no GitHub. No Streamlit Community Cloud, use
    "Secrets" (Settings > Secrets) para guardar o DERIV_TOKEN.
"""

import asyncio
import json
import threading
import time
from collections import deque
from datetime import datetime

import pandas as pd
import streamlit as st
import websockets

# ----------------------------------------------------------------------
# CONFIGURAÇÃO PADRÃO (ajustável na sidebar)
# ----------------------------------------------------------------------

DEFAULT_APP_ID = "1089"
SYMBOL = "R_100"
CONTRACT_TYPE = "DIGITUND"
BARRIER = "9"
DURATION = 1

DEFAULT_STAKE = 0.35
DEFAULT_WINDOW_SIZE = 25
DEFAULT_HIGH_PCT_THRESHOLD = 0.75
NINE_LOW_PCTS = {0.00, 0.01, 0.04, 0.08}
NINE_LOW_TOLERANCE = 0.015
DEFAULT_MAX_TRADES = 200
DEFAULT_COOLDOWN_AFTER_WINS = 3
DEFAULT_COOLDOWN_TICKS = 10

# ----------------------------------------------------------------------
# ESTADO GLOBAL COMPARTILHADO ENTRE A THREAD DO BOT E O STREAMLIT
# (usar um dict simples + lock evita problemas de concorrência)
# ----------------------------------------------------------------------

if "bot_state" not in st.session_state:
    st.session_state.bot_state = {
        "running": False,
        "stop_requested": False,
        "trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "total_pnl": 0.0,
        "trades": [],   # lista de dicts, um por trade
        "log": [],      # linhas de status/erro
        "account": None,
        "error": None,
    }

state = st.session_state.bot_state
state_lock = threading.Lock()


def push_log(msg):
    with state_lock:
        state["log"].append(f"{datetime.now().strftime('%H:%M:%S')} - {msg}")
        state["log"] = state["log"][-200:]  # mantém só as últimas 200 linhas


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


async def authorize(ws, token):
    await ws.send(json.dumps({"authorize": token}))
    resp = json.loads(await ws.recv())
    if "error" in resp:
        raise RuntimeError(f"Falha ao autorizar: {resp['error']['message']}")
    account = resp["authorize"]
    is_demo = account["loginid"].startswith(("VRTC", "VRT"))
    with state_lock:
        state["account"] = account["loginid"]
    if not is_demo:
        raise RuntimeError(
            f"Login '{account['loginid']}' não parece ser conta DEMO (VRTC/VRT). "
            "Abortado por segurança."
        )
    push_log(f"Autenticado na conta demo: {account['loginid']}")
    return account


async def buy_contract(ws, last_digit_dist, params):
    proposal_req = {
        "proposal": 1,
        "amount": params["stake"],
        "basis": "stake",
        "contract_type": CONTRACT_TYPE,
        "currency": "USD",
        "duration": DURATION,
        "duration_unit": "t",
        "symbol": SYMBOL,
        "barrier": BARRIER,
    }
    await ws.send(json.dumps(proposal_req))
    proposal_resp = json.loads(await ws.recv())
    if "error" in proposal_resp:
        push_log(f"Erro na proposta: {proposal_resp['error']['message']}")
        return None

    proposal_id = proposal_resp["proposal"]["id"]
    ask_price = proposal_resp["proposal"]["ask_price"]

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

    while True:
        msg = json.loads(await ws.recv())
        if msg.get("msg_type") != "proposal_open_contract":
            continue
        contract = msg["proposal_open_contract"]
        if contract.get("is_sold"):
            payout = contract.get("payout", 0.0)
            buy_price = contract.get("buy_price", params["stake"])
            pnl = payout - buy_price
            result_label = "GANHO" if pnl > 0 else "PERDA"
            await ws.send(json.dumps({"forget": msg["subscription"]["id"]}))
            break

    pct_1_8 = sum(last_digit_dist.get(d, 0) for d in range(1, 9))
    pct_9 = last_digit_dist.get(9, 0)

    trade_row = {
        "timestamp": datetime.utcnow().isoformat(),
        "resultado": result_label,
        "payout": round(payout, 2),
        "pnl": round(pnl, 2),
        "pct_1_a_8": f"{pct_1_8:.1%}",
        "pct_9": f"{pct_9:.1%}",
    }
    return trade_row


async def bot_loop(token, app_id, params):
    ws_url = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    digit_window = deque(maxlen=params["window_size"])
    consecutive_wins = 0
    cooldown_remaining = 0
    trade_in_flight = False

    try:
        async with websockets.connect(ws_url, ping_interval=20) as ws:
            await authorize(ws, token)
            await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))
            push_log(f"Simulação iniciada. Stake={params['stake']} | "
                     f"Janela={params['window_size']} | Meta={params['max_trades']} trades")

            while True:
                with state_lock:
                    if state["stop_requested"] or state["trade_count"] >= params["max_trades"]:
                        break

                msg = json.loads(await ws.recv())
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
                    trade_row = await buy_contract(ws, dist_snapshot, params)
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

    except Exception as e:
        with state_lock:
            state["error"] = str(e)
        push_log(f"ERRO: {e}")
    finally:
        with state_lock:
            state["running"] = False
        push_log("Simulação encerrada.")


def start_bot_thread(token, app_id, params):
    def runner():
        asyncio.run(bot_loop(token, app_id, params))

    t = threading.Thread(target=runner, daemon=True)
    t.start()


# ----------------------------------------------------------------------
# INTERFACE STREAMLIT
# ----------------------------------------------------------------------

st.set_page_config(page_title="Coringa Cash - Simulação (Demo)", layout="wide")
st.title("🃏 Coringa Cash — Simulação em conta DEMO")
st.caption(
    "Ferramenta de teste/estudo: mede o EV real observado da regra de entrada "
    "contra um RNG i.i.d. Não roda em conta real."
)

with st.sidebar:
    st.header("Configuração")
    token = st.text_input("Token DEMO da Deriv", type="password",
                           value=st.secrets.get("DERIV_TOKEN", "") if hasattr(st, "secrets") else "")
    app_id = st.text_input("App ID", value=DEFAULT_APP_ID)
    stake = st.number_input("Stake", min_value=0.35, value=DEFAULT_STAKE, step=0.05)
    window_size = st.number_input("Janela (ticks)", min_value=5, value=DEFAULT_WINDOW_SIZE, step=1)
    high_pct_threshold = st.slider("Limiar % dígitos 1-8", 0.5, 1.0, DEFAULT_HIGH_PCT_THRESHOLD, 0.01)
    max_trades = st.number_input("Máximo de trades", min_value=1, value=DEFAULT_MAX_TRADES, step=10)
    cooldown_after_wins = st.number_input("Pausa após N vitórias seguidas",
                                           min_value=1, value=DEFAULT_COOLDOWN_AFTER_WINS, step=1)
    cooldown_ticks = st.number_input("Ticks de pausa", min_value=1, value=DEFAULT_COOLDOWN_TICKS, step=1)

    col_a, col_b = st.columns(2)
    start_clicked = col_a.button("▶ Iniciar", disabled=state["running"])
    stop_clicked = col_b.button("⏹ Parar", disabled=not state["running"])

if start_clicked:
    if not token:
        st.sidebar.error("Cola o token DEMO antes de iniciar.")
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
            })
        params = dict(
            stake=stake, window_size=int(window_size),
            high_pct_threshold=high_pct_threshold, max_trades=int(max_trades),
            cooldown_after_wins=int(cooldown_after_wins), cooldown_ticks=int(cooldown_ticks),
        )
        start_bot_thread(token, app_id, params)

if stop_clicked:
    with state_lock:
        state["stop_requested"] = True

# ---- painel de status ----
col1, col2, col3, col4 = st.columns(4)
col1.metric("Trades", state["trade_count"])
col2.metric("Vitórias", state["win_count"])
col3.metric("Derrotas", state["loss_count"])
win_rate = (state["win_count"] / state["trade_count"] * 100) if state["trade_count"] else 0
col4.metric("Win rate", f"{win_rate:.1f}%")

st.metric("P&L acumulado", f"{state['total_pnl']:+.2f}")

if state["account"]:
    st.success(f"Conta: {state['account']}")
if state["error"]:
    st.error(state["error"])

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
