"""
coringa_cash_bot.py
====================

Bot de SIMULAÇÃO (conta DEMO da Deriv) que implementa a estratégia
"Coringa Cash" da Placa Curiosa:

  - Contrato: DIGITUND (Digit Under), barreira = 9
    -> Ganha se o último dígito do tick for 0-8 (qualquer coisa exceto 9)
    -> Perde se o último dígito for exatamente 9

  - Regra de entrada (conforme a placa):
    Entra quando, numa janela dos últimos N ticks:
      * o percentual combinado dos dígitos 1-8 estiver "alto"
      * E o percentual do dígito 9 estiver em {0%, 1%, 4%, 8%}

OBJETIVO: rodar isso numa conta DEMO, registrar CADA trade (ganho/perda,
stake, payout) e no final calcular o EV real observado, comparando com
o EV teórico. Isso serve para provar (ou refutar, com dados reais) se a
regra de entrada tem algum poder preditivo sobre um RNG i.i.d.

REQUISITOS:
  pip install websockets --break-system-packages

USO:
  1. Cria uma conta demo em https://deriv.com (é grátis)
  2. Gera um token de API (Deriv > Configurações > API Token) com
     escopo "Trade" habilitado para a conta DEMO (nunca uses token
     de conta REAL para testes).
  3. Preenche DERIV_TOKEN e (opcional) DERIV_APP_ID abaixo, ou passa
     como variáveis de ambiente.
  4. python3 coringa_cash_bot.py

IMPORTANTE:
  - Este script é só para fins de teste/estudo em conta DEMO.
  - Não há garantia de lucro. A intenção declarada é MEDIR o EV real
    da estratégia, não "vencer" o mercado.
"""

import asyncio
import csv
import json
import os
import sys
import time
from collections import deque
from datetime import datetime

import websockets

# ----------------------------------------------------------------------
# CONFIGURAÇÃO
# ----------------------------------------------------------------------

DERIV_APP_ID = os.environ.get("DERIV_APP_ID", "1089")  # app_id público de teste da Deriv
DERIV_TOKEN = os.environ.get("DERIV_TOKEN", "COLOQUE_AQUI_SEU_TOKEN_DEMO")
DERIV_WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={DERIV_APP_ID}"

SYMBOL = "R_100"          # índice sintético (Volatility 100 Index)
STAKE = 0.35              # valor da entrada, igual ao sugerido na placa
DURATION = 1              # duração do contrato em ticks
CONTRACT_TYPE = "DIGITUND"  # "Digit Under"
BARRIER = "9"              # ganha se o dígito < 9 (ou seja, 0-8)

WINDOW_SIZE = 25           # tamanho da janela de ticks para calcular percentuais
HIGH_PCT_THRESHOLD = 0.75  # "percentual alto" dos dígitos 1-8 combinados (ajustável)
NINE_LOW_PCTS = {0.00, 0.01, 0.04, 0.08}  # percentuais "baixos" aceitáveis p/ o dígito 9
NINE_LOW_TOLERANCE = 0.015  # tolerância em torno dos valores acima (ticks discretos)

MAX_TRADES = 200           # número de trades da simulação antes de parar e resumir
COOLDOWN_AFTER_WINS = 3    # "pausa após ganhos consecutivos" (regra da placa)
COOLDOWN_TICKS = 10        # quantos ticks pular após a pausa

LOG_FILE = "coringa_cash_log.csv"

# ----------------------------------------------------------------------
# ESTADO
# ----------------------------------------------------------------------

digit_window = deque(maxlen=WINDOW_SIZE)
trade_count = 0
win_count = 0
loss_count = 0
total_pnl = 0.0
consecutive_wins = 0
cooldown_remaining = 0
trade_in_flight = False


def digit_distribution(window):
    """Retorna dict {digito: percentual} da janela atual."""
    if not window:
        return {}
    counts = {d: 0 for d in range(10)}
    for d in window:
        counts[d] += 1
    n = len(window)
    return {d: counts[d] / n for d in range(10)}


def should_enter(window):
    """Implementa a regra de entrada do Coringa Cash."""
    if len(window) < WINDOW_SIZE:
        return False
    dist = digit_distribution(window)
    pct_1_to_8 = sum(dist[d] for d in range(1, 9))
    pct_9 = dist[9]

    cond_1_to_8_high = pct_1_to_8 >= HIGH_PCT_THRESHOLD
    cond_9_low = any(abs(pct_9 - target) <= NINE_LOW_TOLERANCE for target in NINE_LOW_PCTS)

    return cond_1_to_8_high and cond_9_low


def log_trade(row):
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "symbol", "stake", "contract_type", "barrier",
                "resultado", "payout", "pnl", "saldo_acumulado",
                "pct_1_a_8", "pct_9",
            ])
        writer.writerow(row)


def print_summary():
    print("\n" + "=" * 60)
    print("RESUMO DA SIMULAÇÃO - CORINGA CASH (conta DEMO)")
    print("=" * 60)
    print(f"Total de trades:      {trade_count}")
    print(f"Vitórias:             {win_count}")
    print(f"Derrotas:             {loss_count}")
    if trade_count > 0:
        win_rate = win_count / trade_count * 100
        print(f"Win rate observado:   {win_rate:.2f}%")
    print(f"P&L total:            {total_pnl:.2f}")
    if trade_count > 0:
        ev_por_trade = total_pnl / trade_count
        print(f"EV médio por trade:   {ev_por_trade:.4f}")
    print(f"Log completo salvo em: {LOG_FILE}")
    print("=" * 60)


async def send_request(ws, request):
    await ws.send(json.dumps(request))
    while True:
        response = json.loads(await ws.recv())
        if response.get("msg_type") == request.get("_expect", response.get("msg_type")):
            return response
        # devolve a primeira resposta relevante; chamador filtra por msg_type
        return response


async def authorize(ws):
    await ws.send(json.dumps({"authorize": DERIV_TOKEN}))
    resp = json.loads(await ws.recv())
    if "error" in resp:
        raise RuntimeError(f"Falha ao autorizar: {resp['error']['message']}")
    account = resp["authorize"]
    print(f"Autenticado na conta: {account['loginid']} "
          f"({'DEMO' if account['loginid'].startswith(('VRTC', 'VRT')) else 'REAL - CUIDADO'})")
    if not account["loginid"].startswith(("VRTC", "VRT")):
        print("!!! ATENÇÃO: este NÃO parece ser um login de conta demo (VRTC/VRT). "
              "Abortando por segurança. Usa um token de conta demo. !!!")
        sys.exit(1)
    return account


async def buy_contract(ws, last_digit_dist):
    global trade_count, win_count, loss_count, total_pnl
    global trade_in_flight, consecutive_wins, cooldown_remaining

    trade_in_flight = True

    proposal_req = {
        "proposal": 1,
        "amount": STAKE,
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
        print(f"Erro na proposta: {proposal_resp['error']['message']}")
        trade_in_flight = False
        return

    proposal_id = proposal_resp["proposal"]["id"]
    ask_price = proposal_resp["proposal"]["ask_price"]

    buy_req = {"buy": proposal_id, "price": ask_price}
    await ws.send(json.dumps(buy_req))
    buy_resp = json.loads(await ws.recv())
    if "error" in buy_resp:
        print(f"Erro na compra: {buy_resp['error']['message']}")
        trade_in_flight = False
        return

    contract_id = buy_resp["buy"]["contract_id"]

    # Subscreve ao contrato até ele fechar
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
            buy_price = contract.get("buy_price", STAKE)
            pnl = payout - buy_price
            result_label = "GANHO" if pnl > 0 else "PERDA"
            # cancela a subscrição
            await ws.send(json.dumps({"forget": msg["subscription"]["id"]}))
            break

    trade_count += 1
    total_pnl += pnl
    if result_label == "GANHO":
        win_count += 1
        consecutive_wins += 1
    else:
        loss_count += 1
        consecutive_wins = 0

    if consecutive_wins >= COOLDOWN_AFTER_WINS:
        cooldown_remaining = COOLDOWN_TICKS
        consecutive_wins = 0
        print(f"  -> {COOLDOWN_AFTER_WINS} ganhos seguidos: pausa de {COOLDOWN_TICKS} ticks (regra da placa).")

    pct_1_8 = sum(last_digit_dist.get(d, 0) for d in range(1, 9))
    pct_9 = last_digit_dist.get(9, 0)

    log_trade([
        datetime.utcnow().isoformat(), SYMBOL, STAKE, CONTRACT_TYPE, BARRIER,
        result_label, f"{payout:.2f}", f"{pnl:.2f}", f"{total_pnl:.2f}",
        f"{pct_1_8:.2%}", f"{pct_9:.2%}",
    ])

    print(f"[{trade_count:4d}] {result_label:6s}  pnl={pnl:+.2f}  "
          f"saldo_acumulado={total_pnl:+.2f}  win_rate={win_count/trade_count:.1%}")

    trade_in_flight = False


async def run_bot():
    global cooldown_remaining

    if DERIV_TOKEN == "COLOQUE_AQUI_SEU_TOKEN_DEMO":
        print("!!! Configura DERIV_TOKEN (variável de ambiente ou no topo do script) "
              "com um token de conta DEMO antes de rodar. !!!")
        return

    async with websockets.connect(DERIV_WS_URL, ping_interval=20) as ws:
        await authorize(ws)

        await ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1,
        }))

        print(f"Simulação iniciada. Estratégia=Coringa Cash | Símbolo={SYMBOL} | "
              f"Stake={STAKE} | Janela={WINDOW_SIZE} ticks | Meta={MAX_TRADES} trades\n")

        while trade_count < MAX_TRADES:
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

            if should_enter(digit_window):
                dist_snapshot = digit_distribution(digit_window)
                await buy_contract(ws, dist_snapshot)

        # cancela subscrição de ticks
        await ws.send(json.dumps({"forget_all": "ticks"}))

    print_summary()


if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print_summary()
