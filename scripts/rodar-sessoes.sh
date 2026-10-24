#!/usr/bin/env bash
# rodar-sessoes.sh — orquestra as 12 sessões de pagamento do protocolo (TCC2, metodologia 3.3.3).
#
# Roda no PC (que alcança o Pi por SSH, o Hetzner por SSH e o nó C pelo Docker local).
# Para cada sessão: confere o estado dos nós, liga a leitura de energia no Pi, espera o repouso,
# executa `pagar` no Pi (em segundo plano, para uma queda de SSH não matar a sessão), desliga a
# energia, analisa, e devolve ao Pi os fundos gastos, para que todas as sessões comecem com a
# mesma distribuição de saldo.
#
# Ordem intercalada (repetição 1 de todos os caminhos/modos, depois a 2, depois a 3), para que o
# horário do dia não favoreça um modo sobre outro.
#
# USO
#   scripts/rodar-sessoes.sh --simular          só confere o estado e mostra o plano (não altera nada)
#   scripts/rodar-sessoes.sh                    roda as 12 sessões
#   SESSOES="1salto-sinc-1 2saltos-conc-1" scripts/rodar-sessoes.sh      roda só estas
#   TOTAL=5 REPOUSO_S=10 SESSOES="1salto-sinc-1" scripts/rodar-sessoes.sh   teste curto
#
# VARIÁVEIS (valores do protocolo entre parênteses)
#   TOTAL (1000)  VALOR_SAT (100)  REPOUSO_S (300)  CONCORRENCIA (10)  SESSOES (todas)
#   PERMITIR_COM_SONDAGENS=1   só para testes: NÃO use durante os 7 dias de sondagem
#
# Retomável: sessões já concluídas ficam em dados-sessoes/concluidas.txt e são puladas.

set -u -o pipefail

# --------------------------------------------------------------------------- configuração
PI_KEY=020ed1b3da0593e40fc53d460f259075c205007fc6bbdf2c7a4fe2c1e7703a6a6a
HZ_KEY=03a06f2f5b4d2f365222b51962be967d81bb6bce5741b980749f2b064e47ed2d3b
PC_KEY=036737ff34775ca8af545a3663b22a61a5139a2de78a67634b8f5bbec7a5e0cfcf

TOTAL=${TOTAL:-1000}
VALOR_SAT=${VALOR_SAT:-100}
REPOUSO_S=${REPOUSO_S:-300}
CONCORRENCIA=${CONCORRENCIA:-10}
TAXA_MAX_SAT=${TAXA_MAX_SAT:-10}
PERMITIR_COM_SONDAGENS=${PERMITIR_COM_SONDAGENS:-0}
SIMULAR=0
[ "${1:-}" = "--simular" ] && SIMULAR=1

export SSH_AUTH_SOCK=${SSH_AUTH_SOCK:-$(ls -t "$HOME"/.ssh/agent/s.* 2>/dev/null | head -1)}
SSH="ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30"

DIR=$(cd "$(dirname "$0")/.." && pwd)
SAIDA="$DIR/dados-sessoes"
mkdir -p "$SAIDA"
LOG="$SAIDA/orquestrador.log"
CONCLUIDAS="$SAIDA/concluidas.txt"
touch "$CONCLUIDAS"
COLETOR_PC="$DIR/target/release/ln-coletor"

PI_BIN='~/ln-coletor/target/release/ln-coletor'
PI_DIR='~/dados/sessoes'

# Plano: repetição -> caminho -> modo
declare -a PLANO=()
for rep in 1 2 3; do
  for caminho in 1salto 2saltos; do
    for modo in sinc conc; do
      PLANO+=("${caminho}-${modo}-${rep}")
    done
  done
done
if [ -n "${SESSOES:-}" ]; then read -r -a PLANO <<< "$SESSOES"; fi

# --------------------------------------------------------------------------- utilidades
agora() { date '+%d/%m %H:%M:%S'; }
say() { echo "[$(agora)] $*" | tee -a "$LOG"; }
fail() { say "ERRO: $*"; exit 1; }

# comandos nos nós
pi()  { $SSH pi bash -s; }                                   # script via stdin (o shell do Pi é fish)
lncli_pi() { $SSH pi "~/tcc-ln/bin/lncli --network=signet $*"; }
lncli_hz() { $SSH hetzner "docker exec tcc-ln-lnd-1 lncli --network=signet $*"; }
lncli_pc() { docker exec tcc-pc-lnd-1 lncli --network=signet "$@"; }

estado() { grep -o '"state": *"[A-Z_]*"' | cut -d'"' -f4; }

saldo_local_pi() {   # saldo local do Pi no canal com o Hetzner (sat)
  lncli_pi listchannels 2>/dev/null | python3 -c '
import sys,json
d=json.load(sys.stdin)
for c in d["channels"]:
    if c["remote_pubkey"]=="'$HZ_KEY'": print(c["local_balance"]); break'
}

caminho_de()  { echo "${1%%-*}"; }
modo_de()     { local m; m=$(echo "$1" | cut -d- -f2); [ "$m" = sinc ] && echo sincrono || echo concorrente; }
destino_de()  { [ "$(caminho_de "$1")" = 1salto ] && echo "$HZ_KEY" || echo "$PC_KEY"; }

# --------------------------------------------------------------------------- verificações
preflight() {
  say "== verificação prévia =="
  local ok=1 s
  s=$(lncli_pi state | estado);          say "  LND do Pi:      ${s:-sem resposta}";  [ "$s" = SERVER_ACTIVE ] || ok=0
  s=$(lncli_hz state | estado);          say "  LND do Hetzner: ${s:-sem resposta}";  [ "$s" = SERVER_ACTIVE ] || ok=0
  s=$(lncli_pc state | estado);          say "  LND do PC:      ${s:-sem resposta}";  [ "$s" = SERVER_ACTIVE ] || ok=0

  local nch
  nch=$(lncli_pi listchannels | grep -c '"active": true');  say "  canais ativos do Pi: $nch (esperado 1)"
  [ "$nch" -ge 1 ] || ok=0
  nch=$(lncli_hz listchannels | grep -c '"active": true');  say "  canais ativos do Hetzner: $nch (esperado 2)"
  [ "$nch" -ge 2 ] || ok=0

  if lncli_pi queryroutes --dest "$PC_KEY" --amt 1 2>/dev/null | grep -q total_fees; then
    say "  rota Pi→PC (2 saltos): ok"
  else say "  rota Pi→PC (2 saltos): NÃO ENCONTRADA"; ok=0; fi

  local saldo need
  saldo=$(saldo_local_pi); need=$(( TOTAL * VALOR_SAT + 5000 ))
  say "  saldo local do Pi no canal: ${saldo:-?} sat (precisa de pelo menos $need)"
  [ -n "$saldo" ] && [ "$saldo" -ge "$need" ] || ok=0

  if $SSH pi 'vcgencmd pmic_read_adc | head -1' >/dev/null 2>&1; then say "  leitura do PMIC no Pi: ok"
  else say "  leitura do PMIC no Pi: FALHOU"; ok=0; fi

  [ -x "$COLETOR_PC" ] && say "  análise local ($COLETOR_PC): ok" \
    || { say "  FALTA compilar a análise local: cargo build --release"; ok=0; }

  local ativo linhas
  ativo=$($SSH pi 'systemctl --user is-active ln-sondador' 2>/dev/null)
  linhas=$($SSH pi 'wc -l < ~/dados/sondagens.csv' 2>/dev/null)
  if [ "$ativo" = active ]; then
    say "  ATENÇÃO: o sondador dos 7 dias ainda está ativo (${linhas:-?} linhas). Rodar sessões agora contamina as sondagens."
    [ "$PERMITIR_COM_SONDAGENS" = 1 ] || ok=0
  else say "  sondador dos 7 dias: parado/concluído (${linhas:-?} linhas): ok"; fi

  [ "$ok" = 1 ] && say "verificação: TUDO OK" || say "verificação: HÁ PROBLEMAS (corrija antes de rodar)"
  [ "$ok" = 1 ]
}

# --------------------------------------------------------------------------- devolução de fundos
# $1 = origem (hetzner|pc)  $2 = valor em sat. Tenta o valor inteiro e, se falhar, em 4 partes.
pagar_de_volta() {
  local origem=$1 valor=$2 saida
  if [ "$origem" = hetzner ]; then
    saida=$(lncli_hz sendpayment --dest "$PI_KEY" --amt "$valor" --keysend --fee_limit 100 --force --timeout 90s 2>&1)
  else
    saida=$(lncli_pc sendpayment --dest "$PI_KEY" --amt "$valor" --keysend --fee_limit 100 --force --timeout 90s 2>&1)
  fi
  echo "$saida" | grep -q SUCCEEDED
}
devolver() {
  local origem=$1 valor=$2
  [ "$valor" -gt 0 ] || return 0
  say "  devolvendo $valor sat ($origem → Pi)"
  if pagar_de_volta "$origem" "$valor"; then say "  devolução: ok"; return 0; fi
  say "  devolução inteira falhou; tentando em 4 partes"
  local parte=$(( valor / 4 )) i resto=$valor
  for i in 1 2 3 4; do
    [ "$i" = 4 ] && parte=$resto
    pagar_de_volta "$origem" "$parte" || { say "  parte $i ($parte sat) FALHOU"; return 1; }
    resto=$(( resto - parte ))
  done
  say "  devolução em partes: ok"
}

# --------------------------------------------------------------------------- uma sessão
rodar_sessao() {
  local sessao=$1 caminho modo destino ok_n origem
  caminho=$(caminho_de "$sessao"); modo=$(modo_de "$sessao"); destino=$(destino_de "$sessao")
  say ">>> sessão $sessao  (caminho $caminho, modo $modo, $TOTAL × $VALOR_SAT sat)"

  # limpa restos de uma tentativa anterior desta sessão e prepara a pasta no Pi
  pi <<EOF
mkdir -p $PI_DIR
f=$PI_DIR/pagamentos.csv
if test -f \$f; then grep -v '^$sessao,' \$f > \$f.tmp; mv \$f.tmp \$f; fi
rm -f $PI_DIR/$sessao.rc $PI_DIR/$sessao.log $PI_DIR/energia-$sessao.csv $PI_DIR/energia.pid
EOF

  # 1) energia: liga ANTES do repouso
  pi <<EOF
nohup $PI_BIN energia --saida $PI_DIR/energia-$sessao.csv > /dev/null 2>&1 &
echo \$! > $PI_DIR/energia.pid
EOF
  say "  energia ligada; repouso de ${REPOUSO_S} s (sem outros comandos no Pi)"
  sleep "$REPOUSO_S"

  # 2) sessão em segundo plano no Pi
  pi <<EOF
nohup bash -c "$PI_BIN pagar --destino $destino --valor-sat $VALOR_SAT --total $TOTAL --modo $modo --concorrencia $CONCORRENCIA --sessao $sessao --taxa-max-sat $TAXA_MAX_SAT --saida $PI_DIR/pagamentos.csv > $PI_DIR/$sessao.log 2>&1; echo \\\$? > $PI_DIR/$sessao.rc" > /dev/null 2>&1 &
EOF
  say "  pagamentos em andamento no Pi"
  local rc="" t=0
  while [ -z "$rc" ]; do
    sleep 10; t=$((t+10))
    rc=$($SSH pi "cat $PI_DIR/$sessao.rc 2>/dev/null")
    [ "$t" -gt 7200 ] && { say "  tempo excedido (2 h)"; break; }
  done
  [ "$rc" = 0 ] || say "  AVISO: código de saída da sessão: ${rc:-?}"

  # 3) para a energia (um pouco depois do fim da sessão)
  sleep 5
  pi <<EOF
kill \$(cat $PI_DIR/energia.pid) 2>/dev/null
EOF
  say "  energia desligada"

  # 4) traz os dados e analisa
  scp -q "pi:dados/sessoes/pagamentos.csv" "$SAIDA/pagamentos.csv" || fail "não consegui copiar pagamentos.csv"
  scp -q "pi:dados/sessoes/energia-$sessao.csv" "$SAIDA/energia-$sessao.csv" || fail "não consegui copiar a energia"
  scp -q "pi:dados/sessoes/$sessao.log" "$SAIDA/$sessao.log" 2>/dev/null
  "$COLETOR_PC" analisar-sessao --pagamentos "$SAIDA/pagamentos.csv" --sessao "$sessao" \
      --energia "$SAIDA/energia-$sessao.csv" --repouso-s "$REPOUSO_S" --resumo "$SAIDA/resumo.csv" 2>&1 | tee -a "$LOG"

  # 5) devolve os fundos gastos (só os pagamentos que tiveram sucesso)
  ok_n=$(grep -c "^$sessao,.*,SUCCEEDED," "$SAIDA/pagamentos.csv")
  [ "$caminho" = 1salto ] && origem=hetzner || origem=pc
  devolver "$origem" $(( ok_n * VALOR_SAT )) || say "  AVISO: devolução incompleta; confira os saldos antes da próxima sessão"
  say "  saldo local do Pi no canal: $(saldo_local_pi) sat"

  echo "$sessao" >> "$CONCLUIDAS"
  say "<<< sessão $sessao concluída"
  sleep 60   # assenta os canais antes da próxima sessão
}

# --------------------------------------------------------------------------- principal
exec 9>/tmp/rodar-sessoes.lock
flock -n 9 || fail "já há uma execução em andamento (trava /tmp/rodar-sessoes.lock)"

say "=== orquestrador de sessões: $(echo "${PLANO[@]}" | wc -w) sessões planejadas; TOTAL=$TOTAL VALOR_SAT=$VALOR_SAT REPOUSO_S=$REPOUSO_S ==="
say "plano: ${PLANO[*]}"
preflight || { [ "$SIMULAR" = 1 ] && exit 1 || fail "verificação prévia falhou"; }

if [ "$SIMULAR" = 1 ]; then
  say "modo --simular: nada foi executado. Sessões que seriam rodadas:"
  for s in "${PLANO[@]}"; do
    grep -qx "$s" "$CONCLUIDAS" && say "  $s (já concluída, seria pulada)" \
      || say "  $s → destino $(destino_de "$s" | cut -c1-12)…, modo $(modo_de "$s")"
  done
  exit 0
fi

for s in "${PLANO[@]}"; do
  if grep -qx "$s" "$CONCLUIDAS"; then say "pulando $s (já concluída)"; continue; fi
  rodar_sessao "$s"
done

say "=== TODAS AS SESSÕES CONCLUÍDAS ==="
say "resumo em $SAIDA/resumo.csv"
column -s, -t "$SAIDA/resumo.csv" 2>/dev/null | cut -c1-200 | tee -a "$LOG"
