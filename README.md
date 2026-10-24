# ln-coletor

Coleta e análise de métricas da Lightning Network (LND v0.21.4-beta, rede signet), escrita em Rust. Foi desenvolvida para o
trabalho de conclusão de curso *Viabilidade da Lightning Network para pagamentos de varejo em hardware embarcado*
(UTFPR, campus Cornélio Procópio, 2026), que mede latência, vazão, taxa de sucesso, custo de roteamento, energia e
disponibilidade de um nó em Raspberry Pi 5 e as compara com o critério de viabilidade do Pix.

## Comandos

| Comando | O que faz | Roda onde |
|---|---|---|
| `info` | Mostra o estado do LND (teste de conexão) | Pi |
| `sondar` | Uma sondagem (keysend, 1 sat) a cada 60 s; grava `sondagens.csv`; retoma a contagem se reiniciar | Pi (serviço `ln-sondador`) |
| `pagar` | Sessão de pagamentos, `--modo sincrono` ou `concorrente`; grava `pagamentos.csv` e imprime um resumo | Pi |
| `energia` | Lê o PMIC (`vcgencmd pmic_read_adc`) a ~1 Hz e grava `energia.csv` | Pi |
| `analisar-sessao` | Estatísticas da sessão, vazão por janelas de 60 s e energia adicional por pagamento | qualquer máquina |
| `analisar-sondagens` | Disponibilidade com e sem exclusão de falhas, latência, motivos de falha e lacunas | qualquer máquina |

## Rodar uma sessão (exemplo, no Pi)

```sh
ln-coletor energia --saida energia.csv &            # inicie ANTES: precisa de 5 min de repouso
sleep 300
ln-coletor pagar --destino <PUBKEY> --sessao 1salto-sinc-1 --modo sincrono --saida pagamentos.csv
```

## Analisar

```sh
ln-coletor analisar-sessao --pagamentos pagamentos.csv --sessao 1salto-sinc-1 \
    --energia energia.csv --resumo resumo.csv      # --resumo acrescenta uma linha por sessão

scp pi:dados/sondagens.csv .   &&   scp hetzner:/opt/tcc-ln/saude.csv .
ln-coletor analisar-sondagens --sondagens sondagens.csv --saude saude.csv
```

## Regras de cálculo (metodologia 3.3.3 e 3.3.5)

- Latência: do envio até o estado final do pagamento; estatísticas só dos pagamentos com sucesso.
- Desvio-padrão amostral (n-1); percentis por interpolação linear.
- Vazão: pagamentos bem-sucedidos por segundo em janelas fixas de 60 s (a última janela, se incompleta, é descartada).
- Energia adicional por pagamento = (potência média na sessão − potência média nos 300 s anteriores) × duração ÷ pagamentos bem-sucedidos. A potência é a soma de tensão × corrente dos trilhos do PMIC e **subestima** a potência na tomada.
- Falha atribuível ao nó auxiliar: a amostra de saúde mais próxima (≤ 90 s) estava ruim (LND fora de `SERVER_ACTIVE`, não sincronizado, sem canal ativo ou com erro), ou não há amostra de saúde no intervalo. Na disponibilidade COM exclusão, essas falhas saem do numerador e do denominador.

## Compilar

O código gRPC está em `src/gen` (gerado dos `.proto` do LND v0.21.4-beta), então não precisa de `protoc`.

```sh
cargo build --release
cargo test
```

## Sessões de pagamento automáticas (`scripts/rodar-sessoes.sh`)

Orquestra as 12 sessões do protocolo (3 repetições × 2 caminhos × 2 modos, em ordem intercalada).
Roda **no PC** (alcança o Pi e o Hetzner por SSH e o nó C pelo Docker). Para cada sessão: confere os nós,
liga a leitura de energia no Pi, espera o repouso (300 s), roda `pagar` no Pi em segundo plano, desliga a
energia, analisa, e devolve ao Pi os fundos gastos (Hetzner→Pi no caminho de 1 salto, PC→Pi no de 2).

```sh
cargo build --release                      # análise local
scripts/rodar-sessoes.sh --simular         # só confere o estado e mostra o plano (não altera nada)
scripts/rodar-sessoes.sh                   # roda as 12 sessões (~4 h)
```

- **Só rode depois que as sondagens de 7 dias terminarem**: o script recusa se o `ln-sondador` ainda estiver ativo
  (`PERMITIR_COM_SONDAGENS=1` ignora isso, só para testes).
- Retomável: as sessões concluídas ficam em `dados-sessoes/concluidas.txt` e são puladas.
- Saídas em `dados-sessoes/`: `pagamentos.csv`, `energia-<sessão>.csv`, `resumo.csv` (uma linha por sessão), `orquestrador.log`.
- **Teste curto antes da execução real** (e limpeza depois, senão a sessão de teste conta como feita):

```sh
TOTAL=5 REPOUSO_S=10 SESSOES="1salto-sinc-1" scripts/rodar-sessoes.sh
rm -rf dados-sessoes && ssh pi 'rm -rf ~/dados/sessoes'
```

## Dados do experimento

Os dados do TCC estão em `dados-experimento-2026-10-24/` (com `SHA256SUMS`):

- `sondagens/`: 10.080 sondagens de 1 sat, uma a cada 60 s, de 17/10/2026 05:31 a 24/10/2026 05:32 (UTC−3), e o `saude.csv` do servidor auxiliar.
- `sessoes/`: 12 sessões de 1.000 pagamentos de 100 sat (24/10/2026, 06:20 a 09:33), com `resumo.csv`, `pagamentos.csv` (12.000 linhas), a energia de cada sessão (`energia-*.csv`) e os registros do orquestrador.

Resultado resumido: 22.080 operações sem falha; no caminho direto, mediana de 797 ms (síncrono) e 947 ms (concorrente) e
percentil 99 de 882 ms e 1.379 ms; a vazão chegou a 10,0 pagamentos/s com dez pagamentos simultâneos.
A vazão é limitada pela concorrência configurada, e não pelo nó; a energia vem da telemetria interna da placa (ver as regras acima).

## Cuidados ao reproduzir

- **Reserva do canal.** O nó que recebe os pagamentos de 2 saltos precisa terminar com saldo local acima da reserva do canal
  (1% da capacidade, 1.500 sat em um canal de 150.000 sat). Abaixo disso o LND não deixa o nó enviar, e a devolução dos fundos
  falha. No experimento foi preciso enviar 1.100 sat ao nó C antes das sessões.
- **Canal desabilitado.** Se um par fica fora do ar, o LND anuncia o canal como desabilitado e só o reabilita depois de cerca de
  19 min de par estável. Para reabilitar antes: `lncli updatechanstatus --chan_point <txid:idx> --action enable`, no nó que anuncia.
- Os nós, as chaves públicas e os endereços em `scripts/rodar-sessoes.sh` são os do experimento (signet); troque-os pelos seus.
- O `analisar-sondagens --janela-encerrada` trata sondagens ausentes como falha; use-o só quando a janela de coleta acabou.

## Licença

MIT (ver `LICENSE`). Os arquivos em `proto/` e `src/gen/` vêm do LND (licença MIT, Lightning Labs) e mantêm a licença de origem.
