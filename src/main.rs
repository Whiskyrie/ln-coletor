//! ln-coletor: coleta de métricas da Lightning Network para o TCC2.
//!
//! Comandos:
//!   info    consulta o LND e mostra o estado (teste de conexão);
//!   sondar  envia um pagamento de sondagem (keysend) a cada intervalo e registra o resultado em CSV;
//!   pagar   executa uma sessão de pagamentos (síncrona ou concorrente) e registra cada um em CSV;
//!   energia lê a telemetria do PMIC do Raspberry Pi (~1 Hz) e registra a potência estimada em CSV.

mod analise;

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use rand::RngCore;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Certificate, Channel, ClientTlsConfig},
    Request, Status,
};

pub mod lnrpc {
    include!("gen/lnrpc.rs");
}
pub mod routerrpc {
    include!("gen/routerrpc.rs");
}

use lnrpc::lightning_client::LightningClient;
use routerrpc::router_client::RouterClient;

/// Registro de tipo personalizado do keysend (BOLT: preimagem do pagamento).
const KEYSEND_PREIMAGE_TLV: u64 = 5_482_373_484;

#[derive(Parser)]
#[command(name = "ln-coletor", version, about = "Coleta de métricas da Lightning Network (TCC2)")]
struct Cli {
    #[command(flatten)]
    conexao: Conexao,
    #[command(subcommand)]
    comando: Comando,
}

#[derive(Args)]
struct Conexao {
    /// Endereço gRPC do LND (host:porta).
    #[arg(long, default_value = "127.0.0.1:10009", global = true)]
    endereco: String,
    /// Certificado TLS do LND.
    #[arg(long, default_value = "~/.lnd/tls.cert", global = true)]
    cert: String,
    /// Macaroon usado na autenticação.
    #[arg(
        long,
        default_value = "~/.lnd/data/chain/bitcoin/signet/admin.macaroon",
        global = true
    )]
    macaroon: String,
}

#[derive(Subcommand)]
enum Comando {
    /// Consulta o LND e mostra o estado do nó.
    Info,
    /// Envia uma sondagem a cada intervalo e grava o resultado em CSV.
    Sondar(Sondagem),
    /// Executa uma sessão de pagamentos e grava cada pagamento em CSV.
    Pagar(Sessao),
    /// Registra a potência estimada pela telemetria do PMIC (Raspberry Pi 5).
    Energia(Energia),
    /// Resume uma sessão de pagamentos: estatísticas, vazão por janelas de 60 s e energia.
    AnalisarSessao(ArgsSessao),
    /// Resume as sondagens de disponibilidade e compara com os limiares do critério de viabilidade.
    AnalisarSondagens(ArgsSondagens),
}

#[derive(Args)]
struct ArgsSessao {
    /// CSV gerado pelo comando `pagar`.
    #[arg(long)]
    pagamentos: PathBuf,
    /// Rótulo da sessão a resumir.
    #[arg(long)]
    sessao: String,
    /// CSV de energia (comando `energia`) coletado durante a sessão e nos minutos anteriores.
    #[arg(long)]
    energia: Option<PathBuf>,
    /// Janela de repouso, em segundos, imediatamente antes da sessão (protocolo: 300).
    #[arg(long, default_value_t = 300)]
    repouso_s: u64,
    /// Acrescenta uma linha-resumo a este CSV (para montar o quadro de resultados).
    #[arg(long)]
    resumo: Option<PathBuf>,
}

#[derive(Args)]
struct ArgsSondagens {
    /// CSV gerado pelo comando `sondar`.
    #[arg(long, default_value = "sondagens.csv")]
    sondagens: PathBuf,
    /// CSV do verificador de saúde do nó auxiliar (para excluir falhas atribuíveis a ele).
    #[arg(long)]
    saude: Option<PathBuf>,
    /// Número de sondagens esperadas na janela completa.
    #[arg(long, default_value_t = 10_080)]
    esperado: usize,
    #[arg(long, default_value_t = 6_000.0)]
    limiar_mediana_ms: f64,
    #[arg(long, default_value_t = 10_000.0)]
    limiar_p99_ms: f64,
    #[arg(long, default_value_t = 99.9)]
    limiar_disponibilidade_pct: f64,
    #[arg(long, default_value_t = 99.0)]
    limiar_sucesso_pct: f64,
    /// A janela de sondagens já terminou: sondagens esperadas e não registradas contam como falha.
    #[arg(long)]
    janela_encerrada: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum Modo {
    Sincrono,
    Concorrente,
}

impl Modo {
    fn nome(self) -> &'static str {
        match self {
            Modo::Sincrono => "sincrono",
            Modo::Concorrente => "concorrente",
        }
    }
}

#[derive(Args)]
struct Sessao {
    /// Chave pública (hex) do nó de destino.
    #[arg(long)]
    destino: String,
    /// Valor de cada pagamento, em satoshis.
    #[arg(long, default_value_t = 100)]
    valor_sat: i64,
    /// Número de pagamentos da sessão.
    #[arg(long, default_value_t = 1_000)]
    total: u64,
    /// Modo de execução.
    #[arg(long, value_enum, default_value_t = Modo::Sincrono)]
    modo: Modo,
    /// Pagamentos simultâneos no modo concorrente.
    #[arg(long, default_value_t = 10)]
    concorrencia: usize,
    /// Rótulo da sessão (aparece em cada linha do CSV), por exemplo "1salto-sinc-1".
    #[arg(long)]
    sessao: String,
    /// Tempo máximo de cada pagamento, em segundos.
    #[arg(long, default_value_t = 30)]
    tempo_limite_s: i32,
    /// Limite de taxa por pagamento, em satoshis.
    #[arg(long, default_value_t = 10)]
    taxa_max_sat: i64,
    /// Arquivo CSV de saída (acrescenta ao final se já existir).
    #[arg(long, default_value = "pagamentos.csv")]
    saida: PathBuf,
}

#[derive(Args)]
struct Energia {
    /// Intervalo entre leituras, em milissegundos.
    #[arg(long, default_value_t = 1_000)]
    intervalo_ms: u64,
    /// Arquivo CSV de saída.
    #[arg(long, default_value = "energia.csv")]
    saida: PathBuf,
}

#[derive(Args)]
struct Sondagem {
    /// Chave pública (hex) do nó de destino.
    #[arg(long)]
    destino: String,
    /// Valor de cada sondagem, em satoshis.
    #[arg(long, default_value_t = 1)]
    valor_sat: i64,
    /// Intervalo entre sondagens, em segundos.
    #[arg(long, default_value_t = 60)]
    intervalo_s: u64,
    /// Número total de sondagens (10.080 = 7 dias a cada 60 s).
    #[arg(long, default_value_t = 10_080)]
    total: u64,
    /// Tempo máximo de cada pagamento, em segundos.
    #[arg(long, default_value_t = 30)]
    tempo_limite_s: i32,
    /// Limite de taxa por sondagem, em satoshis.
    #[arg(long, default_value_t = 10)]
    taxa_max_sat: i64,
    /// Arquivo CSV de saída (acrescenta ao final se já existir).
    #[arg(long, default_value = "sondagens.csv")]
    saida: PathBuf,
}

/// Adiciona o macaroon a cada chamada gRPC.
#[derive(Clone)]
struct Macaroon(MetadataValue<Ascii>);

impl Interceptor for Macaroon {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut().insert("macaroon", self.0.clone());
        Ok(req)
    }
}

type Servico = InterceptedService<Channel, Macaroon>;

struct Clientes {
    ln: LightningClient<Servico>,
    router: RouterClient<Servico>,
}

fn expandir(caminho: &str) -> PathBuf {
    match caminho.strip_prefix("~/") {
        Some(resto) => PathBuf::from(std::env::var("HOME").unwrap_or_default()).join(resto),
        None => PathBuf::from(caminho),
    }
}

async fn conectar(c: &Conexao) -> Result<Clientes> {
    let cert = fs::read(expandir(&c.cert)).context("lendo o certificado TLS do LND")?;
    let mac = fs::read(expandir(&c.macaroon)).context("lendo o macaroon do LND")?;
    let mac_hex = hex::encode(mac);
    let mac_val: MetadataValue<Ascii> = mac_hex.parse().map_err(|_| anyhow!("macaroon inválido"))?;

    // O certificado do LND é autoassinado e válido para "localhost".
    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(cert))
        .domain_name("localhost");
    let canal = Channel::from_shared(format!("https://{}", c.endereco))?
        .tls_config(tls)?
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .with_context(|| format!("conectando ao LND em {}", c.endereco))?;
    let interceptor = Macaroon(mac_val);
    Ok(Clientes {
        ln: LightningClient::with_interceptor(canal.clone(), interceptor.clone()),
        router: RouterClient::with_interceptor(canal, interceptor),
    })
}

fn agora_ns() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0)
}

/// Uma linha do CSV (sondagem ou pagamento de sessão).
#[derive(Serialize, Clone)]
struct Linha {
    sessao: String,
    modo: String,
    seq: u64,
    inicio_unix_ns: u128,
    fim_unix_ns: u128,
    valor_sat: i64,
    status: String,
    falha: String,
    latencia_ms: f64,
    taxa_msat: i64,
    erro_rpc: String,
}

async fn cmd_info(cl: &mut Clientes) -> Result<()> {
    let info = cl.ln.get_info(lnrpc::GetInfoRequest {}).await?.into_inner();
    println!("alias:            {}", info.alias);
    println!("versão:           {}", info.version);
    println!("chave pública:    {}", info.identity_pubkey);
    println!("altura do bloco:  {}", info.block_height);
    println!("sincronizado:     cadeia={} grafo={}", info.synced_to_chain, info.synced_to_graph);
    println!("canais ativos:    {}", info.num_active_channels);
    println!("pares:            {}", info.num_peers);
    Ok(())
}

/// Envia um pagamento keysend e devolve a linha de resultado.
async fn enviar_keysend(
    router: &mut RouterClient<Servico>,
    sessao: &str,
    modo: &str,
    seq: u64,
    destino: &[u8],
    valor_sat: i64,
    tempo_limite_s: i32,
    taxa_max_sat: i64,
) -> Linha {
    let inicio_ns = agora_ns();
    let t0 = Instant::now();

    let mut preimagem = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut preimagem);
    let hash = Sha256::digest(preimagem).to_vec();

    let req = routerrpc::SendPaymentRequest {
        dest: destino.to_vec(),
        amt: valor_sat,
        payment_hash: hash,
        final_cltv_delta: 40,
        timeout_seconds: tempo_limite_s,
        fee_limit_sat: taxa_max_sat,
        no_inflight_updates: true,
        dest_custom_records: [(KEYSEND_PREIMAGE_TLV, preimagem.to_vec())].into_iter().collect(),
        ..Default::default()
    };

    let mut linha = Linha {
        sessao: sessao.to_string(),
        modo: modo.to_string(),
        seq,
        inicio_unix_ns: inicio_ns,
        fim_unix_ns: 0,
        valor_sat,
        status: String::new(),
        falha: String::new(),
        latencia_ms: 0.0,
        taxa_msat: 0,
        erro_rpc: String::new(),
    };

    match router.send_payment_v2(req).await {
        Err(e) => {
            linha.status = "ERRO_RPC".into();
            linha.erro_rpc = e.message().to_string();
        }
        Ok(resp) => {
            let mut fluxo = resp.into_inner();
            loop {
                match fluxo.message().await {
                    Ok(Some(p)) => {
                        let status = lnrpc::payment::PaymentStatus::try_from(p.status)
                            .map(|v| v.as_str_name().to_string())
                            .unwrap_or_else(|_| format!("DESCONHECIDO({})", p.status));
                        // SUCCEEDED e FAILED são estados finais.
                        if status == "SUCCEEDED" || status == "FAILED" {
                            linha.falha = lnrpc::PaymentFailureReason::try_from(p.failure_reason)
                                .map(|v| v.as_str_name().to_string())
                                .unwrap_or_default();
                            linha.taxa_msat = p.fee_msat;
                            linha.status = status;
                            break;
                        }
                    }
                    Ok(None) => {
                        linha.status = "FLUXO_ENCERRADO".into();
                        break;
                    }
                    Err(e) => {
                        linha.status = "ERRO_RPC".into();
                        linha.erro_rpc = e.message().to_string();
                        break;
                    }
                }
            }
        }
    }

    linha.latencia_ms = t0.elapsed().as_secs_f64() * 1000.0;
    linha.fim_unix_ns = agora_ns();
    linha
}

/// Número de linhas de dados já gravadas no CSV (0 se o arquivo não existir).
fn contar_linhas(caminho: &Path) -> u64 {
    match fs::read_to_string(caminho) {
        Ok(t) => (t.lines().count() as u64).saturating_sub(1),
        Err(_) => 0,
    }
}

fn abrir_csv(caminho: &Path) -> Result<csv::Writer<fs::File>> {
    let novo = !caminho.exists() || fs::metadata(caminho).map(|m| m.len() == 0).unwrap_or(true);
    let arquivo = fs::OpenOptions::new().create(true).append(true).open(caminho)?;
    Ok(csv::WriterBuilder::new().has_headers(novo).from_writer(arquivo))
}

async fn cmd_sondar(cl: &mut Clientes, s: Sondagem) -> Result<()> {
    let destino = hex::decode(&s.destino).context("o destino deve ser a chave pública em hexadecimal")?;
    if destino.len() != 33 {
        return Err(anyhow!("a chave pública deve ter 33 bytes (66 caracteres hexadecimais)"));
    }
    // Retoma de onde parou: se o serviço reiniciar, a contagem continua e não passa de `total`.
    let feitas = contar_linhas(&s.saida);
    if feitas > 0 {
        eprintln!("retomando: {feitas} sondagens já registradas em {}", s.saida.display());
    }
    let mut csv = abrir_csv(&s.saida)?;
    let mut tick = tokio::time::interval(Duration::from_secs(s.intervalo_s));
    // Se uma sondagem demorar mais que o intervalo, pula os ticks perdidos em vez de acumulá-los.
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let (mut ok, mut falhas) = (0u64, 0u64);
    for seq in feitas..s.total {
        tokio::select! {
            _ = tick.tick() => {}
            _ = tokio::signal::ctrl_c() => {
                eprintln!("interrompido; sondagens feitas: {seq}");
                break;
            }
        }
        let linha = enviar_keysend(&mut cl.router, "sondagem", "sondagem", seq, &destino, s.valor_sat, s.tempo_limite_s, s.taxa_max_sat).await;
        if linha.status == "SUCCEEDED" {
            ok += 1;
        } else {
            falhas += 1;
        }
        eprintln!(
            "#{seq} {} {:.0} ms {}{}",
            linha.status,
            linha.latencia_ms,
            linha.falha,
            linha.erro_rpc
        );
        csv.serialize(&linha)?;
        csv.flush()?; // grava a cada sondagem: uma queda não perde dados
    }
    eprintln!("fim: {ok} com sucesso, {falhas} falhas");
    Ok(())
}

use analise::percentil;

/// Executa uma sessão de pagamentos e imprime um resumo ao final.
async fn cmd_pagar(cl: &mut Clientes, s: Sessao) -> Result<()> {
    let destino = hex::decode(&s.destino).context("o destino deve ser a chave pública em hexadecimal")?;
    if destino.len() != 33 {
        return Err(anyhow!("a chave pública deve ter 33 bytes (66 caracteres hexadecimais)"));
    }
    let mut csv = abrir_csv(&s.saida)?;
    let modo = s.modo.nome();
    let inicio = Instant::now();
    let inicio_ns = agora_ns();
    let mut linhas: Vec<Linha> = Vec::with_capacity(s.total as usize);

    match s.modo {
        Modo::Sincrono => {
            for seq in 0..s.total {
                let l = enviar_keysend(&mut cl.router, &s.sessao, modo, seq, &destino, s.valor_sat, s.tempo_limite_s, s.taxa_max_sat).await;
                csv.serialize(&l)?;
                csv.flush()?;
                linhas.push(l);
            }
        }
        Modo::Concorrente => {
            let permissoes = Arc::new(tokio::sync::Semaphore::new(s.concorrencia.max(1)));
            let mut tarefas = tokio::task::JoinSet::new();
            for seq in 0..s.total {
                let permissao = permissoes.clone().acquire_owned().await?;
                let mut router = cl.router.clone();
                let (sessao, destino, valor, tl, tx) = (s.sessao.clone(), destino.clone(), s.valor_sat, s.tempo_limite_s, s.taxa_max_sat);
                tarefas.spawn(async move {
                    let l = enviar_keysend(&mut router, &sessao, modo, seq, &destino, valor, tl, tx).await;
                    drop(permissao);
                    l
                });
                // Grava os que já terminaram, sem bloquear o envio dos próximos.
                while let Some(r) = tarefas.try_join_next() {
                    let l = r?;
                    csv.serialize(&l)?;
                    linhas.push(l);
                }
            }
            while let Some(r) = tarefas.join_next().await {
                let l = r?;
                csv.serialize(&l)?;
                linhas.push(l);
            }
            csv.flush()?;
        }
    }

    let duracao = inicio.elapsed().as_secs_f64();
    let fim_ns = agora_ns();
    let ok: Vec<&Linha> = linhas.iter().filter(|l| l.status == "SUCCEEDED").collect();
    let mut lat: Vec<f64> = ok.iter().map(|l| l.latencia_ms).collect();
    lat.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let media = if lat.is_empty() { f64::NAN } else { lat.iter().sum::<f64>() / lat.len() as f64 };
    eprintln!("sessão {} ({modo}): {} pagamentos em {:.1} s", s.sessao, linhas.len(), duracao);
    eprintln!("  início/fim (unix ns): {inicio_ns} / {fim_ns}");
    eprintln!("  sucesso: {} ({:.2}%)", ok.len(), 100.0 * ok.len() as f64 / linhas.len().max(1) as f64);
    eprintln!("  vazão: {:.3} pagamentos/s", ok.len() as f64 / duracao);
    eprintln!(
        "  latência ms: média {:.1}, mediana {:.1}, p95 {:.1}, p99 {:.1}",
        media,
        percentil(&lat, 50.0),
        percentil(&lat, 95.0),
        percentil(&lat, 99.0)
    );
    eprintln!("  taxa total: {} msat", ok.iter().map(|l| l.taxa_msat).sum::<i64>());
    Ok(())
}

/// Linha do CSV de energia.
#[derive(Serialize)]
struct LinhaEnergia {
    unix_ns: u128,
    potencia_w: f64,
    trilhos: usize,
}

/// Soma tensão x corrente dos trilhos que têm as duas leituras (saída de `vcgencmd pmic_read_adc`).
fn potencia_pmic(saida: &str) -> (f64, usize) {
    let mut corrente: BTreeMap<String, f64> = BTreeMap::new();
    let mut tensao: BTreeMap<String, f64> = BTreeMap::new();
    for linha in saida.lines() {
        let linha = linha.trim();
        let Some((nome_lido, valor)) = linha.split_once('=') else { continue };
        let Some(nome) = nome_lido.split_whitespace().next() else { continue };
        let numero: f64 = valor.trim_end_matches(['A', 'V']).parse().unwrap_or(f64::NAN);
        if let Some(base) = nome.strip_suffix("_A") {
            corrente.insert(base.to_string(), numero);
        } else if let Some(base) = nome.strip_suffix("_V") {
            tensao.insert(base.to_string(), numero);
        }
    }
    let mut total = 0.0;
    let mut n = 0;
    for (base, i) in &corrente {
        if let Some(v) = tensao.get(base) {
            if i.is_finite() && v.is_finite() {
                total += i * v;
                n += 1;
            }
        }
    }
    (total, n)
}

async fn cmd_energia(e: Energia) -> Result<()> {
    let mut csv = abrir_csv(&e.saida)?;
    let mut tick = tokio::time::interval(Duration::from_millis(e.intervalo_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            _ = tokio::signal::ctrl_c() => break,
        }
        let t = agora_ns();
        let saida = tokio::process::Command::new("vcgencmd").arg("pmic_read_adc").output().await
            .context("executando vcgencmd (este comando só existe no Raspberry Pi)")?;
        let (w, n) = potencia_pmic(&String::from_utf8_lossy(&saida.stdout));
        csv.serialize(&LinhaEnergia { unix_ns: t, potencia_w: w, trilhos: n })?;
        csv.flush()?;
    }
    Ok(())
}

fn linha_estat(rotulo: &str, e: &analise::Estatisticas) -> String {
    format!(
        "  {rotulo}: média {}, mediana {}, p95 {}, p99 {}, desvio-padrão {}",
        analise::br(e.media, 1),
        analise::br(e.mediana, 1),
        analise::br(e.p95, 1),
        analise::br(e.p99, 1),
        analise::br(e.desvio, 1)
    )
}

fn cmd_analisar_sessao(a: ArgsSessao) -> Result<()> {
    let linhas: Vec<analise::LinhaCsv> = analise::ler_csv(&a.pagamentos)?;
    let energia = match &a.energia {
        Some(c) => Some(analise::ler_energia(c)?),
        None => None,
    };
    let r = analise::resumir_sessao(&linhas, &a.sessao, energia.as_deref(), a.repouso_s)
        .ok_or_else(|| anyhow!("nenhuma linha da sessão '{}' em {}", a.sessao, a.pagamentos.display()))?;

    println!("Sessão {} (modo {}): {} pagamentos em {} s", r.sessao, r.modo, r.total, analise::br(r.duracao_s, 1));
    println!("  sucesso: {} de {} ({} %)", r.ok, r.total, analise::br(r.sucesso_pct, 2));
    match r.vazao_janelas {
        Some(v) => println!(
            "  vazão por janelas de 60 s: {} pagamentos/s ({} janelas completas); vazão média da sessão: {} pagamentos/s",
            analise::br(v, 3), r.janelas, analise::br(r.vazao_total, 3)
        ),
        None => println!(
            "  vazão: sessão menor que 60 s, sem janela completa; vazão média da sessão: {} pagamentos/s",
            analise::br(r.vazao_total, 3)
        ),
    }
    if let Some(e) = &r.latencia {
        println!("{}", linha_estat("latência (ms, só sucessos)", e));
    }
    println!("  taxa de roteamento: total {} msat, média {} msat/pagamento", r.taxa_total_msat, analise::br(r.taxa_media_msat, 1));
    match (r.p_repouso_w, r.p_sessao_w, r.energia_extra_mj) {
        (Some(rep), Some(ses), extra) => {
            println!(
                "  energia: repouso {} W ({} amostras), sessão {} W ({} amostras)",
                analise::br(rep, 3), r.amostras_repouso, analise::br(ses, 3), r.amostras_sessao
            );
            if let Some(x) = extra {
                println!("  energia adicional por pagamento: {} mJ", analise::br(x, 1));
            }
            if r.amostras_repouso < (a.repouso_s as usize) / 2 {
                println!("  AVISO: poucas amostras de repouso ({} para {} s); confira se a coleta de energia começou antes da sessão", r.amostras_repouso, a.repouso_s);
            }
        }
        _ => {
            if a.energia.is_some() {
                println!("  energia: sem amostras suficientes no repouso ou na sessão (confira os horários do CSV de energia)");
            }
        }
    }

    if let Some(destino) = &a.resumo {
        let novo = !destino.exists();
        let arq = fs::OpenOptions::new().create(true).append(true).open(destino)?;
        let mut w = csv::WriterBuilder::new().has_headers(false).from_writer(arq);
        if novo {
            w.write_record(["sessao","modo","pagamentos","sucessos","sucesso_pct","duracao_s","vazao_janelas_ps","janelas","vazao_media_ps","lat_media_ms","lat_mediana_ms","lat_p95_ms","lat_p99_ms","lat_desvio_ms","taxa_total_msat","taxa_media_msat","p_repouso_w","p_sessao_w","energia_extra_mj_por_pagamento"])?;
        }
        let f = |x: Option<f64>| x.map(|v| format!("{v:.4}")).unwrap_or_default();
        let l = r.latencia;
        w.write_record([
            r.sessao.clone(), r.modo.clone(), r.total.to_string(), r.ok.to_string(), format!("{:.4}", r.sucesso_pct),
            format!("{:.3}", r.duracao_s), f(r.vazao_janelas), r.janelas.to_string(), format!("{:.4}", r.vazao_total),
            f(l.map(|e| e.media)), f(l.map(|e| e.mediana)), f(l.map(|e| e.p95)), f(l.map(|e| e.p99)), f(l.map(|e| e.desvio)),
            r.taxa_total_msat.to_string(), format!("{:.2}", r.taxa_media_msat), f(r.p_repouso_w), f(r.p_sessao_w), f(r.energia_extra_mj),
        ])?;
        w.flush()?;
        println!("  linha-resumo acrescentada a {}", destino.display());
    }
    Ok(())
}

fn atende(ok: bool) -> &'static str {
    if ok { "ATENDE" } else { "NÃO ATENDE" }
}

fn cmd_analisar_sondagens(a: ArgsSondagens) -> Result<()> {
    let linhas: Vec<analise::LinhaCsv> = analise::ler_csv(&a.sondagens)?;
    let saude: Option<Vec<analise::LinhaSaude>> = match &a.saude {
        Some(c) => Some(analise::ler_csv(c)?),
        None => None,
    };
    let r = analise::resumir_sondagens(
        &linhas,
        saude.as_deref(),
        a.janela_encerrada.then_some(a.esperado),
    );
    let max_falhas = ((a.esperado as f64) * (1.0 - a.limiar_disponibilidade_pct / 100.0)).floor() as usize;

    println!("Sondagens registradas: {} de {} esperadas ({} %)", r.total, a.esperado, analise::br(100.0 * r.total as f64 / a.esperado as f64, 1));
    if r.total < a.esperado {
        println!("  AVISO: janela incompleta; os resultados abaixo são parciais");
    }
    println!("  sucesso: {} ({} %); falhas: {}", r.ok, analise::br(r.sucesso_pct, 3), r.falhas);
    if r.faltantes > 0 {
        println!("  sondagens não registradas, contadas como falha (janela encerrada): {}", r.faltantes);
    }
    if saude.is_some() {
        println!("  falhas atribuíveis ao nó auxiliar (saúde ruim ou verificador fora do ar): {}; demais falhas: {}", r.falhas_atribuiveis, r.falhas_restantes);
    } else {
        println!("  sem arquivo de saúde: nenhuma falha foi excluída");
    }
    println!("  disponibilidade SEM exclusão: {} %", analise::br(r.disponibilidade_sem_exclusao, 3));
    println!("  disponibilidade COM exclusão: {} %", analise::br(r.disponibilidade_com_exclusao, 3));
    println!("  (com {} sondagens e limiar de {} %, o máximo de falhas admitido é {})", a.esperado, analise::br(a.limiar_disponibilidade_pct, 1), max_falhas);
    if let Some(e) = &r.latencia {
        println!("{}", linha_estat("latência das sondagens com sucesso (ms)", e));
    }
    if !r.motivos.is_empty() {
        println!("  motivos de falha:");
        for (m, n) in &r.motivos {
            println!("    {n} × {m}");
        }
    }
    if !r.lacunas.is_empty() {
        println!("  lacunas (intervalo > 90 s entre sondagens): {}", r.lacunas.len());
        for (seq, d) in r.lacunas.iter().take(10) {
            println!("    antes da sondagem #{seq}: {} s", analise::br(*d, 0));
        }
    }

    println!("Comparação com os limiares (indicativa; a latência de referência do critério vem das sessões de pagamento):");
    println!("  disponibilidade ≥ {} %: sem exclusão {}; com exclusão {}",
        analise::br(a.limiar_disponibilidade_pct, 1),
        atende(r.disponibilidade_sem_exclusao >= a.limiar_disponibilidade_pct),
        atende(r.disponibilidade_com_exclusao >= a.limiar_disponibilidade_pct));
    println!("  taxa de sucesso ≥ {} %: {}", analise::br(a.limiar_sucesso_pct, 1), atende(r.sucesso_pct >= a.limiar_sucesso_pct));
    if let Some(e) = &r.latencia {
        println!("  latência das sondagens: mediana ≤ {} ms: {}; p99 ≤ {} ms: {}",
            analise::br(a.limiar_mediana_ms, 0), atende(e.mediana <= a.limiar_mediana_ms),
            analise::br(a.limiar_p99_ms, 0), atende(e.p99 <= a.limiar_p99_ms));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli { conexao, comando } = Cli::parse();
    match comando {
        Comando::Energia(e) => cmd_energia(e).await,
        Comando::AnalisarSessao(a) => cmd_analisar_sessao(a),
        Comando::AnalisarSondagens(a) => cmd_analisar_sondagens(a),
        outro => {
            let mut clientes = conectar(&conexao).await?;
            match outro {
                Comando::Info => cmd_info(&mut clientes).await,
                Comando::Sondar(s) => cmd_sondar(&mut clientes, s).await,
                Comando::Pagar(s) => cmd_pagar(&mut clientes, s).await,
                _ => unreachable!(),
            }
        }
    }
}
#[cfg(test)]
mod testes {
    use super::*;

    /// Leitura real de `vcgencmd pmic_read_adc` (Raspberry Pi 5 em repouso, 17/10/2026).
    /// Soma conferida à mão: 1,4728 W em 12 trilhos.
    #[test]
    fn potencia_pmic_confere_com_soma_manual() {
        let amostra = include_str!("../tests/amostra_pmic.txt");
        let (w, trilhos) = potencia_pmic(amostra);
        assert_eq!(trilhos, 12);
        assert!((w - 1.4728).abs() < 0.001, "potência calculada: {w}");
    }

    #[test]
    fn percentil_basico() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(percentil(&v, 50.0), 3.0);
        assert_eq!(percentil(&v, 0.0), 1.0);
        assert_eq!(percentil(&v, 100.0), 5.0);
    }
}
