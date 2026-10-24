//! Análise dos CSV gerados pelo coletor: estatísticas por sessão, energia por pagamento e
//! disponibilidade das sondagens. Todas as regras seguem a metodologia (seções 3.3.3 e 3.3.5).

use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

const NS_POR_S: u64 = 1_000_000_000;

/// Linha dos CSV de sondagens e de pagamentos (mesmo formato).
#[derive(Debug, Deserialize, Clone)]
pub struct LinhaCsv {
    pub sessao: String,
    pub modo: String,
    pub seq: u64,
    pub inicio_unix_ns: u64,
    pub fim_unix_ns: u64,
    #[allow(dead_code)]
    pub valor_sat: i64,
    pub status: String,
    pub falha: String,
    pub latencia_ms: f64,
    pub taxa_msat: i64,
    #[serde(default)]
    pub erro_rpc: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmostraEnergia {
    pub unix_ns: u64,
    pub potencia_w: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LinhaSaude {
    pub unix_ns: u64,
    #[serde(default)]
    pub lnd_estado: String,
    #[serde(default)]
    pub lnd_sincronizado: String,
    #[serde(default)]
    pub canais_ativos: String,
    #[serde(default)]
    pub erro: String,
}

pub fn ler_csv<T: for<'de> Deserialize<'de>>(caminho: &Path) -> Result<Vec<T>> {
    let mut leitor = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(caminho)
        .with_context(|| format!("abrindo {}", caminho.display()))?;
    let mut saida = Vec::new();
    for r in leitor.deserialize() {
        saida.push(r.with_context(|| format!("lendo uma linha de {}", caminho.display()))?);
    }
    Ok(saida)
}

// ---------------------------------------------------------------- estatísticas

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Estatisticas {
    pub media: f64,
    pub mediana: f64,
    pub p95: f64,
    pub p99: f64,
    pub desvio: f64,
}

/// Percentil por interpolação linear sobre valores já ordenados.
pub fn percentil(ordenado: &[f64], p: f64) -> f64 {
    if ordenado.is_empty() {
        return f64::NAN;
    }
    let pos = (p / 100.0) * (ordenado.len() as f64 - 1.0);
    let (i, f) = (pos.floor() as usize, pos - pos.floor());
    match ordenado.get(i + 1) {
        Some(prox) => ordenado[i] * (1.0 - f) + prox * f,
        None => ordenado[i],
    }
}

/// Média, mediana, percentis 95 e 99 e desvio-padrão amostral (n-1).
pub fn estatisticas(valores: &[f64]) -> Option<Estatisticas> {
    if valores.is_empty() {
        return None;
    }
    let mut v = valores.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len() as f64;
    let media = v.iter().sum::<f64>() / n;
    let desvio = if v.len() > 1 {
        (v.iter().map(|x| (x - media).powi(2)).sum::<f64>() / (n - 1.0)).sqrt()
    } else {
        0.0
    };
    Some(Estatisticas {
        media,
        mediana: percentil(&v, 50.0),
        p95: percentil(&v, 95.0),
        p99: percentil(&v, 99.0),
        desvio,
    })
}

/// Vazão em janelas fixas de `janela_s` segundos a partir de `t0_ns`: conta os pagamentos
/// concluídos com sucesso em cada janela completa e devolve a média (pagamentos/s) e o número
/// de janelas completas. A última janela, se incompleta, é descartada.
pub fn vazao_por_janelas(fins_ok_ns: &[u64], t0_ns: u64, t_fim_ns: u64, janela_s: u64) -> (Option<f64>, usize) {
    let largura = janela_s * NS_POR_S;
    let janelas = ((t_fim_ns.saturating_sub(t0_ns)) / largura) as usize;
    if janelas == 0 {
        return (None, 0);
    }
    let limite = t0_ns + janelas as u64 * largura;
    let dentro = fins_ok_ns.iter().filter(|&&t| t >= t0_ns && t < limite).count();
    (Some(dentro as f64 / (janelas as f64 * janela_s as f64)), janelas)
}

/// Potência média no intervalo [de, ate) e número de amostras usadas.
pub fn potencia_media(amostras: &[AmostraEnergia], de_ns: u64, ate_ns: u64) -> Option<(f64, usize)> {
    let dentro: Vec<f64> = amostras
        .iter()
        .filter(|a| a.unix_ns >= de_ns && a.unix_ns < ate_ns)
        .map(|a| a.potencia_w)
        .collect();
    if dentro.is_empty() {
        None
    } else {
        Some((dentro.iter().sum::<f64>() / dentro.len() as f64, dentro.len()))
    }
}

// ---------------------------------------------------------------- sessão

#[derive(Debug)]
pub struct ResumoSessao {
    pub sessao: String,
    pub modo: String,
    pub total: usize,
    pub ok: usize,
    pub sucesso_pct: f64,
    pub duracao_s: f64,
    pub vazao_janelas: Option<f64>,
    pub janelas: usize,
    pub vazao_total: f64,
    pub latencia: Option<Estatisticas>,
    pub taxa_total_msat: i64,
    pub taxa_media_msat: f64,
    pub p_repouso_w: Option<f64>,
    pub p_sessao_w: Option<f64>,
    pub amostras_repouso: usize,
    pub amostras_sessao: usize,
    /// Energia adicional por pagamento bem-sucedido, em milijoules.
    pub energia_extra_mj: Option<f64>,
}

pub fn resumir_sessao(
    linhas: &[LinhaCsv],
    sessao: &str,
    energia: Option<&[AmostraEnergia]>,
    repouso_s: u64,
) -> Option<ResumoSessao> {
    let l: Vec<&LinhaCsv> = linhas.iter().filter(|x| x.sessao == sessao).collect();
    if l.is_empty() {
        return None;
    }
    let t0 = l.iter().map(|x| x.inicio_unix_ns).min().unwrap();
    let t1 = l.iter().map(|x| x.fim_unix_ns).max().unwrap();
    let duracao_s = (t1 - t0) as f64 / NS_POR_S as f64;
    let ok: Vec<&&LinhaCsv> = l.iter().filter(|x| x.status == "SUCCEEDED").collect();
    let fins: Vec<u64> = ok.iter().map(|x| x.fim_unix_ns).collect();
    let (vazao_janelas, janelas) = vazao_por_janelas(&fins, t0, t1, 60);
    let lat: Vec<f64> = ok.iter().map(|x| x.latencia_ms).collect();
    let taxa_total: i64 = ok.iter().map(|x| x.taxa_msat).sum();

    let (mut p_rep, mut p_ses, mut n_rep, mut n_ses, mut extra) = (None, None, 0, 0, None);
    if let Some(e) = energia {
        if let Some((p, n)) = potencia_media(e, t0.saturating_sub(repouso_s * NS_POR_S), t0) {
            p_rep = Some(p);
            n_rep = n;
        }
        if let Some((p, n)) = potencia_media(e, t0, t1) {
            p_ses = Some(p);
            n_ses = n;
        }
        if let (Some(r), Some(s)) = (p_rep, p_ses) {
            if !ok.is_empty() {
                extra = Some((s - r) * duracao_s / ok.len() as f64 * 1000.0);
            }
        }
    }

    Some(ResumoSessao {
        sessao: sessao.to_string(),
        modo: l[0].modo.clone(),
        total: l.len(),
        ok: ok.len(),
        sucesso_pct: 100.0 * ok.len() as f64 / l.len() as f64,
        duracao_s,
        vazao_janelas,
        janelas,
        vazao_total: ok.len() as f64 / duracao_s.max(f64::EPSILON),
        latencia: estatisticas(&lat),
        taxa_total_msat: taxa_total,
        taxa_media_msat: if ok.is_empty() { 0.0 } else { taxa_total as f64 / ok.len() as f64 },
        p_repouso_w: p_rep,
        p_sessao_w: p_ses,
        amostras_repouso: n_rep,
        amostras_sessao: n_ses,
        energia_extra_mj: extra,
    })
}

// ---------------------------------------------------------------- sondagens

#[derive(Debug)]
pub struct ResumoSondagens {
    pub total: usize,
    pub ok: usize,
    pub falhas: usize,
    /// Sondagens esperadas e não registradas (só quando a janela já terminou): contam como falha.
    pub faltantes: usize,
    pub falhas_atribuiveis: usize,
    pub falhas_restantes: usize,
    pub disponibilidade_sem_exclusao: f64,
    pub disponibilidade_com_exclusao: f64,
    pub sucesso_pct: f64,
    pub latencia: Option<Estatisticas>,
    pub motivos: BTreeMap<String, usize>,
    /// (seq, intervalo em segundos) quando o intervalo entre duas sondagens passa de 90 s.
    pub lacunas: Vec<(u64, f64)>,
}

/// Uma amostra de saúde é "ruim" se o LND do nó auxiliar não estava ativo, sincronizado e com canal.
pub fn saude_ruim(s: &LinhaSaude) -> bool {
    s.lnd_estado != "SERVER_ACTIVE"
        || s.lnd_sincronizado != "true"
        || s.canais_ativos.is_empty()
        || s.canais_ativos == "0"
        || !s.erro.is_empty()
}

/// Falha atribuível ao nó auxiliar: a amostra de saúde mais próxima (até 90 s) estava ruim, ou
/// não há amostra de saúde nesse intervalo (o próprio verificador estava fora do ar).
pub fn falha_atribuivel(inicio_ns: u64, saude: &[LinhaSaude]) -> bool {
    const JANELA: u64 = 90 * NS_POR_S;
    let mais_proxima = saude
        .iter()
        .min_by_key(|s| s.unix_ns.abs_diff(inicio_ns));
    match mais_proxima {
        Some(s) if s.unix_ns.abs_diff(inicio_ns) <= JANELA => saude_ruim(s),
        _ => true,
    }
}

pub fn resumir_sondagens(
    linhas: &[LinhaCsv],
    saude: Option<&[LinhaSaude]>,
    esperado_janela_encerrada: Option<usize>,
) -> ResumoSondagens {
    let total = linhas.len();
    let faltantes = esperado_janela_encerrada.map(|e| e.saturating_sub(total)).unwrap_or(0);
    let total_ef = total + faltantes;
    let ok = linhas.iter().filter(|x| x.status == "SUCCEEDED").count();
    let falhas_v: Vec<&LinhaCsv> = linhas.iter().filter(|x| x.status != "SUCCEEDED").collect();
    let mut motivos: BTreeMap<String, usize> = BTreeMap::new();
    for f in &falhas_v {
        let m = if !f.falha.is_empty() && f.falha != "FAILURE_REASON_NONE" {
            f.falha.clone()
        } else if !f.erro_rpc.is_empty() {
            format!("{}: {}", f.status, f.erro_rpc)
        } else {
            f.status.clone()
        };
        *motivos.entry(m).or_insert(0) += 1;
    }
    let atrib = match saude {
        Some(s) => falhas_v.iter().filter(|f| falha_atribuivel(f.inicio_unix_ns, s)).count(),
        None => 0,
    };
    let falhas = falhas_v.len();
    let restantes = falhas - atrib;
    let lat: Vec<f64> = linhas.iter().filter(|x| x.status == "SUCCEEDED").map(|x| x.latencia_ms).collect();

    let mut ordenadas: Vec<&LinhaCsv> = linhas.iter().collect();
    ordenadas.sort_by_key(|x| x.inicio_unix_ns);
    let lacunas = ordenadas
        .windows(2)
        .filter_map(|w| {
            let d = (w[1].inicio_unix_ns - w[0].inicio_unix_ns) as f64 / NS_POR_S as f64;
            (d > 90.0).then_some((w[1].seq, d))
        })
        .collect();

    ResumoSondagens {
        total,
        ok,
        falhas,
        faltantes,
        falhas_atribuiveis: atrib,
        falhas_restantes: restantes,
        disponibilidade_sem_exclusao: if total_ef > 0 { 100.0 * ok as f64 / total_ef as f64 } else { f64::NAN },
        // Excluídas as falhas atribuíveis, também saem do denominador (análogo ao índice do BCB).
        disponibilidade_com_exclusao: if total_ef > atrib {
            100.0 * ok as f64 / (total_ef - atrib) as f64
        } else {
            f64::NAN
        },
        sucesso_pct: if total_ef > 0 { 100.0 * ok as f64 / total_ef as f64 } else { f64::NAN },
        latencia: estatisticas(&lat),
        motivos,
        lacunas,
    }
}

/// Texto do relatório com vírgula decimal, para copiar ao trabalho.
pub fn br(x: f64, casas: usize) -> String {
    format!("{:.*}", casas, x).replace('.', ",")
}

pub fn ler_energia(caminho: &Path) -> Result<Vec<AmostraEnergia>> {
    ler_csv(caminho)
}

#[cfg(test)]
mod testes {
    use super::*;

    fn linha(sessao: &str, seq: u64, ini_s: u64, fim_s: u64, status: &str, lat: f64) -> LinhaCsv {
        LinhaCsv {
            sessao: sessao.into(),
            modo: "sincrono".into(),
            seq,
            inicio_unix_ns: ini_s * NS_POR_S,
            fim_unix_ns: fim_s * NS_POR_S,
            valor_sat: 100,
            status: status.into(),
            falha: if status == "SUCCEEDED" { "FAILURE_REASON_NONE".into() } else { "FAILURE_REASON_NO_ROUTE".into() },
            latencia_ms: lat,
            taxa_msat: 0,
            erro_rpc: String::new(),
        }
    }

    #[test]
    fn estatisticas_conhecidas() {
        let e = estatisticas(&[1.0, 2.0, 3.0, 4.0, 5.0]).unwrap();
        assert_eq!(e.media, 3.0);
        assert_eq!(e.mediana, 3.0);
        assert!((e.desvio - 1.5811388).abs() < 1e-6); // amostral
        assert!((e.p95 - 4.8).abs() < 1e-9);
    }

    #[test]
    fn vazao_em_janelas_descarta_a_incompleta() {
        // 150 s de sessão = 2 janelas completas; 120 conclusões nas 2 janelas, 30 na parte incompleta.
        let t0 = 1_000 * NS_POR_S;
        let mut fins: Vec<u64> = (0..120).map(|i| t0 + i * NS_POR_S).collect();
        fins.extend((0..30).map(|i| t0 + (120 + i) * NS_POR_S));
        let (v, n) = vazao_por_janelas(&fins, t0, t0 + 150 * NS_POR_S, 60);
        assert_eq!(n, 2);
        assert!((v.unwrap() - 1.0).abs() < 1e-9); // 120 pagamentos / 120 s
        assert_eq!(vazao_por_janelas(&fins, t0, t0 + 30 * NS_POR_S, 60), (None, 0));
    }

    #[test]
    fn energia_extra_por_pagamento() {
        // Repouso: 1,5 W nos 300 s antes. Sessão de 100 s a 2,0 W com 50 pagamentos bem-sucedidos.
        let t0 = 10_000u64;
        let mut energia = Vec::new();
        for s in (t0 - 300)..t0 {
            energia.push(AmostraEnergia { unix_ns: s * NS_POR_S, potencia_w: 1.5 });
        }
        for s in t0..(t0 + 100) {
            energia.push(AmostraEnergia { unix_ns: s * NS_POR_S, potencia_w: 2.0 });
        }
        let linhas: Vec<LinhaCsv> = (0..50).map(|i| linha("S", i, t0 + i * 2, t0 + i * 2 + 1, "SUCCEEDED", 1000.0)).collect();
        let mut linhas = linhas;
        linhas.push(linha("S", 50, t0 + 99, t0 + 100, "SUCCEEDED", 900.0)); // fecha em t0+100
        let r = resumir_sessao(&linhas, "S", Some(&energia), 300).unwrap();
        assert_eq!(r.ok, 51);
        assert!((r.p_repouso_w.unwrap() - 1.5).abs() < 1e-9);
        // (2,0 - 1,5) W * 100 s / 51 pagamentos = 0,98039 J = 980,39 mJ
        assert!((r.energia_extra_mj.unwrap() - 980.392).abs() < 0.01, "{:?}", r.energia_extra_mj);
    }

    #[test]
    fn atribuicao_de_falhas_pela_saude() {
        let boa = |s: u64| LinhaSaude { unix_ns: s * NS_POR_S, lnd_estado: "SERVER_ACTIVE".into(), lnd_sincronizado: "true".into(), canais_ativos: "1".into(), erro: String::new() };
        let ruim = |s: u64| LinhaSaude { unix_ns: s * NS_POR_S, lnd_estado: "LOCKED".into(), lnd_sincronizado: String::new(), canais_ativos: String::new(), erro: "lnd_sem_resposta".into() };
        let saude = vec![boa(60), ruim(120), boa(180)];
        assert!(!falha_atribuivel(62 * NS_POR_S, &saude)); // saúde boa por perto
        assert!(falha_atribuivel(121 * NS_POR_S, &saude)); // saúde ruim por perto
        assert!(falha_atribuivel(900 * NS_POR_S, &saude)); // sem amostra: verificador fora do ar
    }

    #[test]
    fn sondagens_com_e_sem_exclusao_e_lacunas() {
        let mut l: Vec<LinhaCsv> = (0..98).map(|i| linha("sondagem", i, 60 * (i + 1), 60 * (i + 1), "SUCCEEDED", 600.0)).collect();
        l.push(linha("sondagem", 98, 60 * 99, 60 * 99, "FAILED", 5.0));
        l.push(linha("sondagem", 99, 60 * 100 + 200, 60 * 100 + 200, "FAILED", 5.0)); // 200 s depois: lacuna
        let boa = |s: u64| LinhaSaude { unix_ns: s * NS_POR_S, lnd_estado: "SERVER_ACTIVE".into(), lnd_sincronizado: "true".into(), canais_ativos: "1".into(), erro: String::new() };
        let ruim = |s: u64| LinhaSaude { unix_ns: s * NS_POR_S, lnd_estado: "LOCKED".into(), ..boa(s) };
        let saude = vec![boa(60 * 99), ruim(60 * 100 + 200)];
        let r = resumir_sondagens(&l, Some(&saude), None);
        assert_eq!((r.total, r.ok, r.falhas), (100, 98, 2));
        assert_eq!(r.falhas_atribuiveis, 1);
        assert!((r.disponibilidade_sem_exclusao - 98.0).abs() < 1e-9);
        assert!((r.disponibilidade_com_exclusao - 98.0 / 99.0 * 100.0).abs() < 1e-9);
        assert_eq!(r.lacunas.len(), 1);
        assert_eq!(r.lacunas[0].0, 99);
    }

    #[test]
    fn sondagens_nao_registradas_contam_como_falha_quando_a_janela_encerrou() {
        // 98 sucessos e 2 falhas registradas, de 110 esperadas: 10 faltantes contam como falha.
        let mut l: Vec<LinhaCsv> = (0..98).map(|i| linha("sondagem", i, 60 * (i + 1), 60 * (i + 1), "SUCCEEDED", 600.0)).collect();
        l.push(linha("sondagem", 98, 60 * 99, 60 * 99, "FAILED", 5.0));
        l.push(linha("sondagem", 99, 60 * 100, 60 * 100, "FAILED", 5.0));
        let parcial = resumir_sondagens(&l, None, None);
        assert_eq!(parcial.faltantes, 0);
        assert!((parcial.disponibilidade_sem_exclusao - 98.0).abs() < 1e-9);
        let encerrada = resumir_sondagens(&l, None, Some(110));
        assert_eq!(encerrada.faltantes, 10);
        assert!((encerrada.disponibilidade_sem_exclusao - 100.0 * 98.0 / 110.0).abs() < 1e-9);
    }
}
