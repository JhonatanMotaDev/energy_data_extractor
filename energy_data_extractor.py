# -*- coding: utf-8 -*-
"""
Extrator de dados do mercado de energia eletrico brasileiro.
Fontes: CCEE (PLD horario) e ONS (carga verificada, EAR, constrained-off eolico)
"""

import requests
import pandas as pd
import io
import sys
from datetime import date, timedelta
from pathlib import Path

# Garante saida UTF-8 no Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

TODAY = date.today()
START_DATE = (TODAY - timedelta(days=7)).strftime("%Y-%m-%d")
END_DATE = TODAY.strftime("%Y-%m-%d")

SUBSISTEMAS_ONS = ["SE", "S", "NE", "N", "SIN"]

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
}

# ─── CCEE ─────────────────────────────────────────────────────────────────────

def _ccee_session() -> requests.Session:
    """Sessao com cookies para contornar bloqueio 403 da CCEE."""
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    try:
        s.get("https://dadosabertos.ccee.org.br/", timeout=15)
    except Exception:
        pass
    return s


def list_ccee_datasets() -> list:
    s = _ccee_session()
    try:
        r = s.get(
            "https://dadosabertos.ccee.org.br/api/3/action/package_list",
            timeout=30,
        )
        r.raise_for_status()
        datasets = r.json()["result"]
        print(f"[CCEE] {len(datasets)} datasets disponiveis")
        return datasets
    except Exception as e:
        print(f"[CCEE] Listagem falhou - {e}")
        return []


def _ccee_fetch_package_csv(package_id: str, ano: int) -> pd.DataFrame:
    """Baixa CSV de um dataset CCEE filtrando pelo ano."""
    s = _ccee_session()
    try:
        r = s.get(
            "https://dadosabertos.ccee.org.br/api/3/action/package_show",
            params={"id": package_id},
            timeout=30,
        )
        r.raise_for_status()
        resources = r.json()["result"]["resources"]
        for res in resources:
            name = res.get("name", "") + res.get("url", "")
            if str(ano) in name:
                csv_r = s.get(res["url"], timeout=60)
                csv_r.raise_for_status()
                df = pd.read_csv(io.StringIO(csv_r.text), sep=";", decimal=",")
                return df
    except Exception as e:
        print(f"[CCEE] {package_id} {ano}: falhou - {e}")
    return pd.DataFrame()


def fetch_pld_horario(ano: int = TODAY.year) -> pd.DataFrame:
    """PLD horario por submercado via CCEE (IDs reais: pld_horario e pld_horario_submercado)."""
    for pkg_id in ("pld_horario_submercado", "pld_horario"):
        df = _ccee_fetch_package_csv(pkg_id, ano)
        if not df.empty:
            print(f"[CCEE] PLD horario {ano}: {len(df)} registros via {pkg_id}")
            return df
    return pd.DataFrame()


def fetch_pld_sombra(ano: int = TODAY.year) -> pd.DataFrame:
    """PLD sombra horario via CCEE."""
    df = _ccee_fetch_package_csv("pld_sombra", ano)
    if not df.empty:
        print(f"[CCEE] PLD sombra {ano}: {len(df)} registros")
    return df


# ─── ONS ──────────────────────────────────────────────────────────────────────

def list_ons_datasets() -> list:
    try:
        r = requests.get(
            "https://dados.ons.org.br/api/3/action/package_list",
            timeout=30,
        )
        r.raise_for_status()
        datasets = r.json()["result"]
        print(f"[ONS] {len(datasets)} datasets disponiveis")
        return datasets
    except Exception as e:
        print(f"[ONS] Listagem falhou - {e}")
        return []


def fetch_carga_verificada(start: str = START_DATE, end: str = END_DATE) -> pd.DataFrame:
    """Carga verificada por subsistema via API ONS."""
    frames = []
    for cod in SUBSISTEMAS_ONS:
        try:
            r = requests.get(
                "https://apicarga.ons.org.br/prd/cargaverificada",
                params={"dat_inicio": start, "dat_fim": end, "cod_areacarga": cod},
                timeout=30,
            )
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            df["subsistema"] = cod
            frames.append(df)
        except Exception as e:
            print(f"[ONS] Carga verificada {cod}: falhou - {e}")
    if frames:
        result = pd.concat(frames, ignore_index=True)
        print(f"[ONS] Carga verificada: {len(result)} registros ({start} ate {end})")
        return result
    return pd.DataFrame()


def _ons_fetch_latest(package_id: str) -> pd.DataFrame:
    """Baixa o PARQUET mais recente de um dataset ONS (fallback: XLSX, CSV)."""
    try:
        r = requests.get(
            "https://dados.ons.org.br/api/3/action/package_show",
            params={"id": package_id},
            timeout=30,
        )
        r.raise_for_status()
        resources = r.json()["result"]["resources"]
        # Prefere PARQUET > XLSX > CSV, pega o mais recente de cada
        for fmt in ("PARQUET", "XLSX", "CSV"):
            urls = [x["url"] for x in resources if x.get("format", "").upper() == fmt]
            if not urls:
                continue
            file_r = requests.get(urls[-1], timeout=120)
            file_r.raise_for_status()
            if fmt == "PARQUET":
                return pd.read_parquet(io.BytesIO(file_r.content))
            if fmt == "XLSX":
                return pd.read_excel(io.BytesIO(file_r.content))
            # CSV: detecta separador automaticamente
            sample = file_r.text[:2000]
            sep = "," if sample.count(",") > sample.count(";") else ";"
            return pd.read_csv(io.StringIO(file_r.text), sep=sep, decimal=",")
    except Exception as e:
        print(f"[ONS] {package_id}: falhou - {e}")
    return pd.DataFrame()


def fetch_ear_diario() -> pd.DataFrame:
    """Energia Armazenada por Subsistema (EAR) diaria via ONS."""
    df = _ons_fetch_latest("ear-diario-por-subsistema")
    if not df.empty:
        print(f"[ONS] EAR diario por subsistema: {len(df)} registros")
    return df


def fetch_constrained_off_eolico() -> pd.DataFrame:
    """Restricoes constrained-off eolico via ONS."""
    df = _ons_fetch_latest("restricao_coff_eolica_usi")
    if not df.empty:
        print(f"[ONS] Constrained-off eolico: {len(df)} registros")
    return df


def fetch_constrained_off_solar() -> pd.DataFrame:
    """Restricoes constrained-off fotovoltaico via ONS."""
    df = _ons_fetch_latest("restricao_coff_fotovoltaica")
    if not df.empty:
        print(f"[ONS] Constrained-off solar: {len(df)} registros")
    return df


# ─── UTIL ─────────────────────────────────────────────────────────────────────

def save(df: pd.DataFrame, name: str):
    if df.empty:
        print(f"  >> {name}: sem dados")
        return
    path = OUTPUT_DIR / f"{name}.csv"
    df.to_csv(path, index=False, sep=";", decimal=",")
    print(f"  >> Salvo: {path} ({len(df)} linhas x {len(df.columns)} colunas)")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("EXTRATOR DE DADOS - MERCADO DE ENERGIA BRASIL")
    print(f"Periodo: {START_DATE} ate {END_DATE}")
    print("=" * 60)

    print("\n[1/7] Catalogo CCEE")
    ccee_ds = list_ccee_datasets()
    save(pd.DataFrame({"dataset": ccee_ds}), "ccee_datasets")

    print("\n[2/7] Catalogo ONS")
    ons_ds = list_ons_datasets()
    save(pd.DataFrame({"dataset": ons_ds}), "ons_datasets")

    print("\n[3/7] PLD Horario (CCEE)")
    pld = fetch_pld_horario(TODAY.year)
    save(pld, "pld_horario")

    print("\n[4/7] PLD Sombra (CCEE)")
    pld_sombra = fetch_pld_sombra(TODAY.year)
    save(pld_sombra, "pld_sombra")

    print("\n[5/7] Carga Verificada (ONS)")
    carga = fetch_carga_verificada()
    save(carga, "carga_verificada")

    print("\n[6/7] EAR Diario (ONS)")
    ear = fetch_ear_diario()
    save(ear, "ear_diario")

    print("\n[7/8] Constrained-off Eolico (ONS)")
    coff = fetch_constrained_off_eolico()
    save(coff, "constrained_off_eolico")

    print("\n[8/8] Constrained-off Solar (ONS)")
    coff_sol = fetch_constrained_off_solar()
    save(coff_sol, "constrained_off_solar")

    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    for nome, df in [
        ("pld_horario", pld),
        ("pld_sombra", pld_sombra),
        ("carga_verificada", carga),
        ("ear_diario", ear),
        ("constrained_off_eolico", coff),
        ("constrained_off_solar", coff_sol),
    ]:
        status = f"{len(df):>6} registros" if not df.empty else "  SEM DADOS"
        print(f"  {nome:<30} {status}")

    print(f"\nArquivos em: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
