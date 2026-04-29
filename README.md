# Extrator de Dados do Setor Elétrico Nacional

Projeto de coleta e estruturação de dados públicos do setor elétrico brasileiro, desenvolvido em parceria entre **LICA**, **ENACOM**, **PPGMCS** e **Unimontes**.

## Sobre o Projeto

O script consome APIs e portais de dados abertos da **CCEE** e do **ONS** para extrair, consolidar e salvar em CSV os principais indicadores do mercado de energia elétrico nacional — com foco em precificação, armazenamento hídrico e restrições operativas de fontes renováveis.

Os dados coletados são base para análises de:
- Formação e previsão do PLD (Preço de Liquidação das Diferenças)
- Curtailment de geração eólica e solar
- Correlação entre energia armazenada (EAR) e custo marginal de operação (CMO)
- Carga verificada por subsistema

## Fontes de Dados

| Fonte | Portal | Dados |
|---|---|---|
| CCEE | [dadosabertos.ccee.org.br](https://dadosabertos.ccee.org.br) | PLD horário, PLD sombra |
| ONS | [dados.ons.org.br](https://dados.ons.org.br) | EAR diário, constrained-off eólico e solar |
| ONS API | [apicarga.ons.org.br](https://apicarga.ons.org.br) | Carga verificada por subsistema |
| ONS S3 | AWS S3 (`ons-aws-prod-opendata`) | Arquivos Parquet/XLSX mensais |

## Dados Extraídos

| Arquivo gerado | Descrição | Granularidade |
|---|---|---|
| `pld_horario.csv` | Preço de Liquidação das Diferenças por submercado | Horária |
| `pld_sombra.csv` | PLD sombra por submercado | Horária |
| `carga_verificada.csv` | Carga de energia verificada por subsistema | Sub-horária |
| `ear_diario.csv` | Energia Armazenada nos Reservatórios por subsistema | Diária |
| `constrained_off_eolico.csv` | Restrições de corte em usinas eólicas (Tipo I, II-B, II-C) | Por usina/hora |
| `constrained_off_solar.csv` | Restrições de corte em usinas fotovoltaicas | Por usina/hora |
| `ccee_datasets.csv` | Catálogo completo de datasets da CCEE | — |
| `ons_datasets.csv` | Catálogo completo de datasets do ONS | — |

## Instalação

```bash
git clone https://github.com/JhonatanMotaDev/energy_data_extractor.git
cd energy_data_extractor
pip install -r requirements.txt
```

## Uso

```bash
python energy_data_extractor.py
```

Os arquivos CSV serão salvos na pasta `output/`.

## Dependências

```
requests
pandas
openpyxl
pyarrow
```

## Contexto

Em 2025, os cortes na geração eólica e solar chegaram a **20,6%** de toda a capacidade integrada ao SIN — mais que o dobro de 2024 — com 54% motivados por sobreoferta e 33% por confiabilidade do sistema. Este projeto fornece a infraestrutura de dados necessária para modelar e analisar esse fenômeno.

## Parceria

| Instituição | Papel |
|---|---|
| **LICA** | Laboratório de Inteligência Computacional Aplicada |
| **ENACOM** | Empresa Nacional de Comercialização de Energia |
| **PPGMCS** | Programa de Pós-Graduação em Modelagem Computacional e Sistemas |
| **Unimontes** | Universidade Estadual de Montes Claros |

## Licença

Dados públicos conforme termos de uso da CCEE e do ONS.
