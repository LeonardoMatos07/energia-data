# energia-data

![CI](../../actions/workflows/ci.yml/badge.svg)

> **Repositório privado.** Pipelines de dados da plataforma de inteligência em energia
> (nome do produto a definir). Parte da Fase 1: monitor público de curtailment.

O `energia-data` ingere os dados abertos do ONS, padroniza e modela o curtailment de
usinas eólicas e solares, calcula os agregados que o portal lê e os grava no S3 e no
Postgres. **Ele só escreve**: o schema do banco pertence ao `energia-api` (Alembic), e o
contrato entre os repositórios está em
[`energia-api/docs/contrato-fase1.md`](../energia-api/docs/contrato-fase1.md).

| Repositório | Papel |
|---|---|
| **`energia-data`** | Airflow: ingestão do ONS, bronze → prata → ouro, carga no Postgres |
| `energia-api` | FastAPI: dono do schema, leitura pública, newsletter, admin |
| `energia-web` | Next.js: portal público com ISR |
| `energia-infra` | Terraform: AWS `sa-east-1` |

---

## Fluxo

```mermaid
flowchart LR
    ONS[("ONS<br/>bucket S3 público")]
    subgraph Airflow["Airflow 3 · ons_curtailment_pipeline"]
        E[extract] --> T[transform] --> V[validate] --> R[record]
        R --> P[publish] --> N[notify_portal]
    end
    subgraph Lake["Data lake (S3)"]
        B[(bronze)]
        PR[(prata)]
        O[(ouro)]
    end
    subgraph PG["Postgres (schema do energia-api)"]
        T1[(usina<br/>curtailment_mensal_*)]
        T2[(carga_execucao)]
    end
    ONS --> E --> B --> T --> PR --> P --> O
    P --> T1
    R --> T2
    N -->|POST /v1/admin/revalidar| API[energia-api] --> WEB[energia-web]
```

| Etapa | Grão | O que faz | Escreve |
|---|---|---|---|
| `extract` | fonte × mês | Baixa o arquivo do ONS, a menos que o ETag mostre que ele não mudou | `bronze/` (com sha256, URL e ETag nos metadados) |
| `transform` | fonte × mês | Lê tudo como texto, converte cada coluna explicitamente, deduplica, separa linhas inválidas e modela o curtailment usando as saídas das dependências; guarda horário de Brasília e UTC | `prata/`, `quarantine/` |
| `validate` | fonte × mês | Quality gates; checagens ERROR barram a carga | relatório no XCom |
| `record` | fonte × mês | Registra a carga | `carga_execucao` |
| `publish` | mês | Monta os agregados do portal e carrega o Postgres de forma idempotente (apaga e reinsere o mês numa transação) | `ouro/`, `usina`, `curtailment_mensal_*` |
| `notify_portal` | execução | Pede ao `energia-api` para revalidar o portal (token de máquina do Cognito) | — |

O pipeline roda no dia 3 de cada mês e **reprocessa os últimos 3 meses**
(`REPROCESS_MONTHS`), porque o ONS revisa dados já publicados. Arquivos com o mesmo
ETag não são baixados de novo. Uma falha em qualquer tarefa também é registrada em
`carga_execucao`, e aparece na área admin.

## Fontes

| Fonte | Dataset do ONS | Grão | Desde |
|---|---|---|---|
| `marginal_cost` | `cmo_tm` | CMO por subsistema, semi-horário (R$/MWh) | 2020 |
| `generation_capacity` | `capacidade-geracao` | Capacidade por unidade geradora (foto atual) | — |
| `wind_cluster_curtailment` | `restricao_coff_eolica_tm` | Conjunto, semi-horário: GNRa oficial, minutos por razão, ponto de conexão | out/2021 |
| `wind_plant_curtailment` | `restricao_coff_eolica_detail_tm` | Usina, semi-horário: geração estimada × verificada | out/2021 |
| `solar_cluster_curtailment` | `restricao_coff_fotovoltaica_tm` | Igual à eólica | **abr/2024** |
| `solar_plant_curtailment` | `restricao_coff_fotovoltaica_detail_tm` | Igual à eólica | **abr/2024** |

Há também o pipeline `ons_grid_pipeline` (carga, geração por usina e capacidade), que
por enquanto só alimenta o lake; ele é a base das fases seguintes (localização de BESS).

## Metodologia do curtailment

O ONS publica o corte em dois níveis, e nenhum sozinho responde "quanto *esta usina*
perdeu, e por quê":

- **conjunto** (ou usina despachada individualmente): a *Geração Não Realizada Apurada*
  (GNRa) oficial e quantos minutos de cada meia hora foram restritos por cada razão;
- **usina**: geração estimada e verificada e uma marca de meia hora restrita, mas sem o
  corte oficial e sem a razão.

O modelo combina os dois:

1. **O corte oficial manda.** A GNRa do conjunto é a verdade (MW médio × 0,5 h = MWh).
2. **Divisão por razão**, proporcional aos minutos restritos: `REL` (indisponibilidade
   externa, transmissão), `CNF` (confiabilidade), `ENE` (razão energética: oferta maior do
   que a rede absorve) e `outros`. Sem minutos, o código de razão informado leva tudo.
3. **Rateio para as usinas**, proporcional à perda estimada de cada uma
   (estimada − verificada nas meias horas restritas); na falta dela, pela geração estimada;
   e, por fim, em partes iguais. A soma das usinas **bate com o ONS por construção**, e a
   validação confere isso (tolerância de 1%).
4. **Preço**: cada meia hora cortada é valorada pelo CMO do subsistema, limitado ao piso e
   ao teto do PLD do ano quando configurados (`seeds/pld_limits.csv`).

Por que ratear em vez de usar a estimativa por usina direto: ela superestima o corte
oficial, e não por um fator constante (1,32× a GNRa em jan/2023, 1,10× em jan/2025,
1,02× em set/2026).

### O que os dados brutos mostram (e como é tratado)

| Achado | Tratamento |
|---|---|
| Dados de solar só existem a partir de abr/2024 (eólica desde out/2021) | Cada fonte declara `available_from`; meses anteriores são pulados, não falham |
| Usinas despachadas individualmente ("Tipo I") não têm conjunto nos arquivos de usina, mas aparecem com o próprio id nos de conjunto | Usina sem conjunto é o próprio conjunto |
| Usinas que entram num conjunto no meio do mês aparecem duas vezes por meia hora | Fica a linha com o conjunto |
| O CMO é a programação do DESSEM: o arquivo já traz o dia seguinte | Só meses completos são processados |
| Faltam dias inteiros de CMO em 19 de 69 meses (jan/2023 tem 21 de 31) | Preenchidos com a mesma meia hora do dia anterior, marcados `*_FILLED`; o gate falha abaixo de 60% de cobertura |
| O CMO é **zero** em quase todas as meias horas de mar–jun/2023, e o corte se concentra nas horas de CMO baixo (solar ao meio-dia) | A perda em R$ só faz sentido com o piso do PLD configurado (ver premissas) |
| O ONS chega a publicar até 56 minutos de restrição numa meia hora (set/2023) | Aviso; os minutos só são usados como proporção entre razões, a GNRa não é afetada |
| `dsc_restricao` só existe a partir de set/2025 | Texto opcional |
| O schema é estável em todo o histórico (conferido de 2021 a 2026) | Os parsers ainda leem tudo como texto e falham alto se algo mudar |

## Quality gates

| Checagem | Severidade |
|---|---|
| Não vazio, chaves sem nulos, chave natural única, datas dentro do mês | ERROR |
| Valores aceitos (subsistema em `N, NE, S, SE`) | ERROR |
| Limites físicos (geração por usina, carga por subsistema) | ERROR / WARN |
| Cobertura do CMO ≥ 60% (WARN listando os dias faltantes) | ERROR / WARN |
| Linhas rejeitadas ≤ 1% | ERROR |
| **Reconciliação com o ONS**: rateio por usina a até 1% da GNRa oficial | ERROR |
| Energia cortada sem preço ≤ 1% | WARN |

## Premissas em aberto

- **Limites do PLD.** `src/energy_pipeline/seeds/pld_limits.csv` está vazio de propósito:
  preencha uma linha por ano a partir das resoluções da ANEEL. Até lá a perda usa o CMO
  puro, que é quase zero justamente quando a maior parte do corte acontece.
- **O PLD é a referência certa?** Geradores com contratos de longo prazo perdem o preço do
  contrato, não o spot.
- **Ressarcimento de REL.** O ONS envia à CCEE a geração de referência final das meias
  horas `REL` para fins de ressarcimento (ESS); o portal deveria separar perda
  ressarcível de não ressarcível. Confirmar as regras vigentes com alguém do setor.
- **Dados da CCEE** (PLD oficial) bloqueiam acesso automatizado (HTTP 403); por isso o CMO
  do ONS.

---

## Como rodar

### Com Docker (Airflow)

Os repositórios ficam lado a lado (`../energia-api` é usado pelo serviço `migrate`).

```bash
cp .env.example .env        # opcional
docker compose up -d --build
make backfill FROM=2023-01 TO=2026-09
```

| Serviço | Endereço | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Lake (navegador de arquivos) | http://localhost:8888/buckets/energy-lake/ | |
| Lake (API S3) | http://localhost:8333 | `lakeadmin` / `lakesecret` |
| Postgres | `localhost:5433`, banco `warehouse` | `warehouse` / `warehouse` |

### Sem Airflow (CLI)

As etapas não dependem do orquestrador, então tudo roda como CLI contra qualquer
Postgres e endpoint S3. É assim que backfills e depuração são feitos localmente.

```bash
uv sync --extra local
# o schema precisa existir: em ../energia-api, rode `uv run alembic upgrade head`
export PYTHONPATH=src LAKE_S3_ENDPOINT_URL=http://localhost:8333
export AWS_ACCESS_KEY_ID=lakeadmin AWS_SECRET_ACCESS_KEY=lakesecret AWS_DEFAULT_REGION=us-east-1
export WAREHOUSE_DSN=postgresql://warehouse:warehouse@localhost:5433/warehouse
export API_URL=http://localhost:8000          # opcional: revalida o portal no fim

uv run python -m energy_pipeline curtailment                     # janela de reprocessamento
uv run python -m energy_pipeline curtailment --from 2023-01 --to 2026-09
uv run python -m energy_pipeline grid --month 2026-09
```

### Configuração

| Variável | Padrão | Significado |
|---|---|---|
| `REPROCESS_MONTHS` | `3` | Meses reprocessados a cada execução |
| `RECONCILIATION_TOLERANCE` | `0.01` | Desvio máximo entre rateio e GNRa oficial |
| `MAX_REJECTED_RATIO` | `0.01` | Fração máxima de linhas em quarentena |
| `PIPELINE_START_DATE` | `2025-01-01` | Primeiro mês do pipeline de rede |
| `LAKE_S3_ENDPOINT_URL` | SeaweedFS no Docker | Vazio = S3 da AWS |
| `LAKE_BUCKET` | `energy-lake` | |
| `WAREHOUSE_DSN` | Postgres do Docker | |
| `API_URL` | — | `energia-api` para revalidar o portal (vazio = não avisa) |
| `COGNITO_TOKEN_URL`, `DATA_CLIENT_ID`, `DATA_CLIENT_SECRET` | — | Token de máquina do Cognito (produção) |

### Qualidade do código

```bash
make check      # ruff check, mypy, pytest e ruff format --check
```

Os testes cobrem leitura, deduplicação, divisão por razão, rateio, reconciliação,
preço e preenchimento de lacunas, os agregados do portal e a revalidação, usando linhas
reais dos arquivos do ONS como fixtures. O CI também constrói a imagem do Airflow e
confere que as DAGs importam. No merge na `main`, a imagem vai para o ECR e é
implantada na EC2 do Airflow via SSM (`deploy.sh`).

---

## Estrutura

```
.
├── dags/
│   ├── pipeline_factory.py           # Um grupo de tarefas por fonte, mapeado por mês
│   ├── ons_curtailment_pipeline.py   # Fase 1
│   └── ons_grid_pipeline.py
├── src/energy_pipeline/
│   ├── sources.py                    # Datasets do ONS, disponibilidade, dependências
│   ├── extract.py                    # ETag e download com checksum
│   ├── transform.py                  # Leitura e padronização (Polars)
│   ├── curtailment.py                # Divisão por razão, rateio, preço
│   ├── publish.py                    # Agregados do portal (tabelas do contrato)
│   ├── quality.py                    # Quality gates por fonte
│   ├── warehouse.py                  # Carga idempotente via COPY, registro de cargas
│   ├── storage.py                    # Acesso ao lake (boto3)
│   ├── notify.py                     # Revalidação do portal
│   ├── steps.py                      # extract / transform / validate / record / publish
│   ├── __main__.py                   # CLI
│   └── seeds/pld_limits.csv          # Piso e teto do PLD por ano (a preencher)
├── tests/
├── Dockerfile                        # Imagem do Airflow + dependências do uv.lock
├── docker-compose.yml                # Local: Airflow, Postgres, SeaweedFS, migrações
├── docker-compose.prod.yml           # EC2: imagem do ECR, S3 e RDS
└── deploy.sh                         # Chamado pelo CI via SSM
```

## Licença dos dados

Dados do ONS (Operador Nacional do Sistema Elétrico), sob os
[termos de uso de dados abertos](https://dados.ons.org.br).
