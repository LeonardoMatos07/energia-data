# Fontes de dados: CCEE (preço de energia, PLD)

Relatório da Etapa 0 (descoberta), conferido em 30/10/2026. Nenhum código de produção foi escrito.

## Acesso e termos

| Item | Resultado |
|---|---|
| Portal | `https://dadosabertos.ccee.org.br/` (CKAN) |
| `robots.txt` | Proíbe `/api/`, `/revision/`, `/dataset/rate/` e `/dataset/*/history`. **`Crawl-Delay: 10`**. As páginas `/dataset/<nome>` são permitidas |
| API | Existe (CKAN), mas está em `/api/`, **proibida pelo robots**. Não será usada |
| Como baixar | Cada recurso tem um link direto `https://pda-download.ccee.org.br/<id>/content`, listado na página do conjunto. É o caminho oficial. O `pda-download` não tem `robots.txt` (responde 403) |
| Licença | **Creative Commons Attribution 4.0 (CC BY 4.0)**, declarada na página de cada conjunto, com "Termo Legal" ligando ao aceite da licença e aos Termos de Uso. Atribuir à CCEE |
| Cadastro, chave ou login | Não são necessários para baixar |
| Coleta | 1 requisição a cada 10 s (o `Crawl-Delay`), `User-Agent` identificado, `Range` para ver só o cabeçalho |

## Conjuntos

| Conjunto | O que é | Arquivos | Atualização | Decisão |
|---|---|---|---|---|
| `pld_horario` | **PLD oficial horário** por submercado | `pld_horario_2021` a `pld_horario_2026` (CSV por ano) e `pld_historico_semanal_2001_2020` | Mensal (referência: todos os dias). Última atualização vista: 29/10/2026 | **Entra** |
| `pld_media_diaria` | Média diária do PLD por submercado | `pld_media_diaria_2021` e seguintes | Mensal | Entra (conferência) |
| `pld_media_semanal` | Média semanal do PLD por submercado | `pld_media_semanal_2001_2020` e anos seguintes | Mensal (quinta-feira) | **Entra** (único histórico de 2001 a 2020) |
| `pld_final_historico` | PLD final semanal, histórico antigo | 1 arquivo | Não se aplica | Fora (coberto pela média semanal) |
| `pld_sombra` | PLD "sombra" horário, com tipo de metodologia | `pld_sombra_2022`, `_2023`, `_2024` | "Periódica" (última atualização 20/11/2025) | Entra como variante `shadow`, só 2022 a 2024 |
| `fator_alavancagem`, `url_fator_alavancagem`, `reserva_penalidade`, `desligamento_descumprimento` | Dados **por agente** (empresas) | | | **Não entram.** Identificam agentes; precisa de decisão do usuário |

Outros conjuntos vistos no catálogo e não analisados nesta rodada: `rd_atend_prod_contab_*`, `rd_encargos_contab_*`, `energia_incentivada_gf_fonte`.

## Colunas (lidas dos cabeçalhos reais)

- `pld_horario_AAAA.csv`: `MES_REFERENCIA` (AAAAMM), `SUBMERCADO` (NORDESTE, NORTE, SUDESTE, SUL), `PERIODO_COMERCIALIZACAO` (hora corrida do mês), `DIA`, `HORA` (0 a 23), `PLD_HORA` (R$/MWh, ponto decimal). O arquivo `pld_historico_semanal_2001_2020` tem as mesmas colunas, com o valor semanal repetido em cada hora.
- `pld_sombra_AAAA.csv`: as mesmas, mais `TIPO_SOMBRA` (por exemplo `NEWAVE_HIBRIDO`) e `PLD_SOMBRA` no lugar de `PLD_HORA`.
- `pld_media_semanal_*.csv`: `MES_REFERENCIA`, `SUBMERCADO`, `SEMANA` (início, `dd/mm/aaaa`), `PLD_MEDIA_SEMANA`.
- `pld_media_diaria_*.csv`: `MES_REFERENCIA`, `SUBMERCADO`, `DIA` (`dd/mm/aaaa`), `PLD_MEDIA_DIA`.

Irregularidades a tratar no pipeline:

- A **codificação muda** entre anos (`ISO-8859-2` em alguns, `UTF-8` em outros).
- As **aspas variam** (2021 com aspas, 2025 sem).
- `DIA` e `HORA` ora têm zero à esquerda, ora não.
- O arquivo de 2025 começava por `202512`: **não assumir a ordem** das linhas.
- `MES_REFERENCIA` mais `DIA` e `HORA` formam o instante, em horário de Brasília.

## PLD semanal, PLD horário oficial e PLD sombra

- **PLD horário (oficial):** preço de liquidação por hora e submercado, usado na contabilização desde 2021.
- **PLD semanal:** o preço oficial calculado por semana até 2020. Depois, a média semanal é a média das horas.
- **PLD sombra:** cálculo paralelo, com outra metodologia (`TIPO_SOMBRA`), publicado para comparação. **Não** é o preço de liquidação. A página diz "dados como estão" e que a disponibilidade pode ser interrompida.

Isso vem das páginas do portal. A documentação metodológica completa da CCEE não foi lida nesta etapa.

## Pisos e tetos regulatórios

Não foram inventados. O repositório já tem `src/energy_pipeline/seeds/pld_limits.csv` (fonte: ANEEL, REH 3.167/2022 e atualizações). O gate de preço usa essa tabela e **alerta**, sem bloquear, quando o valor sai do intervalo.

## Tamanho e frequência

Cada CSV anual tem cerca de 1 MB, então o backfill completo fica abaixo de 20 MB. Uma DAG diária basta.

## Atribuição

"Fonte: CCEE, Preço de Liquidação das Diferenças (PLD), licença CC BY 4.0, acessado em DATA."

## Perguntas ao usuário

- Os conjuntos por agente (fator de alavancagem, penalidades) ficaram de fora. Publicá-los por empresa exige decisão sua.
