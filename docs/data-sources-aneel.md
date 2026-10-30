# Fontes de dados: ANEEL (qualidade, tarifas, bandeiras, geração distribuída, empreendimentos)

Relatório da Etapa 0 (descoberta), conferido em 30/10/2026. Nenhum código de produção foi escrito.

## Acesso e termos

| Item | Resultado |
|---|---|
| Portal | `https://dadosabertos.aneel.gov.br/` (CKAN 2.12) |
| `robots.txt` | Hoje permite as páginas de conjuntos e de recursos. Proíbe `/api/`, `/revision/`, `/dataset/rate/` e `/dataset/*/history`. **`Crawl-Delay: 10`**. O bloqueio visto na consulta anterior não aparece no `robots.txt` atual |
| API | CKAN em `/api/`, **proibida pelo robots**. Não será usada |
| Caminho oficial | As páginas `/dataset/<slug>` listam os arquivos e os links diretos `.../resource/<id>/download/<arquivo>` (CSV, ZIP, Parquet, XML) |
| Licença | **Open Data Commons Open Database License (ODbL)**, declarada na página de cada conjunto |
| Cadastro, chave, login | Não são necessários |
| Coleta | 1 requisição a cada 10 s, `User-Agent` identificado |

**Atenção sobre a ODbL.** Exige atribuição e, se o portal distribuir uma *base de dados derivada*, ela deve usar a mesma licença. Exibir gráficos e números calculados com atribuição é uso permitido. Oferecer um download em massa das tabelas pode acionar a cláusula. Vale conferir com um advogado antes de oferecer downloads.

## Conjuntos

| Tema | Slug | Arquivos | Atualização | Tamanho | Decisão |
|---|---|---|---|---|---|
| **Qualidade (DEC e FEC)** | `indicadores-coletivos-de-continuidade-dec-e-fec` | ZIP/Parquet por década (2000-2009, 2010-2019, 2020-2029), limites, atributos, domínio | Mensal (arquivo de 05/10/2026) | ZIP 2020-2029: 72 MB (CSV de 527 MB) | **Entra** |
| Tarifas | `tarifas-distribuidoras-energia-eletrica` | CSV, XML e Parquet | Semanal | CSV 179 MB | Só referência |
| Bandeiras | `bandeiras-tarifarias` | CSVs de acionamento, adicional e conta bandeira | Mensal | 6 KB | Só referência |
| Geração distribuída | `relacao-de-empreendimentos-de-geracao-distribuida`, `atendimento-mmgd-mini-e-micro-geracao-distribuida` | ZIP/Parquet e CSVs técnicos por fonte | Diária | ZIP 112 MB | Só referência |
| Empreendimentos | `empreendimentos-em-operacao`, `siga-sistema-de-informacoes-de-geracao-da-aneel`, `agentes-de-geracao-de-energia-eletrica` | CSV histórico | Trimestral | | Só referência |

Colunas já vistas nos cabeçalhos:

- Tarifas: `DatGeracaoConjuntoDados`, `DscREH`, `SigAgente`, `NumCNPJDistribuidora`, `DatInicioVigencia`, `DatFimVigencia`, `DscBaseTarifaria`, `DscSubGrupo`, `DscModalidadeTarifaria`, `DscClasse`, `DscSubClasse`, `DscDetalhe`, `NomPostoTarifario`, `DscUnidadeTerciaria`, `SigAgenteAcessante`, `VlrTUSD`, `VlrTE`.
- Bandeiras (acionamento): `DatGeracaoConjuntoDados`, `DatCompetencia`, `NomBandeiraAcionada`, `VlrAdicionalBandeira`.
- Geração distribuída e empreendimentos: colunas não inspecionadas. A relação de GD pode ter identificadores de unidades consumidoras, que **nunca** devem ir para o banco; só agregados.

## Qualidade: o que foi lido

Arquivo `indicadores-continuidade-coletivos-2020-2029.csv` (separador `;`, aspas, decimal com vírgula, como em `",08"`):

| Coluna | Descrição |
|---|---|
| `DatGeracaoConjuntoDados` | Data de geração do arquivo |
| `SigAgente` | Sigla da distribuidora (com espaços à direita) |
| `NumCNPJ` | CNPJ da distribuidora (empresa regulada) |
| `IdeConjUndConsumidoras`, `DscConjUndConsumidoras` | Conjunto de unidades consumidoras (código e nome) |
| `SigIndicador` | Tipo de indicador (domínio em `dominio-indicadores.csv`) |
| `AnoIndice` | Ano |
| `NumPeriodoIndice` | Mês (1 a 12). O dicionário diz "período em meses"; os dados mostram o mês do ano |
| `VlrIndiceEnviado` | Valor |

Indicadores a usar: `DEC` e `FEC` (3 minutos), as parcelas por origem (`DECIND`, `DECINE`, `DECIP`, `DECXN`, `DECXP` e os equivalentes `FEC*`) e `NumCon` (consumidores do conjunto). DEC em horas e centésimos; FEC em número de interrupções.

Limites em `indicadores-continuidade-coletivos-limite.csv` (26 MB): `SigAgente`, `NumCNPJ`, `IdeConjUndConsumidoras`, `DscConjUndConsumidoras`, `SigIndicador` (`DEC` ou `FEC`), `AnoLimiteQualidade`, `VlrLimite` (anual).

Perfil de 2020-2029: anos 2020 a 2026, 105 distribuidoras, 3.980 conjuntos.

**Dado pessoal:** não há. Os registros são de concessionárias reguladas e de conjuntos elétricos. A página `/qualidade` mostra por distribuidora, que é o objetivo da spec.

## Recomendação

| Conjunto | Decisão |
|---|---|
| DEC e FEC | **Entra**: licença ODbL declarada, acesso oficial por arquivo, 72 MB |
| Tarifas, bandeiras, GD, empreendimentos | Fora desta rodada. O caminho de acesso é o mesmo, os links diretos das páginas dos conjuntos. Tarifas e GD têm arquivos de 100 a 180 MB; na GD só agregados |
