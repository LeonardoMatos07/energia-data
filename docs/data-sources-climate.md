# Fontes de dados: clima (INMET e NASA POWER)

Relatório da Etapa 0 (descoberta), conferido em 30/10/2026. Nenhum código de produção foi escrito.

## Resumo

| Fonte | Decisão | Motivo |
|---|---|---|
| NASA POWER, pelo bucket aberto da AWS (`s3://nasa-power`) | **Entra** (irradiância mensal) | Licença CC BY 4.0 declarada, acesso anônimo, sem chave |
| NASA POWER, pela API REST (`power.larc.nasa.gov/api/`) | **Fica de fora** | O `robots.txt` proíbe `/api/`. Não foi contornado: o canal oficial em massa é o bucket |
| INMET, dados históricos | **Fica de fora, aguarda contato** | A página `portal.inmet.gov.br/dadoshistoricos` não declara licença nem termos de uso (regra: licença antes de código) |
| INPE | Não investigado | Opcional na spec |

## NASA POWER

| Item | Resultado |
|---|---|
| Registro | AWS Registry of Open Data, `https://registry.opendata.aws/nasa-power/` |
| Licença | "Não há restrições de uso, acesso e/ou download. Pedimos que o projeto NASA POWER seja citado." O bucket traz `LICENSE.txt` com **Creative Commons Attribution 4.0** |
| Como citar | "NASA Prediction of Worldwide Energy Resources (POWER) was accessed on DATA from https://registry.opendata.aws/nasa-power" |
| Chave, conta, cadastro | Não precisam. Acesso HTTPS anônimo ao bucket `nasa-power.s3.amazonaws.com` |
| `robots.txt` | `power.larc.nasa.gov` proíbe `/api/` e `/beta/`. Por isso a API REST (com 429 em excesso) **não é usada** |
| Conjuntos | `syn1deg/temporal/` (CERES SYN1deg, grade de 1 grau): `hourly`, `daily`, `monthly`, `annual` e `climatology`, cada um em horário local solar (`lst`) e UTC; também `merra2`, `geosit`, `flashflux`, `imerg`, `srb`, `gwm` |
| Formato | Zarr (`power_syn1deg_monthly_temporal_lst.zarr`) |
| Atualização | `last_available.json` na raiz do bucket. Radiação com atraso de cerca de 5 a 7 dias |
| Natureza do dado | **Estimativa de satélite e reanálise, não medição.** Toda tela deve dizer "estimativa de satélite/reanálise" |

**Não verificado nesta etapa:** os nomes exatos dos parâmetros dentro do Zarr mensal (radiação global horizontal, direta, difusa, temperatura a 2 m, vento) e a resolução espacial real. Isso precisa de leitura do `.zmetadata` e vai determinar se a grade de 1 grau serve para o criador de projeto solar.

**Proposta de grade:** os 27 centroides estaduais mais um ponto por município com usina do Spillway. Com o conjunto mensal (poucos MB por ponto e parâmetro), o volume fica abaixo de 100 MB. O horário (`hourly`) fica fora: é grande demais para ponto a ponto e não é necessário para a média mensal.

## INMET

| Item | Resultado |
|---|---|
| Página | `https://portal.inmet.gov.br/dadoshistoricos` (um `.zip` por ano de 2000 a 2026) |
| `robots.txt` | Não existe (a URL devolve a página do portal) |
| Licença e termos | **Não declarados** na página |
| Decisão | Não baixar nem carregar. A spec manda parar e pedir contato ao órgão |

O que falta, e o que você precisa fazer: pedir ao INMET, pelos canais do portal, a licença (ou permissão de reuso) dos dados históricos das estações automáticas. Quando houver resposta, a Etapa 1 (colunas, unidades de radiação, valores ausentes, fuso e cadastro de estações) é refeita com um zip de exemplo.

## Tabelas previstas (só quando houver fonte liberada)

`irradiance_point` (ponto, latitude, longitude) e `irradiance_monthly` (ponto × mês × GHI, direta, difusa em kWh/m²/dia, com a variante da fonte). `weather_station` e `weather_hourly` só se o INMET liberar.

## IMERG diário (chuva para a previsão de afluência), conferido em 30/10/2026

| Item | Resultado |
|---|---|
| Conjunto | `s3://nasa-power/imerg/temporal/power_imerg_daily_temporal_utc.zarr`, variável `IMERG_PRECTOT` (mm/dia), grade de 0,1 grau, dia em UTC |
| Licença | CC BY 4.0 (`LICENSE.txt` do bucket). Citar POWER e IMERG (doi:10.5067/GPM/IMERGDF/DAY/07). Texto em `energy_pipeline.rain.ATTRIBUTION` |
| Acesso | HTTPS anônimo no bucket; sem conta, sem chave. A API REST do POWER não é usada (robots.txt proíbe `/api/`) |
| Leitura | `energy_pipeline.rain`: lê só os blocos Zarr necessários (blosc + fixedscaleoffset, `numcodecs`), média de janela 3 x 3 pixels ao redor da barragem |
| **Atraso real** | O `last_available.json` diz 2026-10-28, mas o Zarr analisado só tem dados **até 2026-09-14** (bloco modificado em 16/09; mesmo fim em 4 pontos testados). Atraso efetivo de cerca de 46 dias. Os últimos dias de chuva faltam no serviço diário |
| Final x Late | Dias com menos de 120 dias de idade são marcados `late`; cada linha guarda `first_seen_at` e `first_precip_mm` para medir a diferença quando o valor Final chegar |

Consequência para o serviço: sem chuva dos últimos cerca de 46 dias, o modelo diário não pode depender dela como entrada de origem. Decisão pendente do dono (ver relatório do forecast): treinar e operar apenas com as vazões, ou achar uma segunda fonte de menor atraso (GPM IMERG Late no GES DISC exige conta Earthdata).
