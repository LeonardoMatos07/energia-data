# ONS datasets: column dictionary

Summary of the ONS data dictionaries (`DicionarioDados_*.json`, same folder as each dataset in
`https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/`). Only the datasets used so far are
described; the others are added in the phase that loads them. All power values are in **MWmed**
(average MW over the period), which for hourly data equals MWh in that hour.

## File layout quirks

| Dataset | Layout |
|---|---|
| `geracao_usina_2_ho` | Yearly file up to 2021 (`GERACAO_USINA-2_{year}`), monthly from 2022 |
| `geracao_termica_despacho_2_ho` | Yearly up to 2021, monthly after |
| `energia_vertida_turbinavel_ho` | Yearly up to 2024, monthly after (2024 has both: use monthly) |
| any | `.parquet` and `.csv` are usually both published; the pipeline tries the configured format, then the other one, and records the one used in the manifest (`file_format`) |

## `geracao_usina_2_ho` (plant generation, hourly)

| Column | Description | Unit |
|---|---|---|
| `din_instante` | Reference timestamp (Brasilia time) | |
| `id_subsistema`, `nom_subsistema` | Subsystem of the plant | |
| `id_estado`, `nom_estado` | State of the plant | |
| `cod_modalidadeoperacao` | Operation mode | |
| `nom_tipousina` | Plant type (hydro, thermal, wind, solar, nuclear) | |
| `nom_tipocombustivel` | Fuel type | |
| `nom_usina`, `id_ons`, `ceg` | Plant name, ONS id, CEG code | |
| `val_geracao` | Generation | MWmed |

## `balanco_energia_subsistema_ho` (energy balance per subsystem, hourly)

| Column | Description | Unit |
|---|---|---|
| `id_subsistema`, `nom_subsistema`, `din_instante` | Subsystem and timestamp | |
| `val_gerhidraulica` | Hydro generation | MWmed |
| `val_gertermica` | Thermal generation | MWmed |
| `val_gereolica` | Wind generation | MWmed |
| `val_gersolar` | Solar (photovoltaic) generation | MWmed |
| `val_carga` | Load | MWmed |
| `val_intercambio` | Net interchange | MWmed |

There is no nuclear or "other" column here: nuclear only appears in `geracao_usina_2_ho`.

## `geracao_termica_despacho_2_ho` (thermal dispatch by reason)

Keys: `din_instante`, `nom_tipopatamar` (load level), `id_subsistema`, `nom_usina`,
`cod_usinaplanejamento`, `ceg`. Each dispatch reason has a programmed (`val_prog*`) and a verified
(`val_verif*`) column, in MWmed: merit order (`ordemmerito`, `ordemdemeritoref`,
`ordemdemeritoacimadainflex`), inflexibility (`inflexibilidade`, `inflexembutmerito`,
`inflexpura`), electrical reason (`razaoeletrica`), energy-security guarantee
(`garantiaenergetica`), `gfom`, loss replacement (`reposicaoperdas`), export (`exportacao`),
power reserve (`reservapotencia`), `gsub`, `unitcommitment`, `constrainedoff`; plus the totals
`val_proggeracao` and `val_verifgeracao`.

## `energia_vertida_turbinavel_ho` (spilled turbinable energy, hourly)

| Column | Description | Unit |
|---|---|---|
| `id_subsistema`, `nom_subsistema`, `nom_bacia`, `nom_rio`, `nom_agente`, `nom_reservatorio`, `cod_usina` | Location and owner | |
| `din_instante` | Timestamp | |
| `val_geracao`, `val_disponibilidade`, `val_folgadegeracao` | Generation, availability, headroom | MWmed |
| `val_vazaoturbinada`, `val_vazaovertida`, `val_vazaovertidanaoturbinavel`, `val_vazaovertidaturbinavel` | Flows | m3/s |
| `val_produtividade` | Productivity | MW/(m3/s) |
| `val_energiavertida`, `val_energiavertidaturbinavel` | Spilled energy | MWmed |

## `intercambio_nacional_ho` (interchange between subsystems, hourly)

| Column | Description | Unit |
|---|---|---|
| `din_instante` | Timestamp | |
| `id_subsistema_origem`, `id_subsistema_destino` (+ `nom_*`) | Link, in the direction of the flow of that hour | |
| `val_intercambiomwmed` | Verified exchange (sum of the active power flows on the lines) | MWmed |
| `val_intercambioprogmwmed` | Scheduled exchange | MWmed |

The file has four links (N-NE, N-SE, NE-SE, S-SE), one row per link and hour, always with a
magnitude (>= 0) and origin/destination swapped when the flow reverses. The silver layer fixes each
link in alphabetical order and signs the flow: positive means origin to destination.

## `intercambio_internacional_ho` (interchange with neighbouring countries, hourly)

`din_instante`, `nom_paisdestino` (Argentina, Uruguai), `val_intercambiomwmed` and
`val_intercambioprogmwmed` (MWmed). The value is the flow to the destination country (negative is
an import); the origin is `BR`.

## Thermal dispatch: notes on the real file

The file has one row per plant and hour. Programmed columns are `val_prog*` and verified ones
`val_verif*` (the verified electrical reason column is spelled `val_verifrazaoeletrica`). Some
plants have no `cod_usinaplanejamento` yet; the pipeline then uses `CEG:<ceg>`. The reasons
overlap: merit order contains its above-inflexibility part, inflexibility has two parts
(embedded in merit and pure). `val_proginflexibilidadedessem` and `val_progordemdemeritoref` are
model references, not reasons, and are not loaded.

## Still to read (next phases)

Reservoirs, EAR and ENA (subsystem and reservoir),
daily hydrological data, transmission lines, substations and transformation capacity.
