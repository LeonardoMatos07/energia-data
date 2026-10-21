from __future__ import annotations

import polars as pl
import pytest


def raw_frame(header: str, *lines: str) -> pl.DataFrame:
    columns = header.split(";")
    rows = [line.split(";") for line in lines]
    return pl.DataFrame(rows, schema=dict.fromkeys(columns, pl.String), orient="row")


@pytest.fixture
def raw_load_curve() -> pl.DataFrame:
    return raw_frame(
        "id_subsistema;nom_subsistema;din_instante;val_cargaenergiahomwmed",
        "N;NORTE;2024-01-01 00:00:00;6539.56000000",
        "SE;SUDESTE/CENTRO-OESTE;2024-01-01 00:00:00;38211.10000000",
        "SE;SUDESTE/CENTRO-OESTE;2024-01-01 00:00:00;38300.00000000",
        " s ;  SUL ;2024-01-01 01:00:00;9966.972",
        "NE;NORDESTE;2024-01-01 02:00:00;",
        "NE;NORDESTE;not a date;11976.884",
        "N;NORTE;2024-02-01 00:00:00;6400.0",
    )


@pytest.fixture
def raw_plant_generation() -> pl.DataFrame:
    header = (
        "din_instante;id_subsistema;nom_subsistema;id_estado;nom_estado;"
        "cod_modalidadeoperacao;nom_tipousina;nom_tipocombustivel;nom_usina;id_ons;ceg;val_geracao"
    )
    return raw_frame(
        header,
        "2024-01-01 00:00:00;N;NORTE;AM;AMAZONAS;TIPO I;HIDROELÉTRICA;Hidráulica;Balbina;AMBA;"
        "UHE.PH.AM.000190-2.01;86.93800000",
        "2024-01-01 00:00:00;N;NORTE;AC;ACRE;Pequenas Usinas (MMGD);FOTOVOLTAICA;Fotovoltaica;"
        "PQU ACRAC FOTOV MMGD;PQU_ACRAC_GD;-;0E-8",
        "2024-01-01 00:00:00;N;NORTE;AM;AMAZONAS;Pequenas Usinas (MMGD);FOTOVOLTAICA;Fotovoltaica;"
        "PQU MMAM MMGD;;-;12.5",
        "2024-01-01 01:00:00;N;NORTE;AM;AMAZONAS;TIPO I;HIDROELÉTRICA;Hidráulica;Balbina;AMBA;"
        "UHE.PH.AM.000190-2.01;",
        "2024-01-01 00:00:00;NE;NORDESTE;PE;PERNAMBUCO;TIPO I;TÉRMICA;Biomassa;Usina Cana;PECANA;"
        "UTE.AI.PE.000001-1.01;15.0",
        "2024-01-01 00:00:00;SE;SUDESTE;RJ;RIO DE JANEIRO;TIPO I;TÉRMICA;Gás;Usina Gás;RJGAS;"
        "UTE.GN.RJ.000002-2.01;300.0",
        "2024-01-01 00:00:00;;;RJ;RIO DE JANEIRO;TIPO I;TÉRMICA;Gás;Usina X;RJX;-;1.0",
    )


@pytest.fixture
def raw_generation_capacity() -> pl.DataFrame:
    header = (
        "id_subsistema;nom_subsistema;id_estado;nom_estado;nom_modalidadeoperacao;"
        "nom_agenteproprietario;nom_agenteoperador;nom_tipousina;nom_usina;ceg;"
        "nom_unidadegeradora;cod_equipamento;num_unidadegeradora;nom_combustivel;"
        "dat_entradateste;dat_entradaoperacao;dat_desativacao;val_potenciaefetiva"
    )
    return raw_frame(
        header,
        "NE;NORDESTE       ;AL;ALAGOAS;TIPO I;AXIA NORDESTE;AXIA NORDESTE;HIDROELÉTRICA;XINGÓ;"
        "UHE.PH.SE.027053-9.01;UG  527 MW USINA XINGO               5 AL;ALUXG-0UG5          ;"
        "5     ;HIDRÁULICA;1995-01-31;1995-01-31;;527.0",
        "NE;NORDESTE       ;AL;ALAGOAS;TIPO I;AXIA NORDESTE;AXIA NORDESTE;HIDROELÉTRICA;XINGÓ;"
        "UHE.PH.SE.027053-9.01;UG  527 MW USINA XINGO               2 AL;ALUXG-0UG2          ;"
        "2     ;HIDRÁULICA;1996-12-20;1996-12-20;2020-05-01;527.0",
        "S;SUL;RS;RIO GRANDE DO SUL;TIPO I;AGENTE;AGENTE;EOLIELÉTRICA;PARQUE EOLICO;"
        "EOL.CV.RS.000003-3.01;UG 1;;1;EÓLICA;2015-01-01;2015-02-01;;2.1",
    )
