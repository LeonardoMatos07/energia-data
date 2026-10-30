# Fontes de dados: EPE (Balanço Energético Nacional e Anuário)

Relatório da Etapa 0 (descoberta), conferido em 30/10/2026. Nenhum código de produção foi escrito.

| Item | Resultado |
|---|---|
| Site | `https://www.epe.gov.br/` |
| Licença | "Todo o conteúdo deste site está publicado sob a licença **Creative Commons Atribuição 4.0 Internacional (CC BY 4.0)**", declarada na página do BEN |
| `robots.txt` | Proíbe `/_layouts/`, `/_vti_bin/`, `/_catalogs/`, `/*.aspx`, áreas de acesso restrito e alguns caminhos institucionais. Os arquivos em `/sites-pt/publicacoes-dados-abertos/...` não estão proibidos |
| Cadastro, chave, login | Não precisam |
| Forma de citar | Atribuição à EPE (CC BY 4.0), com o ano-base |

## BEN 2025 (ano-base 2024)

Página: `.../publicacoes-dados-abertos/publicacoes/balanco-energetico-nacional-2025`. Planilhas com link direto:

| Arquivo | Conteúdo | Decisão |
|---|---|---|
| `Anexo IX - Balanços Consolidados (em tep) 1970 a 2024.xlsx` | Balanço energético consolidado por fonte, de 1970 a 2024, em tep | **Entra** |
| `Anexo X - Matriz aberta e Comercial 1970 a 2024.xlsx` | Matriz energética (oferta por fonte) | **Entra** |
| `Matriz ab2024.xlsx` | Matriz aberta do ano-base | Fora (repete o anexo X) |
| PDFs (Síntese e Relatório Final) | Texto e tabelas em PDF | Fora (a spec não permite extrair tabelas de PDF) |

**Não verificado nesta etapa:** o layout das abas e das colunas das duas planilhas (leitura exige abrir o `.xlsx`), as unidades por aba e o mapeamento dos nomes de fonte da EPE para os da ONS. Isso é o primeiro passo da Etapa 1.

## Anuário Estatístico de Energia Elétrica

Não foi localizado nesta rodada o endereço das planilhas do Anuário (consumo por classe e UF). **Fica pendente**: precisa de nova busca no site da EPE antes de qualquer código.

## Tabelas previstas

`energy_matrix_annual` (ano × fonte × oferta em tep e em %), carregada uma vez por ano. `electricity_consumption_annual` (ano × UF × classe) só depois de localizar o Anuário.
