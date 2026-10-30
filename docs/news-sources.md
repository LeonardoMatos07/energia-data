# Fontes do módulo de notícias: relatório de descoberta (Etapa 0)

Verificado em 30/10/2026, com poucas requisições (no máximo 1 por segundo por domínio) e
`User-Agent: SpillwayPortal-research (contact: leonardomatos.bcet@gmail.com)`. Nenhuma conta foi
criada, nenhum termo aceito e nenhuma credencial guardada. Só foram lidos títulos, datas, links e
resumos; nenhum corpo de matéria nem imagem foi copiado.

## Resumo

| Fonte | Como coletar | Licença declarada | `robots.txt` | Recomendação |
|---|---|---|---|---|
| `aneel` | **API JSON do portal** (`++api++` + `@search`), não o RSS | CC BY-ND 3.0 | permite | **Entrar** (decisão sobre o método, abaixo) |
| `mme` | Página de listagem (HTML) | CC BY-ND 3.0 | permite | **Entrar** |
| `govbr-energia` | Feed da categoria existe, mas **não traz notícias** | CC BY-ND 3.0 | permite | **Ficar fora** (redundante e inútil como feed) |
| `anp` | Página de listagem (HTML) | CC BY-ND 3.0 | permite | **Entrar** (candidata natural; confirmar com você) |
| `epe` | Página de listagem (HTML, SharePoint) | **CC BY 4.0** (diferente da spec) | permite | **Perguntar** |
| Agência Gov | RSS existe | **"Todos os direitos reservados"** | restringe `*?` | **Ficar fora** |
| `dou` | Nenhum meio aberto confirmado | atos oficiais (art. 8º, IV) | portal: `Disallow: /` | **Perguntar** (INLABS exige cadastro) |

## Achados que mudam o plano da spec

1. **O RSS por pasta (`.../noticias/RSS`) não funciona** em ANEEL nem em MME. No ANEEL devolve a
   página-casca do portal (HTTP 200, sem feed); no MME devolve 404. O feed de categoria do portal
   (`https://www.gov.br/pt-br/noticias/energia-minerais-e-combustiveis/RSS`) existe, mas é uma
   listagem de **pastas por ano** (2018 a 2023, `dc:type` = Folder), não de notícias. O nome exato da
   categoria de energia é `energia-minerais-e-combustiveis`.
2. **ANEEL usa Plone 6 (Volto) e tem API JSON.** `GET https://www.gov.br/aneel/++api++/pt-br/@search?portal_type=News Item&sort_on=effective&sort_order=descending&b_size=N`
   (cabeçalho `Accept: application/json`) devolve `title`, `description`, `effective`, `modified`,
   `@id` e `@type`. É a mesma API que o próprio front do portal usa. Não é "a página de listagem"
   que a spec cita, por isso **peço sua decisão** (pergunta 1).
3. **MME e ANP usam Plone clássico** (sem `++api++`). A listagem em HTML traz, por item, categoria,
   título com link, data `dd/mm/aaaa` e uma linha de descrição.
4. **Sem `ETag` nem `Last-Modified`.** Nem a API da ANEEL nem a listagem do MME devolvem esses
   cabeçalhos (só `Cache-Control`), e `HEAD` no MME responde 403. A regra "usar ETag e
   If-Modified-Since" **não se aplica**: o controle de mudança será pelo `content_hash` e pela
   regra de uma coleta por hora.
5. **Fuso:** a API da ANEEL entrega `effective` em ISO 8601 com `+00:00` (UTC). MME, ANP e EPE
   entregam só a data local, sem hora. Para esses, `published_at` será meia-noite de
   `America/Sao_Paulo` convertida para UTC, e a tela mostra só a data (sem inventar hora).
6. **DOU:** o `robots.txt` do portal `www.in.gov.br` é `User-agent: * / Disallow: /`. Raspar o
   portal está fora de questão. O INLABS (`inlabs.in.gov.br`) exige cadastro e, na hora do teste,
   exibia uma página de "Manutenção Programada".

## Por fonte

### ANEEL (`aneel`)

- **Feed que funciona:** a API acima (não há RSS). Total de notícias no portal: 2.334.
- **Exemplo (como publicado):**
  - título: "ANEEL promove encontro com analistas de investimentos"
  - descrição: "Executivos de instituições financeiras debateram aspectos do setor elétrico"
  - `effective`: 2026-10-29T21:28:12+00:00 · `modified`: 2026-10-29T21:59:20+00:00
  - url: https://www.gov.br/aneel/pt-br/assuntos/noticias/2026/aneel-promove-encontro-com-analistas-de-investimentos
- **Campos:** título, descrição (a linha fina), data de publicação, data de modificação (serve
  para `source_updated_at`), URL, tipo.
- **Frequência:** 60 itens em 57 dias (cerca de 1 por dia).
- **robots.txt** (`https://www.gov.br/robots.txt`): não há `Disallow` para `/aneel`, nem para
  `++api++`. As regras gerais só barram padrões como `*sendto_form$`; as de `++theme++` e
  `++resource++` valem só para `/ebserh` e `/mre`.
- **Licença:** CC BY-ND 3.0, "Todo o conteúdo deste site está publicado sob a licença Creative
  Commons Atribuição-SemDerivações 3.0 Não Adaptada", em
  `https://creativecommons.org/licenses/by-nd/3.0/deed.pt_BR`. Evidência: rodapé de
  https://www.gov.br/aneel/pt-br/assuntos/noticias.

### MME (`mme`)

- **Feed:** `/RSS` devolve 404. Usar a listagem HTML
  `https://www.gov.br/mme/pt-br/assuntos/noticias` (30 itens na página).
- **Exemplo (como publicado):**
  - categoria: "Energia, Minerais e Combustíveis"
  - título: "PDE 2036 prevê R$ 142 bilhões em investimentos para biocombustíveis"
  - data: 29/10/2026
  - url: https://www.gov.br/mme/pt-br/assuntos/noticias/outubro-26/pde-2036-preve-r-142-bilhoes-em-investimentos-para-biocombustiveis
- **Campos:** título, link, data (sem hora), descrição, categoria. Há também itens sem `/outubro-26/`
  na URL, então não dá para depender do padrão do caminho.
- **Frequência:** vários itens por semana (não medida com precisão; a listagem não tem paginação
  testada).
- **robots.txt:** sem restrição para `/mme`.
- **Licença:** CC BY-ND 3.0 (mesmo texto do rodapé). Evidência: https://www.gov.br/mme/pt-br.

### Portal gov.br, categoria de energia (`govbr-energia`)

- `https://www.gov.br/pt-br/noticias/energia-minerais-e-combustiveis/RSS` responde 200 mas só lista
  pastas de ano. Não é fonte de notícias. O próprio `/pt-br/noticias/RSS` também lista pastas e
  coleções. **Recomendação: não incluir.** As notícias de energia do portal já chegam pelos
  órgãos (ANEEL, MME, ANP).

### ANP (`anp`)

- Listagem: `https://www.gov.br/anp/pt-br/canais_atendimento/imprensa/noticias-comunicados`
  (33 links na página, vários de 29/10/2026). `/RSS` devolve 404.
- Plone clássico, mesma estrutura de data/descrição do MME.
- **Licença:** CC BY-ND 3.0 no rodapé de https://www.gov.br/anp/pt-br. `robots.txt`: sem restrição.
- A ANP trata de petróleo, gás e biocombustíveis; só parte do conteúdo é do setor elétrico, e a
  classificação por tema vai separá-lo. **Entrar, se você concordar** (pergunta 3).

### EPE (`epe`)

- Listagem HTML: `https://www.epe.gov.br/pt/imprensa/noticias` (SharePoint; sem feed encontrado).
- **Exemplo:** "EPE debate desenvolvimento da energia eólica na Brazil Windpower 2026", 27/10/2026,
  https://www.epe.gov.br/pt/imprensa/noticias/epe-debate-desenvolvimento-da-energia-eolica-na-brazil-windpower-2026
  (data e resumo vêm na própria listagem).
- **robots.txt** (`https://www.epe.gov.br/robots.txt`): barra `/*.aspx`, `/_layouts/` e áreas
  restritas; `/pt/imprensa/noticias` está liberado.
- **Licença: CC BY 4.0** ("Todo o conteúdo deste site está publicado sob a licença Creative Commons
  Atribuição 4.0 Internacional", rodapé de https://www.epe.gov.br/pt/imprensa/noticias,
  `https://creativecommons.org/licenses/by/4.0/deed.pt_BR`). É **diferente** da CC BY-ND da spec:
  permite adaptações, mas com atribuição. Pela regra da spec, **peço sua decisão** (pergunta 2).
  Entrando, o texto continua sendo exibido sem alteração e com o bloco de atribuição.

### Agência Gov (EBC)

- Tem feed (`https://agenciagov.ebc.com.br/RSS`, também `rss.xml`), mas o rodapé diz "Todos os
  direitos reservados" e não declara Creative Commons. **Fica fora.** Se você quiser, é um
  contato à parte com a EBC.

### Diário Oficial da União (`dou`)

- `https://www.in.gov.br/robots.txt`: `User-agent: *` / `Disallow: /`. **Não raspar o portal.**
- **INLABS** (Imprensa Nacional): meio oficial de XML diário das seções 1, 2 e 3. Exige
  **cadastro e credencial** (login e senha). Na verificação, `https://inlabs.in.gov.br/` exibia
  "Manutenção Programada". Não criei conta.
- **O que é preciso:** você se cadastrar no INLABS, e a credencial entrar nos segredos do ambiente
  (por exemplo no Secrets Manager e nas variáveis do Airflow). Confirmar também se os termos do
  INLABS permitem republicar os atos.
- Alternativa não verificada: conjuntos de dados abertos do DOU em dados.gov.br. Posso checar se
  você preferir evitar o cadastro.

## Perguntas para você (a spec manda perguntar)

1. **Método da ANEEL:** posso usar a API JSON do portal (`++api++/@search`, mesmos campos do RSS,
   sem baixar corpo de matéria), em vez de raspar a listagem em HTML? Recomendo que sim, é mais
   estável.
2. **EPE (CC BY 4.0):** entra, ainda que a licença seja diferente da CC BY-ND?
3. **ANP:** entra como fonte, com filtro por tema para ficar só o que é do setor elétrico?
4. **DOU:** você faz o cadastro no INLABS, ou prefiro investigar os dados abertos do DOU?

## Deixa de valer da spec

- `ETag`/`If-Modified-Since`: sem suporte nas fontes testadas (controle por `content_hash`).
- `fetch_method = rss`: nenhuma das fontes de energia tem RSS utilizável; os valores reais serão
  `api` (ANEEL) e `listing` (MME, ANP, EPE).
- Nota jurídica da spec continua valendo: leitura prática, não parecer; consultar um advogado
  antes de monetizar.
