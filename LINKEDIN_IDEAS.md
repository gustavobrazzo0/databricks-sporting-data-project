# Ideias de posts para LinkedIn

Rascunhos de pauta, nao textos finais. Cada um tem gancho, pontos a
cobrir e uma sugestao de fechamento. Ajustar tom e tamanho antes de
publicar. Sem mencao a IA ou a forma como o codigo foi produzido, sem
caracteres especiais (travessao longo etc.).

## 1. Post de lancamento (narrativa completa do projeto)

Gancho: construi um pipeline de dados de ponta a ponta simulando a fusao
de duas empresas (uma matriz consolidada e uma startup com dados sujos),
do zero ao dashboard.

Cobrir:
- O cenario: por que simular uma fusao (dados legados bem estruturados
  de um lado, dados crus de planilha/API do outro, e o desafio real de
  unificar os dois em um so modelo).
- Arquitetura Medalhao (Bronze, Silver, Gold) e por que ela separa bem
  essas duas realidades.
- Stack: Databricks Free Edition, Unity Catalog, Volumes no lugar de S3,
  Delta Lake, Jobs, Dashboard e Genie.
- Resultado final: numero de linhas na fato consolidada, dashboard com
  5 KPIs.
- Link do repositorio no GitHub.

Fechamento: convite para comentarios/perguntas sobre a arquitetura, ou
pergunta direta ("qual dessas etapas voces acham mais dificil de
sustentar em produzao?").

## 2. Post tecnico: parsing de data em portugues

Gancho: um problema pequeno de internacionalizacao que quebra silenciosamente
o parse de datas em Spark.

Cobrir:
- O formato de origem: dia da semana e mes por extenso em portugues
  ("terca-feira, 01 de julho de 2025").
- Por que o parse nativo de padrao de mes por extenso (MMMM) depende do
  idioma padrao da JVM, e falha ou da resultado errado sem aviso claro.
- Por que um regex pensado para ingles (sem acento, sem hifen no nome do
  dia da semana) tambem falha.
- Solucao: regex generico baseado na posicao da primeira virgula (funciona
  em qualquer idioma) mais um mapeamento manual de nome de mes para
  numero antes do parse final.
- Licao maior: qualquer parsing de data por nome de mes/dia e um ponto de
  risco de internacionalizacao, vale desenhar pensando nisso desde o
  inicio.

Fechamento: pergunta sobre outros problemas de regionalizacao que a
audiencia ja enfrentou em pipelines de dados.

## 3. Post tecnico: duas estrategias de reconciliacao de granularidade

Gancho: nem toda reconciliacao de granularidade e uma soma. Duas
estrategias usadas no mesmo projeto, para dois tipos diferentes de
metrica.

Cobrir:
- Caso 1 (metrica aditiva): quantidade vendida por dia reconciliada para
  quantidade vendida por mes, via group by + sum.
- Caso 2 (metrica nao aditiva): preco de produto por mes reconciliado
  para um preco representante por ano, via window function
  (partitionBy produto e ano, orderBy priorizando preco valido e mes
  mais recente, row_number).
- O criterio pra escolher entre as duas: a metrica pode ser somada sem
  perder sentido, ou ela representa um estado (preco, saldo, taxa) que
  precisa de um "qual desses vale" em vez de "quanto isso soma"?

Fechamento: pergunta sobre outros exemplos de metricas nao aditivas que
a audiencia ja precisou reconciliar (saldo, taxa de conversao, estoque).

## 4. Post tecnico: carga incremental idempotente

Gancho: por que "somar o incremento de hoje ao total existente" e a
forma errada de fazer carga incremental, e o que fazer no lugar.

Cobrir:
- O problema: se um dia for reprocessado (rerun por erro, atraso, etc.),
  simplesmente somar de novo duplica valores.
- Solucao usada: tabelas de staging (sobrescritas a cada rodada, nunca
  acumulam), identificacao dos meses afetados pela carga do dia, e
  recalculo completo desses meses a partir do historico diario
  acumulado, substituindo (nao somando) o valor existente.
- Por que isso garante idempotencia: reprocessar o mesmo dia dez vezes
  da exatamente o mesmo resultado.

Fechamento: pergunta sobre como a audiencia lida com reprocessamento em
seus proprios pipelines incrementais.

## 5. Post de posicionamento: Databricks Free Edition como ambiente de
   estudo e portfolio

Gancho: e possivel montar um projeto completo de engenharia de dados,
com Delta Lake, orquestracao e dashboard, sem gastar nada em nuvem.

Cobrir:
- Databricks Free Edition: workspace gerenciado, sem cartao de credito,
  sem conta AWS/Azure/GCP vinculada.
- Unity Catalog + Volumes como substituto direto de S3 para o proposito
  deste projeto.
- O que fica igual a um ambiente de producao real (Delta Lake, Jobs,
  Dashboards, Genie) e o que e simplificado (sem custo de storage/
  compute de nuvem publica).
- Por que isso importa pra quem esta estudando ou construindo portfolio:
  reduz a barreira de entrada sem abrir mao das ferramentas reais do
  mercado.

Fechamento: convite pra quem tambem usa Databricks Free Edition trocar
experiencia, ou pergunta sobre quais outras ferramentas a audiencia usa
pra montar projetos de portfolio sem custo de nuvem.

## 6. Post bonus: unificando dois modelos de dados diferentes

Gancho: a parte mais interessante de simular uma fusao nao foi limpar os
dados sujos, foi decidir como duas fontes com maturidade diferente
convergem pro mesmo modelo dimensional.

Cobrir:
- Matriz: dados legados ja no formato Star Schema, direto na camada
  Gold.
- Startup: dados crus, precisam passar pelo pipeline completo (Bronze,
  Silver, Gold) antes de virarem candidatos ao merge.
- A convencao de prefixo (hn_) pras tabelas da startup antes do merge,
  e o merge final na mesma tabela Gold da matriz.
- Decisoes de escopo tomadas no caminho (por exemplo, quais cargas
  historicas viram tasks recorrentes de orquestracao e quais sao
  bootstrap de execucao unica).

Fechamento: pergunta sobre como a audiencia lidou com unificacao de
modelos de dados apos fusoes/aquisicoes reais.
