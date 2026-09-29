# Mapa de renomeacao: original (Codebasics) para nosso projeto

Fonte original: curso Codebasics (video + project-codebasics/), empresas
"Atlon" (matriz) e "Sports Bar" (startup adquirida), dados com typos em
cidades indianas e precos em INR. Este projeto adapta o cenario para
portfolio proprio: mesma logica de engenharia (mesmas transformacoes, mesmos
tipos de dado sujo), mas com nomes, moeda e regiao proprios.

## Empresas

| Original | Neste projeto |
|---|---|
| Atlon (matriz) | Ganapati Artigos Esportivos |
| Sports Bar (startup) | Hastinapur Nutrition |

## Ambiente

| Original | Neste projeto |
|---|---|
| Databricks Community/Free Edition + AWS S3 | Databricks Free Edition + Volumes (Unity Catalog), sem AWS |
| Catalogo fmcg | Catalogo gana_pati |
| s3://sportsbar-dp/(origem)/... | /Volumes/gana_pati/raw/source_files/(origem)/... |
| Prefixo das tabelas da startup pre-merge: sb_ | hn_ (Hastinapur Nutrition) |

## Moeda

| Original | Neste projeto |
|---|---|
| price_inr (rupia), matriz e startup | price_brl (real) |

Conversao aplicada nos dados brutos (nao e uma etapa do pipeline Silver, os
dados ja chegam em BRL): matriz price_inr vezes 0.06, startup gross_price
vezes 0.07 (taxas ficticias escolhidas so para gerar valores plausiveis de
mercado: equipamento esportivo de R$18 a R$1140, barras/produtos de
nutricao de R$3 a R$35). Sinal negativo e o texto "unknown" foram mantidos
intactos, sao o dado sujo proposital que o pipeline Silver trata.

## Cidades (customers.csv da startup): mesmo padrao de erro, cidade diferente

| Original (India) | Aqui (Brasil) | Tipo de erro preservado |
|---|---|---|
| Bengaluru | Sao Paulo (com acento) | forma correta |
| Bengalore | Sao Paulo (sem acento) | nome sem acento |
| Bengaluruu | Sao Pauloo | letra duplicada no final |
| Hyderabad | Rio de Janeiro | forma correta |
| Hyderabadd | Rio de Janeiroo | letra duplicada no final |
| Hyderbad | Rio de Janiro | letra faltando |
| New Delhi | Belo Horizonte | forma correta |
| NewDelhi | BeloHorizonte | sem espaco |
| NewDheli | Belo Horiznote | letras transpostas |
| NewDelhee | BeloHorizontee | sem espaco, final alterado |
| (vazio) | (vazio) | nulo, sem alteracao |

## Nomes de loja/cliente (customers.csv da startup)

Traduzidos/adaptados para o portugues, preservando exatamente o mesmo tipo
de inconsistencia de capitalizacao/espacamento do original (necessario para
os exercicios de trim/initcap). Ver child_company/full_load/customers/customers.csv
para o resultado completo; alguns exemplos:

| Original | Aqui |
|---|---|
| FitFuel Market | Mercado FitFuel |
| Athlete's Choice Store | Loja Escolha do Atleta |
| MacroBite superfoods (typo de caixa) | Superalimentos macroBite (mesmo tipo de typo) |
| Peak performance Store / Peak Performance store | Loja alto Desempenho / Loja Alto desempenho |
| champion's Choice / Champion's choice | escolha do Campeao / Escolha do campeao |

## Datas por extenso em ingles (orders/*.csv da startup)

Formato "Weekday, Month DD, YYYY" traduzido para "dia-da-semana, DD de mes
de YYYY" (exemplo: "Tuesday, July 01, 2025" virou "terca-feira, 01 de julho
de 2025"). Os demais formatos numericos (yyyy/MM/dd, dd-MM-yyyy etc.) foram
mantidos como estao, ja sao formatos ambiguos/genericos, nao especificos de
uma regiao, entao continuam servindo ao mesmo proposito didatico
(coalesce + try_to_date).

## Produtos (products.csv da startup)

- Prefixo de marca "SportsBar" virou "Hastinapur" (exemplo: "SportsBar
  Energy Bar..." virou "Hastinapur Energy Bar...")
- Erro de digitacao proposital em "Protien" (categoria e produto), mantido
  de proposito
- ID invalido "XYZ123" e linhas duplicadas no fim do arquivo, mantidos de
  proposito
- Precos orfaos em gross_price.csv sem produto correspondente (88888888,
  99999999, 77777777, e 25891502 sem linha em products.csv), mantidos de
  proposito, sao o dado sujo do exercicio de join

## O que foi copiado sem alteracao de conteudo

- fact_orders.csv (matriz, full load e incremental): so tem datas ISO
  limpas e quantidades
- dim_customers.csv e dim_products.csv (matriz): nomes genericos, sem marca
  do curso

## Onde estao os arquivos

- Fonte original do curso: project-codebasics/0_data/
- Dados adaptados para este projeto: data/raw/
