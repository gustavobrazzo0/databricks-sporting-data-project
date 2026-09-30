-- Total revenue
SELECT SUM(receita) AS receita_total FROM gana_pati.gold.vw_vendas_completo;

-- Total quantity sold
SELECT SUM(sold_quantity) AS quantidade_total FROM gana_pati.gold.vw_vendas_completo;

-- Top 10 products by revenue
SELECT product, SUM(receita) AS receita
FROM gana_pati.gold.vw_vendas_completo
GROUP BY product
ORDER BY receita DESC
LIMIT 10;

-- Revenue by channel
SELECT channel, SUM(receita) AS receita
FROM gana_pati.gold.vw_vendas_completo
GROUP BY channel
ORDER BY receita DESC;

-- Monthly revenue trend
SELECT date_trunc('MONTH', date) AS mes, SUM(receita) AS receita
FROM gana_pati.gold.vw_vendas_completo
GROUP BY date_trunc('MONTH', date)
ORDER BY mes;
