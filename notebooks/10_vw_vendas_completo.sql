CREATE OR REPLACE VIEW gana_pati.gold.vw_vendas_completo AS
SELECT
    f.date, f.product_code, f.customer_code, f.sold_quantity,
    c.customer, c.market, c.platform, c.channel,
    p.division, p.category, p.product, p.variant,
    gp.price_brl,
    (f.sold_quantity * gp.price_brl) AS receita
FROM gana_pati.gold.fact_orders f
LEFT JOIN gana_pati.gold.dim_customers c ON f.customer_code = c.customer_code
LEFT JOIN gana_pati.gold.dim_products p ON f.product_code = p.product_code
LEFT JOIN gana_pati.gold.dim_gross_price gp
    ON f.product_code = gp.product_code AND CAST(year(f.date) AS STRING) = gp.year;
