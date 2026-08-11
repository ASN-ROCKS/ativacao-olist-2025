WITH tb_base AS (
  SELECT 
    --Campos básicos
    distinct 
    v.idVendedor, 
    ip.idPedido, 
    ip.vlPreco, 
    pr.idProduto, 
    pr.descCategoria, 
    pr.nrTamanhoNome,
    pr.nrTamanhoDescricao,
    pr.nrFotos,
    pr.vlComprimentoCm,
    pr.vlAlturaCm,
    pr.vlLarguraCm,
    pr.vlPesoGramas,
    --Campos calculados para auxiliar nos indicadores
    if(pr.nrFotos > 0, 1, 0) as flTemFoto,
    if(pr.descCategoria is not null, 1, 0) as flTemDesc

  FROM silver.olist.vendedor as v
  LEFT JOIN silver.olist.item_pedido as ip ON v.idVendedor = ip.idVendedor
  LEFT JOIN silver.olist.pedido as p ON ip.idPedido = p.idPedido
  LEFT JOIN silver.olist.produto as pr ON ip.idProduto = pr.idProduto

  WHERE 1=1
    AND DATE(p.dtPedido) < '{date}'
    AND pr.descCategoria is not null
),

-- Agrupando as features para cada vendedor (exceto as que dependem de categoria)
tbFeaturesBase AS (
  SELECT  
    idVendedor,
    COUNT(DISTINCT idProduto) as qtdProdutosDistintos,
    COUNT(DISTINCT descCategoria) as categoriasDistintas,
    AVG(nrTamanhoNome) as mediaTamanhoNomeProduto,
    AVG(nrTamanhoDescricao) as mediaTamanhoDescricaoProduto,
    AVG(vlPesoGramas) AS mediaPesoGramas,
    AVG(nrFotos) as qtdMediaFotos,
    SUM(flTemFoto)/COUNT(idProduto) as pctProdutosComFotos,
    SUM(flTemDesc)/COUNT(idProduto) as pctProdutosComDescricao,
    AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS mediaCubagemProduto

  FROM tb_base
  GROUP BY ALL
),

-- Tabela com resultados por categoria de cada seller
tb_VendasCategoriasSeller AS(
  SELECT
    idVendedor,
    descCategoria,
    SUM(vlPreco) as totalVendasCategoriaSeller,
    COUNT(DISTINCT(idPedido)) as totalPedidosCategoriaSeller

  FROM tb_base
  GROUP BY ALL
),

-- Tabela com a top categoria de cada seller por venda e pedidos
tb_topCategoriaSeller AS(
  SELECT
    DISTINCT
    idVendedor,
    FIRST_VALUE(descCategoria) OVER(PARTITION BY idVendedor ORDER BY totalVendasCategoriaSeller DESC) as maiorCategoriaVenda,
    FIRST_VALUE(TotalVendasCategoriaSeller) OVER(PARTITION BY idVendedor ORDER BY totalVendasCategoriaSeller DESC) as maiorCategoriaTotalReceita,
    FIRST_VALUE(descCategoria) OVER(PARTITION BY idVendedor ORDER BY totalPedidosCategoriaSeller DESC) as maiorCategoriaQtdePedidos,
    FIRST_VALUE(TotalPedidosCategoriaSeller) OVER(PARTITION BY idVendedor ORDER BY totalPedidosCategoriaSeller DESC) as maiorCategoriaTotalPedidos
  FROM tb_VendasCategoriasSeller
),

-- Criar a tabela com as top 15 categorias gerais e o total de vendas delas
tb_topCategorias AS(
  SELECT
    descCategoria,
    sum(vlPreco) as TotalVendasCategoria

  FROM tb_base
  GROUP BY descCategoria 
  ORDER BY TotalVendasCategoria DESC
  LIMIT 15 --Top 15 categorias apenas
),

-- Criar a tabela com o valor de cada seller dentro das top 15 categorias gerais
tb_topCategoriasGeralSeller AS(
  SELECT
    vsc.idVendedor,
    vsc.descCategoria,
    vsc.TotalVendasCategoriaSeller/tc.TotalVendasCategoria as shareSellerCategoria
  FROM tb_VendasCategoriasSeller vsc
  INNER JOIN tb_topCategorias tc ON tc.descCategoria = vsc.descCategoria
),

-- Total share por categorias
tb_shareTopCategoriaSeller AS(
  SELECT
    vsc.idVendedor,
    SUM(vsc.TotalVendasCategoriaSeller/tc.TotalVendasCategoria) as shareTopCategorias
  FROM tb_VendasCategoriasSeller vsc
  INNER JOIN tb_topCategorias tc ON tc.descCategoria = vsc.descCategoria 

  GROUP BY ALL
),

--Pivot pra ficar cada categoria em uma coluna
tb_topCategoriasGeralSellerPivot AS (
  SELECT
    idVendedor,
    COALESCE(SUM(CASE WHEN descCategoria = 'automotivo' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctautomotivo,
    COALESCE(SUM(CASE WHEN descCategoria = 'beleza_saude' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctbeleza_saude,
    COALESCE(SUM(CASE WHEN descCategoria = 'brinquedos' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctbrinquedos,
    COALESCE(SUM(CASE WHEN descCategoria = 'cama_mesa_banho' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctcama_mesa_banho,
    COALESCE(SUM(CASE WHEN descCategoria = 'cool_stuff' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctcool_stuff,
    COALESCE(SUM(CASE WHEN descCategoria = 'eletroportateis' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pcteletroportateis,
    COALESCE(SUM(CASE WHEN descCategoria = 'esporte_lazer' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctesporte_lazer,
    COALESCE(SUM(CASE WHEN descCategoria = 'ferramentas_jardim' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctferramentas_jardim,
    COALESCE(SUM(CASE WHEN descCategoria = 'informatica_acessorios' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctinformatica_acessorios,
    COALESCE(SUM(CASE WHEN descCategoria = 'moveis_decoracao' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctmoveis_decoracao,
    COALESCE(SUM(CASE WHEN descCategoria = 'moveis_escritorio' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctmoveis_escritorio,
    COALESCE(SUM(CASE WHEN descCategoria = 'perfumaria' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctperfumaria,
    COALESCE(SUM(CASE WHEN descCategoria = 'relogios_presentes' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctrelogios_presentes,
    COALESCE(SUM(CASE WHEN descCategoria = 'telefonia' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pcttelefonia,
    COALESCE(SUM(CASE WHEN descCategoria = 'utilidades_domesticas' THEN totalPedidosCategoriaSeller END)/ SUM(totalPedidosCategoriaSeller),0) AS pctutilidades_domesticas

FROM tb_VendasCategoriasSeller
group by all
),

tb_final AS (
    --Join final com todas as tabelas. 998 sellers ao todo
    SELECT '{date}' AS dtRef,
      *
    FROM tbFeaturesBase AS t1
    LEFT JOIN tb_topCategoriaSeller AS t2 USING (idVendedor)
    LEFT JOIN tb_shareTopCategoriaSeller AS t3 USING (idVendedor)
    LEFT JOIN tb_topCategoriasGeralSellerPivot AS t4 USING (idVendedor)

    ORDER BY idVendedor
)

SELECT * FROM tb_final