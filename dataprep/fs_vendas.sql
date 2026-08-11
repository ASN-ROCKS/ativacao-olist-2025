
WITH tb_base AS (
  SELECT  '{date}' AS dtRef,
          v.idVendedor AS idVendedor,
          ip.idPedido AS idPedido,
          DATE(p.dtPedido) AS dtPedido,
          DATE(p.dtAprovado) AS dtAprovacao,
          DATE(p.dtEstimativaEntrega) AS dtEstimativaEntrega,
          DATE(dtEntregue) AS dtEntrega,
          pp.descTipoPagamento AS descTipoPagamento,
          pp.nrParcelas AS nrParcelas,
          SUM(ip.vlPreco) AS vlPreco,
          SUM(ip.vlFrete) AS vlFrete,
          COUNT(ip.idPedidoItem) AS qtdeItensPedido

  FROM    silver.olist.vendedor as v

  LEFT JOIN silver.olist.item_pedido AS ip
  ON v.idVendedor = ip.idVendedor

  LEFT JOIN silver.olist.pedido AS p
  ON ip.idPedido = p.idPedido

  LEFT JOIN silver.olist.pagamento_pedido AS pp
  ON ip.idPedido = pp.idPedido

  WHERE p.dtPedido < '{date}'
  GROUP BY ALL
),

-- Incluir os atributos da feature store pagamento na tabela
tb_feat_vendas AS (
  SELECT  dtRef,
          idVendedor,
          -- Recência Pedidos: qtde de dias desde o último pedido
          DATE_DIFF(dtRef, MAX(dtPedido)) AS qtdeDiasUltimoPedido,
          -- Tempo desde primeiro pedido: qtde de dias desde o primeiro pedido
          DATE_DIFF(dtRef, MIN(dtPedido)) AS qtdeDiasPrimeiroPedido,
          COUNT(idPedido) AS qtdePedidos,
          AVG(vlPreco + vlFrete) AS vlTicketMedio,
          AVG(vlPreco) AS vlMediaPreco,
          AVG(vlFrete) AS vlMediaFrete,
          MAX(dtPedido) >= DATE('{date}') - INTERVAL 6 MONTH AS inPedido6Meses,
          DATE_DIFF(MAX(dtPedido), MIN(dtPedido)) AS qtdeDiasPrimeiroUltimoPedido,
          SUM(COALESCE(qtdeItensPedido, 0)) AS qtdeItensPedido,
          SUM(CASE WHEN dtAprovacao IS NULL THEN 0 ELSE 1 END) / COUNT(idPedido) AS pctPedidosAprovados,
          AVG(DATE_DIFF(dtEstimativaEntrega, dtPedido)) AS qtdeMediaDiasPedidoEstEntrega,
          SUM(vlFrete) AS vlTotalFrete,
          SUM(vlFrete) / SUM(vlPreco) AS indFretePreco,
          AVG(vlFrete / vlPreco) AS indMedioFretePrecoPorPedido,
          SUM(CASE WHEN DATE_DIFF(dtEstimativaEntrega, dtEntrega) >= 0 THEN 1 ELSE 0 END) / COUNT(dtEntrega) AS pctPedidosEntreguesNoPrazo,
          SUM(CASE WHEN COALESCE(nrParcelas, 1) > 1 THEN 1 ELSE 0 END) / COUNT(idPedido) AS pctPedidosParcelados,
        COUNT(CASE WHEN dtPedido >= '{date}' - INTERVAL 28 DAY THEN idPedido END) AS qtdePedidoD28,
        COUNT(CASE WHEN dtPedido >= '{date}' - INTERVAL 14 DAY THEN idPedido END) AS qtdePedidoD14,
        COUNT(CASE WHEN dtPedido >= '{date}' - INTERVAL 7 DAY THEN idPedido END) AS qtdePedidoD7,
        COUNT(CASE WHEN dtPedido >= '{date}' - INTERVAL 28 DAY THEN idPedido END) / COUNT(CASE WHEN dtPedido >= '{date}' - INTERVAL 56 DAY AND dtPedido < '{date}' - INTERVAL 28 DAY THEN idPedido END) AS crescimentoD28,
        count(distinct CASE WHEN dtPedido >= '{date}' - interval 84 DAY THEN idPedido END) / 3 AS avgPedidoM3
  FROM    tb_base
  GROUP BY dtRef,
          idVendedor
),

tb_daily AS (
  SELECT  DISTINCT
          idVendedor,
          date(dtPedido) AS dtPedido
  FROM    tb_base
  GROUP BY ALL
),

tb_lag AS (
  SELECT  *,
          LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido DESC) AS dtProximoPedido
  FROM    tb_daily
),

tb_feat_vendas_lag AS (
  SELECT  idVendedor,
          AVG(DATE_DIFF(tb_lag.dtProximoPedido, tb_lag.dtPedido)) AS qtdeMediaDiasEntrePedidos
  FROM    tb_lag
  GROUP BY idVendedor
),

tb_weekly AS (

SELECT idVendedor,
        year(dtPedido) || weekofyear(dtPedido) AS dtWeek,
        count(distinct idPedido) AS qtdePedidoSemana

FROM tb_base
GROUP BY ALL
),

summary_weekly AS (
SELECT idVendedor,
       stddev_pop(qtdePedidoSemana) AS stdPedidoSemana
FROM tb_weekly
GROUP BY ALL

),

tb_final AS (

SELECT  t1.*,
        t2.qtdeMediaDiasEntrePedidos,
        t3.stdPedidoSemana
FROM tb_feat_vendas AS t1

LEFT JOIN tb_feat_vendas_lag AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN summary_weekly as t3
ON t1.idVendedor = t3.idVendedor

)

SELECT *
FROM tb_final