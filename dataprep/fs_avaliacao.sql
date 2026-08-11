WITH tb_base AS (
  SELECT v.idVendedor,
         ip.idPedido,
         p.dtPedido,
         ap.idAvaliacao,
         ap.vlNota

  FROM silver.olist.vendedor as v
  LEFT JOIN silver.olist.item_pedido as ip ON v.idVendedor = ip.idVendedor
  LEFT JOIN silver.olist.pedido as p ON ip.idPedido = p.idPedido
  LEFT JOIN silver.olist.avaliacao_pedido as ap ON ip.idPedido = ap.idPedido
  WHERE p.dtPedido < '{date}'

),

-- Incluir os atributos da feature store na tabela
tb_feat_avaliacao AS (
  SELECT idVendedor, 
         AVG(vlNota) AS mediaAvaliacao,
         COUNT(vlNota) AS qtdReviewsAteHoje,
         AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) AS mediaAvaliacao28d, -- 1 mes
         AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END) AS mediaAvaliacao56d, -- 2 meses
         AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END) AS mediaAvaliacao84d, -- 3 meses
         AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END) AS mediaAvaliacao168d, -- 6 meses
         AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END) AS mediaAvaliacao336d, -- 12 meses
         COUNT(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) AS qtdReviewsAteHoje28d,
         COUNT(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END) AS qtdReviewsAteHoje56d,
         COUNT(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END) AS qtdReviewsAteHoje84d,
         COUNT(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END) AS qtdReviewsAteHoje168d,
         COUNT(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END) AS qtdReviewsAteHoje336d,
         COUNT(CASE WHEN dtv.idAvaliacao IS NULL THEN 1 END) * 1.0 / COUNT(dtv.idPedido) AS pctPedidosNaoAvaliado,
         COUNT(CASE WHEN dtv.vlNota IS NULL THEN 1 END) qtdeNotaNaoAvaliado,
         COUNT(CASE WHEN dtv.vlNota = 0 THEN 1 END) AS qtdeNota0,
         COUNT(CASE WHEN dtv.vlNota = 1 THEN 1 END) AS qtdeNota1,
         COUNT(CASE WHEN dtv.vlNota = 2 THEN 1 END) AS qtdeNota2,
         COUNT(CASE WHEN dtv.vlNota = 3 THEN 1 END) AS qtdeNota3,
         COUNT(CASE WHEN dtv.vlNota = 4 THEN 1 END) AS qtdeNota4,
         COUNT(CASE WHEN dtv.vlNota = 5 THEN 1 END) AS qtdeNota5,
         SUM(CASE WHEN dtv.vlNota = 1 THEN 1 END) / COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNota1,
         SUM(CASE WHEN dtv.vlNota = 2 THEN 1 END) / COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNota2,
         SUM(CASE WHEN dtv.vlNota = 3 THEN 1 END) / COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNota3,
         SUM(CASE WHEN dtv.vlNota = 4 THEN 1 END) / COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNota4,
         SUM(CASE WHEN dtv.vlNota = 5 THEN 1 END) / COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNota5,
         COUNT(CASE WHEN dtv.vlNota = 0 OR  dtv.vlNota = 1 OR dtv.vlNota = 2 THEN 1 END) AS qtdeNotaBaixa,
         SUM(CASE WHEN dtv.vlNota = 0 OR  dtv.vlNota = 1 OR dtv.vlNota = 2 THEN 1 END) / 
             COUNT(CASE WHEN dtv.idAvaliacao IS NOT NULL THEN dtv.idAvaliacao END) AS pctNotaBaixa,
        -- Percentual de variação da média de avaliações entre 1 mês e 2 meses
         (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
              AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END)) 
              /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END)
              AS pctTendencia1m_2m,             
        -- Percentual de variação da média de avaliações entre 1 mês e 3 meses
        (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
             AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END)) 
            /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END)
             AS pctTendencia1m_3m,
        -- Percentual de variação da média de avaliações entre 1 mês e 4 meses
        (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
             AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END)) 
            /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END)
             AS pctTendencia1m_4m,
        -- Percentual de variação da média de avaliações entre 1 mês e 5 meses
        (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
             AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END)) 
            /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END)
             AS pctTendencia1m_5m,
        -- Percentual de variação da média de avaliações entre 1 mês e 6 meses
        (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
            AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END)) 
            / AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END)
                 AS pctTendencia1m_6m,
        -- Percentual de variação da média de avaliações entre 1 mês e 12 meses
        (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
            AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END)) 
            / AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END)
                 AS pctTendencia1m_12m,
       -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 2 meses
         CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 56 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_2m,             
        -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 3 meses
         CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 84 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_3m,  
        -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 4 meses
         CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 112 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_4m,  
        -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 5 meses
         CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 140 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_5m, 
        -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 6 meses
         CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 168 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_6m, 
        -- Indicador de tendência das avaliações do seller (crescente, estável ou decrescente) entre 1 mês e 12 meses
        CASE WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END) > 0.05 THEN 'Crescente'
              WHEN (AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 28 DAY THEN vlNota END) - 
                    AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END)) 
                    /AVG(CASE WHEN  dtv.dtPedido > '{date}' - INTERVAL 336 DAY THEN vlNota END) < -0.05 THEN 'Decrescente'
              ELSE 'Estavel' END AS Tendencia1m_12m

  FROM tb_base AS dtv
  GROUP BY idVendedor
)

SELECT 
      '{date}' AS dtRef,
      *
FROM tb_feat_avaliacao