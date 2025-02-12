WITH
  TodosVendedores AS (
    SELECT DISTINCT
      t1.idVendedor ,
      count(distinct t2.idPedido) AS QtdPedido ,
      date(date_trunc("month", t3.dtPedido)) AS dtPedido
    FROM silver.olist.Vendedor AS t1
    LEFT JOIN silver.olist.item_pedido AS t2
      ON t1.idVendedor = t2.idVendedor
    LEFT JOIN silver.olist.pedido AS t3
      ON t2.idPedido = t3.idPedido
    GROUP BY ALL 
    ORDER BY dtPedido
  ) ,
  TabelaDataRef1_1 AS (
    SELECT
      * ,
      (CASE WHEN dtPedido >= '2018-01-01' AND dtPedido < '2018-07-01' THEN 'Historico'
        WHEN dtPedido >= '2018-07-01' AND dtPedido < '2018-08-01' THEN 'Predicao'
        ELSE 'Remover'
      END) AS DataRef1
    FROM TodosVendedores
    GROUP BY ALL 
    HAVING DataRef1 != 'Remover'
  ) ,
  TabelaDataRef1_2 AS (
    SELECT *,
      (CASE WHEN DataRef1 = 'Predicao' AND QtdPedido > 0 THEN 1 ELSE 0 END) AS Flag_TevePedido ,
      min(dtPedido) OVER (PARTITION BY DataRef1 ) AS Min_Mes ,
      max(dtPedido) OVER (PARTITION BY DataRef1 ) AS Max_Mes 
    FROM TabelaDataRef1_1
    GROUP BY ALL
  ) ,
  TabelaDataRef1_3 AS (
    SELECT * ,
      date_diff(month, dtPedido, Max_Mes) AS month_diff,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 0 THEN 1 ELSE 0 END) AS Flag_P1M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 1 THEN 1 ELSE 0 END) AS Flag_P2M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 2 THEN 1 ELSE 0 END) AS Flag_P3M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 3 THEN 1 ELSE 0 END) AS Flag_P4M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 4 THEN 1 ELSE 0 END) AS Flag_P5M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) = 5 THEN 1 ELSE 0 END) AS Flag_P6M ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) <= 2 THEN 1 ELSE 0 END) AS Flag_P3Mcomb ,
      (CASE WHEN date_diff(month, dtPedido, Max_Mes) <= 5 THEN 1 ELSE 0 END) AS Flag_P6Mcomb
    FROM TabelaDataRef1_2
  ) 
SELECT 
  idVendedor ,
  sum(Flag_TevePedido) AS Flag_TevePedido ,
  sum(Flag_P1M * QtdPedido) AS QtdPedidoP1M ,
  sum(Flag_P2M * QtdPedido) AS QtdPedidoP2M ,
  sum(Flag_P3M * QtdPedido) AS QtdPedidoP3M ,
  sum(Flag_P4M * QtdPedido) AS QtdPedidoP4M ,
  sum(Flag_P5M * QtdPedido) AS QtdPedidoP5M ,
  sum(Flag_P6M * QtdPedido) AS QtdPedidoP6M ,
  sum(Flag_P3Mcomb * QtdPedido) AS QtdPedidoP3Mcomb ,
  sum(Flag_P6Mcomb * QtdPedido) AS QtdPedidoP6MComb 
FROM TabelaDataRef1_3
GROUP BY ALL
