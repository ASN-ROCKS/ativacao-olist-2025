-- Databricks notebook source
CREATE TABLE sandbox.asn.abt_t5

WITH tb_publico (
  SELECT distinct 
      '2017-06-01' as dtRef,
      t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  where t1.dtPedido < '2017-06-01'
  and t2.idVendedor is not null
),

tb_vendas_futuro AS (

  SELECT distinct t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  where t1.dtPedido < '2017-06-01' + INTERVAL 30 DAY
  AND t1.dtPedido >= '2017-06-01'
  and t2.idVendedor is not null

),

tb_flag_sem_venda AS (

  SELECT t1.dtRef,
         t1.idVendedor,
        CASE WHEN t2.idVendedor IS NULL THEN 1 ELSE 0 END AS flagSemVenda

  FROM tb_publico AS t1

  LEFT JOIN tb_vendas_futuro As t2
  ON t1.idVendedor = t2.idVendedor

), 

abt AS (
    SELECT *
    FROM tb_flag_sem_venda AS t1

    LEFT JOIN sandbox.asn.fs_seller_avaliacao_t5 USING (idVendedor, dtRef)
    LEFT JOIN sandbox.asn.fs_seller_cliente_t5 USING (idVendedor, dtRef)
    LEFT JOIN sandbox.asn.fs_seller_pagamento_t5 USING (idVendedor, dtRef)
    LEFT JOIN sandbox.asn.fs_seller_produto_t5 USING (idVendedor, dtRef)
    LEFT JOIN sandbox.asn.fs_seller_t5 USING (idVendedor, dtRef)
    LEFT JOIN sandbox.asn.fs_seller_vendas_t5 USING (idVendedor, dtRef)
)

SELECT *
FROM abt

-- COMMAND ----------

 select * from sandbox.asn.fs_seller_cliente_t5