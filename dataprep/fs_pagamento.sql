WITH tb_base AS (
  SELECT  v.idVendedor,
          ip.idPedido,
          p.dtPedido,
          pp.descTipoPagamento,
          pp.nrParcelas,
          SUM(ip.vlPreco) AS vlReceita

  FROM    silver.olist.vendedor as v

  LEFT JOIN silver.olist.item_pedido AS ip
  ON v.idVendedor = ip.idVendedor

  LEFT JOIN silver.olist.pedido AS p
  ON ip.idPedido = p.idPedido

  LEFT JOIN silver.olist.pagamento_pedido AS pp
  ON ip.idPedido = pp.idPedido

  WHERE   p.dtPedido < '{date}'
  GROUP BY ALL
),

-- Incluir os atributos da feature store pagamento na tabela
tb_feat_pagamento AS (
  SELECT  idVendedor,
          -- GMV por tipo de pagamento
          SUM(CASE WHEN descTipoPagamento = 'boleto' THEN vlReceita ELSE 0 END) AS gmvBoleto,
          SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN vlReceita ELSE 0 END) AS gmvCredito,
          SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN vlReceita ELSE 0 END) AS gmvDebito,
          SUM(CASE WHEN descTipoPagamento = 'voucher' THEN vlReceita ELSE 0 END) AS gmvVoucher,
          -- % pedidos por tipo de pagamento
          SUM(CASE WHEN descTipoPagamento = 'boleto' THEN 1 ELSE 0 END) 
            / COUNT(DISTINCT idPedido) AS pctPedidosBoleto,
          SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN 1 ELSE 0 END) 
            / COUNT(DISTINCT idPedido) AS pctPedidosCredito,
          SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN 1 ELSE 0 END) 
            / COUNT(DISTINCT idPedido) AS pctPedidosDebito,
          SUM(CASE WHEN descTipoPagamento = 'voucher' THEN 1 ELSE 0 END) 
            / COUNT(DISTINCT idPedido) AS pctPedidosVoucher,
          -- Média da quantidade de parcelas (excluindo à vista)
          AVG(CASE WHEN nrParcelas > 1 THEN nrParcelas END) AS mediaQtdeParcelas
  FROM tb_base
  GROUP BY idVendedor
)

SELECT  '{date}' AS dtRef,
        *
FROM    tb_feat_pagamento;