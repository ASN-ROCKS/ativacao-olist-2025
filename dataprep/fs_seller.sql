
with params as (
  select
    '{date}' as dt_ref
),

sellers as (
  select
    idVendedor,
    descUF as vendedorDescUF,
    case when descUF in ('SP', 'RJ', 'MG', 'ES') then 'SUDESTE'
         when descUF IN ('PR', 'SC', 'RS') THEN 'SUL'
         when descUF in ('RO', 'AC', 'AM', 'RR', 'AP', 'PA', 'TO') then 'NORTE'
         when descUF in ('MT', 'MS', 'GO', 'DF') then 'CENTRO-OESTE'
         when descUF in ('BA', 'SE', 'AL', 'CE', 'MA', 'PE') then 'NORDESTE' end as vendedorRegiao
  from silver.olist.vendedor
),

orders as (
  select
      p.idPedido,
      p.idCliente,
      c.descUF as clienteDescUF,
      ip.idVendedor,
      ip.vlPreco
  from silver.olist.pedido as p

  left join silver.olist.item_pedido as ip
  on ip.idPedido = p.idPedido

  left join silver.olist.cliente as c
  on c.idCliente = p.idCliente

  where date(p.dtPedido) < (select date(dt_ref) from params)
  and idVendedor is not null
),

states as (
  select
    distinct
    clienteDescUF
  from orders
),

sellers_and_states as (
  select
    idVendedor,
    clienteDescUF
  from sellers
  cross join states
),

gmv_by_seller_state as (
  select
    ss.idVendedor,
    ss.clienteDescUF,
    coalesce(sum(ss.vlPreco), 0) as GMV_by_seller_and_state
  from orders as ss
  group by all
),

gmv_by_state as (
  select
    clienteDescUF,
    sum(vlPreco) as GMV_by_state
  from orders
  group by all
),

join_uf AS (

SELECT t1.*,
       t1.GMV_by_seller_and_state / t2.GMV_by_state as shareUfContribution

FROM gmv_by_seller_state AS t1

left join gmv_by_state As t2
ON t1.clienteDescUF = t2.clienteDescUF

),

pivot_uf AS (

  select idVendedor,
      max(case when clienteDescUf = 'AC' THEN shareUfContribution end) as shareUfContributionAC,
      max(case when clienteDescUf = 'AL' THEN shareUfContribution end) as shareUfContributionAL,
      max(case when clienteDescUf = 'AM' THEN shareUfContribution end) as shareUfContributionAM,
      max(case when clienteDescUf = 'AP' THEN shareUfContribution end) as shareUfContributionAP,
      max(case when clienteDescUf = 'BA' THEN shareUfContribution end) as shareUfContributionBA,
      max(case when clienteDescUf = 'CE' THEN shareUfContribution end) as shareUfContributionCE,
      max(case when clienteDescUf = 'DF' THEN shareUfContribution end) as shareUfContributionDF,
      max(case when clienteDescUf = 'ES' THEN shareUfContribution end) as shareUfContributionES,
      max(case when clienteDescUf = 'GO' THEN shareUfContribution end) as shareUfContributionGO,
      max(case when clienteDescUf = 'MA' THEN shareUfContribution end) as shareUfContributionMA,
      max(case when clienteDescUf = 'MG' THEN shareUfContribution end) as shareUfContributionMG,
      max(case when clienteDescUf = 'MS' THEN shareUfContribution end) as shareUfContributionMS,
      max(case when clienteDescUf = 'MT' THEN shareUfContribution end) as shareUfContributionMT,
      max(case when clienteDescUf = 'PA' THEN shareUfContribution end) as shareUfContributionPA,
      max(case when clienteDescUf = 'PB' THEN shareUfContribution end) as shareUfContributionPB,
      max(case when clienteDescUf = 'PE' THEN shareUfContribution end) as shareUfContributionPE,
      max(case when clienteDescUf = 'PI' THEN shareUfContribution end) as shareUfContributionPI,
      max(case when clienteDescUf = 'PR' THEN shareUfContribution end) as shareUfContributionPR,
      max(case when clienteDescUf = 'RJ' THEN shareUfContribution end) as shareUfContributionRJ,
      max(case when clienteDescUf = 'RN' THEN shareUfContribution end) as shareUfContributionRN,
      max(case when clienteDescUf = 'RO' THEN shareUfContribution end) as shareUfContributionRO,
      max(case when clienteDescUf = 'RR' THEN shareUfContribution end) as shareUfContributionRR,
      max(case when clienteDescUf = 'RS' THEN shareUfContribution end) as shareUfContributionRS,
      max(case when clienteDescUf = 'SC' THEN shareUfContribution end) as shareUfContributionSC,
      max(case when clienteDescUf = 'SE' THEN shareUfContribution end) as shareUfContributionSE,
      max(case when clienteDescUf = 'SP' THEN shareUfContribution end) as shareUfContributionSP,
      max(case when clienteDescUf = 'TO' THEN shareUfContribution end) as shareUfContributionTO
      FROM join_uf
      group by all
         
),

fs_seller as (
  select
    (select dt_ref from params) as dtRef,
    s.idVendedor,
    s.vendedorDescUF,
    s.vendedorRegiao,
    max(gs2.GMV_by_state) as gmvEstadoVendedor,

    coalesce(pivot_table.shareUfContributionAC, 0) as shareUfContributionAC,
    coalesce(pivot_table.shareUfContributionAL, 0) as shareUfContributionAL,
    coalesce(pivot_table.shareUfContributionAM, 0) as shareUfContributionAM,
    coalesce(pivot_table.shareUfContributionAP, 0) as shareUfContributionAP,
    coalesce(pivot_table.shareUfContributionBA, 0) as shareUfContributionBA,
    coalesce(pivot_table.shareUfContributionCE, 0) as shareUfContributionCE,
    coalesce(pivot_table.shareUfContributionDF, 0) as shareUfContributionDF,
    coalesce(pivot_table.shareUfContributionES, 0) as shareUfContributionES,
    coalesce(pivot_table.shareUfContributionGO, 0) as shareUfContributionGO,
    coalesce(pivot_table.shareUfContributionMA, 0) as shareUfContributionMA,
    coalesce(pivot_table.shareUfContributionMG, 0) as shareUfContributionMG,
    coalesce(pivot_table.shareUfContributionMS, 0) as shareUfContributionMS,
    coalesce(pivot_table.shareUfContributionMT, 0) as shareUfContributionMT,
    coalesce(pivot_table.shareUfContributionPA, 0) as shareUfContributionPA,
    coalesce(pivot_table.shareUfContributionPB, 0) as shareUfContributionPB,
    coalesce(pivot_table.shareUfContributionPE, 0) as shareUfContributionPE,
    coalesce(pivot_table.shareUfContributionPI, 0) as shareUfContributionPI,
    coalesce(pivot_table.shareUfContributionPR, 0) as shareUfContributionPR,
    coalesce(pivot_table.shareUfContributionRJ, 0) as shareUfContributionRJ,
    coalesce(pivot_table.shareUfContributionRN, 0) as shareUfContributionRN,
    coalesce(pivot_table.shareUfContributionRO, 0) as shareUfContributionRO,
    coalesce(pivot_table.shareUfContributionRR, 0) as shareUfContributionRR,
    coalesce(pivot_table.shareUfContributionRS, 0) as shareUfContributionRS,
    coalesce(pivot_table.shareUfContributionSC, 0) as shareUfContributionSC,
    coalesce(pivot_table.shareUfContributionSE, 0) as shareUfContributionSE,
    coalesce(pivot_table.shareUfContributionSP, 0) as shareUfContributionSP,
    coalesce(pivot_table.shareUfContributionTO, 0) as shareUfContributionTO
    
    from sellers as s

    inner join gmv_by_seller_state as gss
    on gss.idVendedor = s.idVendedor

    inner join gmv_by_state as gs2
    on gs2.clienteDescUF = s.vendedorDescUF

    inner join pivot_uf as pivot_table
    ON pivot_table.idVendedor = s.idVendedor

    group by all
)

select *
from fs_seller