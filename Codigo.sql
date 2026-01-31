 with
-- Define as datas de referencia utilizadas nas bases
ref AS(
select  
20260109 as r1,  ---  Data da base OBT Diaria (Outliers)
202512 as r2,  --- retirar ultima referencia da base de NEJ NCOR, 
-- pois ela é realizada no dia 20 do mes anterior, 
-- não cumprindo o delay de 30 dias entre uma comunicação e outra.
'20260115' as r3  -- ultima referencia anomesdia da base de Contratos e Cliente PURC
),

--Base OBT Seleciona publico Outlrs BPF e NCOR
-- Seleciona contratos PF com mais de 90 dias de atraso e menos de 365 dias
-- sem inibições de contato H,T,C,Y
-- com saldo contabil maior que o minimo definido por franquia e subcanal
-- Calcula HDG para segmentação do publico
-- 
OBT as (
    SELECT
        num_cpf_cnpj_sdac,
        chpras,
        cprodlim,
        produto,
        produto_dt,
        area_executiva,
        sub_canal,
        sub_canal_dt,
        cod_inibicao,
        cod_sist_prod,
        franq_cartoes,
        faixa_atraso_micro_fluxo,
        dias_atraso_fluxo,
        round(vlr_saldo_contabil,2) as vlr_saldo_contabil,
        --round(vlr_saldo_gerencial,2) as vlr_saldo_gerencial,
        case
            when franq_cartoes = 'INTERNOS' and sub_canal_dt  = 'AGENCIAS ITAU' and vlr_saldo_contabil >= 50000  then 'elegivel'
            when  franq_cartoes = 'INTERNOS' and sub_canal_dt in ('ITAU UNICLASS','ITAU UNICLASS DIGITAL') and vlr_saldo_contabil >= 100000 then 'elegivel'
            when  franq_cartoes = 'INTERNOS' and sub_canal_dt in ('PERSONNALITE','ITAU PERSONNALITE DIGITAL') and vlr_saldo_contabil >= 500000 then 'elegivel'
       
            when  franq_cartoes = 'EXTERNOS' and sub_canal_dt = 'NPC - AZUL' and vlr_saldo_contabil >= 200000 then 'elegivel'
            when  franq_cartoes = 'EXTERNOS' and sub_canal_dt = 'TAM' and vlr_saldo_contabil >= 200000 then 'elegivel'
            when  franq_cartoes = 'EXTERNOS' and sub_canal_dt not in ('NPC - AZUL','TAM') and  vlr_saldo_contabil >= 50000 then 'elegivel'
       
            when  franq_cartoes = 'FINANCEIRAS' and sub_canal_dt = 'ASSAI' and vlr_saldo_contabil >= 50000 then 'elegivel'
            when  franq_cartoes = 'FINANCEIRAS' and sub_canal_dt = 'LUIZACRED' and vlr_saldo_contabil >= 50000 then 'elegivel'
        -- when  franq_cartoes = 'FINANCEIRAS' and sub_canal_dt = '' and vlr_saldo_contabil >=  then 'elegivel'
            when  franq_cartoes = 'FINANCEIRAS' and sub_canal_dt not in ('ASSAI','LUIZACRED') and vlr_saldo_contabil >= 50000 then 'elegivel'
       
            else ''
            end as elegivel,
           
        CASE WHEN produto = 'CARTAO' THEN 'CARTAO'
        when produto = 'COMPOSICAO' then 'RENEG'
        else ''
        END AS PROD_AGRUPADO,
        -- Calculo HDG
        cast(mod(from_base(substring(cast(to_hex(md5(cast(concat(cast(num_cpf_cnpj_sdac as varchar),'Carta_Cartoraria_2025') as varbinary))) as varchar),-8,8), 16), 10000) as double) as hdg,
        case when produto = 'CARTAO' THEN chpras *10000 else chpras end as contrato
    FROM database_db_compartilhado_consumer_daentendercliente.tb_spec_obt_diaria
    -- filtra contratos PF, area gestora cartao e banco PF
    WHERE tipo_pessoa = 'F'
        AND area_gestora IN ('BANCO PF', 'CARTAO')
        --and tipo_carteira = 'ATIVO'
        and dias_atraso_fluxo > 89  and dias_atraso_fluxo < 365
        and (cod_inibicao not in ('H','T','C','Y') or cod_inibicao is null)
        and
        -- seleciona ultima referencia da base OBT
        anomes_versao = (select r1 from ref)
),

--Cruza com bases de contratos e de cliente da PURC para enriquecer 
--com dados de endereço e telefone
-- seleciona apenas contratos elegiveis
base_ab1
as (
select distinct
a.*,
b.sig_unid_fedv_cobr,
b.cod_chav_coro_clie,
round(vlr_sald_atul,2) as vlr_saldo_gerencial,
cod_sgto_ctrt,
num_digt_rndc,
cod_moti_baix,
-- Tipo de cliente Correntista, Nao Correntista, Ex Correntista
case when cast(cod_idt_tipo_clie_corn as bigint) = 1 then 'Correntista'
    when cast(cod_idt_tipo_clie_corn as bigint) = 2 then 'Nao Correntista'
    when cast(cod_idt_tipo_clie_corn as bigint) = 3 then 'Ex Correntista'
    else 'Não Correntista' end as cliente_correntista,
b.nom_cida_ende_cobr,
b.dat_ulti_atrs
-- Cruza base da OBT com base de contratos e clientes da PURC para enriquecer dados
from OBT as a left join
db_corp_recuperacaodecredito_estrategiasrecupcredito_spec_01.tbjp7183_ctrt_cobr as b
on a.cprodlim = cast(cod_prod_lgdo as bigint) and a.chpras = (cast(num_ctrt_prod_opcr as bigint)/10000)
left join db_corp_recuperacaodecredito_estrategiasrecupcredito_spec_01.tbjp7001_clie_cobr as c
on b.cod_chav_coro_clie = c.cod_chav_coro_clie
-- filtra contratos elegiveis e ativos com motivo de baixa '00' 
where b.anomesdia = (select r3 from ref)
and b.ind_cobr_judi = 'N'
and c.anomesdia = (select r3 from ref)
and cod_moti_baix = '00'
and elegivel = 'elegivel'
-- select apenas contratos de produtos Cartao e Renegociacao 
-- porque sao os produtos que terao comunicacao cartoraria
and PROD_AGRUPADO != ''
),
-- Base de Telefone para buscar o melhor telefone cadastrado e com maior rating 
baseTelefone as (
select * from (
select
cod_chav_unic_rgto as cpf,
num_tel_clie_frmd as telefone,
vlr_ptsc_tel as rating,
ROW_NUMBER() OVER (PARTITION BY cod_chav_unic_rgto ORDER BY cast(dat_rfrc as bigint),cast(vlr_ptsc_tel as double) desc) AS RNK_0
from database_db_compartilhado_consumer_incubadorascrm.tbhf9389_tel_pfis_finl_cobr
-- where dat_rfrc = '20250513'
where num_tel_clie_frmd is not null)
where RNK_0 = 1),

-- Base de Email para buscar o melhor email cadastrado para contato 
baseEmail as (
select distinct
cod_chav_coro_clie,
min(cod_efte_emai) as cod_efte_emai
from db_corp_recuperacaodecredito_daentendercliente_spec_01.tbho8459_emai_pfis_brau_cobr
group by 1
),

-- adiciona email na base de outliers para contato e enriquecimento de dados
baseEmail1 as (
select
a.*,
b.txt_ende_emai
from baseEmail as a left join
db_corp_recuperacaodecredito_daentendercliente_spec_01.tbho8459_emai_pfis_brau_cobr as b
on a.cod_chav_coro_clie = b.cod_chav_coro_clie
and a.cod_efte_emai = b.cod_efte_emai
),

-- adiciona email na base de outliers para contato e enriquecimento de dados 
-- seu diferente da baseEmail1 é que aqui fazemos o join com a base_ab1
base_ab22 as (
select a.*,
txt_ende_emai as Email
from base_ab1 as a left join baseEmail1 as b
on a.cod_chav_coro_clie = b.cod_chav_coro_clie
),

-- adiciona telefone na base de outliers para contato e enriquecimento de dados
-- seu diferente da base_ab22 é que aqui fazemos o join com a base_ab1
-- a base final com email e telefone é a base_ab2
base_ab2 as (
select a.*,
b.telefone as telefone
from base_ab22 as a left join baseTelefone as b
on a.cod_chav_coro_clie = b.cpf
),

--base nejNCOR
-- Seleciona base NEJ NCOR retirando a ultima referencia
-- para cumprir o delay de 30 dias entre uma comunicação e outra.
base_nejncor as (
select distinct * from
    workspace_db.nejncor as a
    where codigoproduto != 'codigoproduto'
    and anomes != (select r2 from ref)
),
--base nejbpf
-- Seleciona base NEJ BPF para cruzar com base de outliers
-- seleciona apenas contratos que receberam comunicação (flag_acao = 1)
base_nejbpf as (
select
cast(cprodlim as bigint) as codigoproduto,
cast(contrato as bigint) as contrato,
nome,
cpf_sem_dac as cpf
from database_db_compartilhado_consumer_recupanalyticsiii.POTENCIAL_EXTRAJUD
where flag_acao = 1 and anomes >= 202501
),
-- cruza base outliers com NEJ BPF
-- adiciona flag de recebimento de comunicação NEJ BPF
-- para filtragem na base analitica final 
base_ab3 as (
select a.*,
b.codigoproduto as codProd ,
b.contrato as contra,
b.nome as nom,
b.cpf as cpf2,
case when b.contrato is not null then 1 end as recebNejBPF
from base_ab2 as a
left join base_nejbpf as b
on cast(a.num_cpf_cnpj_sdac as bigint) = cast(b.cpf as bigint) and a.contrato = cast(b.contrato as bigint)
),

--cruza base outliers com NEJ NCOR'
-- adiciona flag de recebimento de comunicação NEJ NCOR
-- para filtragem na base analitica final
-- só usando quem recebeu comunicação NEJ NCOR
base_ab4 as (
select a.*,
    b.codigoproduto as codProd1,
    b.contrato AS numctror1,
--b.contrato as numctror1,
    b.nome as nom1,
    b.cpf as cpf1,
    case when b.contrato is not null then 1 end as recebNejNCOR
from base_ab3 as a
left join base_nejncor as b
    on a.contrato = TRY_CAST(b.contrato AS BIGINT)
),

--Base de Teto de juros CAP VQ
--Seleciona todo historico, cruza com bases de outliers e remove duplicidade, 
--permanecendo apenas a ultima referencia
base_ab5 as (
select a.*,
ROW_NUMBER() OVER(PARTITION BY id_cliente,num_chps ORDER BY dt_rfrc_carg desc) AS ordemCap,
vlr_cap_paci as CAP_VQ,
dt_rfrc_carg as dt_rfrc_carg_VQ
from base_ab4 as a
left join db_corp_REPOSITORIOSDEDADOS_CARTOESDADOSOPERACOES_SPEC_01.tbtm2_moto_cap_ctrt_pf_vq as b
on try_cast(a.chpras as bigint) = b.num_chps
),

-- adiciona flag de ultrapassagem do CAP VQ
-- para filtragem na base analitica final 
-- seleciona apenas a ultima referencia do CAP VQ
base_ab6 as (
select *,
case when CAP_VQ is null then 0
when CAP_VQ = 'Cliente nao atingira o CAP' then 0
else 1 end as CAP_VQ_Ultrapassa
from base_ab5 where ordemCap = 1 or dt_rfrc_carg_VQ is null
)
,

--Base de Teto de juros CAP NPC
--Seleciona todo historico, cruza com bases de outliers e remove duplicidade, 
--permanecendo apenas a ultima referencia
-- adiciona flag de ultrapassagem do CAP NPC
base_ab7 as (
select
a.*,
ROW_NUMBER() OVER(PARTITION BY id_cliente,cpf ORDER BY dt_rfrc_carg desc) AS ordemCapNPC,
vlr_cap_paci as CAP_NPC,
dt_rfrc_carg as dt_rfrc_carg_NPC
from base_ab6 as a
left join db_corp_REPOSITORIOSDEDADOS_CARTOESDADOSOPERACOES_SPEC_01.tbtm2_moto_cap_ctrt_pf_npc as b
on cast(substring(b.cpf,1,9) as bigint) = cast(a.num_cpf_cnpj_sdac as bigint)
),

--Base Analitica
--Remove contratos que estouraram o CAP e contratos que não receberam comunicação de NEJ
-- Seleciona apenas o contrato com maior saldo contabil por CPF
-- Filtra contratos com telefone ou email para contato
baseFinal as (
select distinct * from (
select *,
ROW_NUMBER() OVER(PARTITION BY num_cpf_cnpj_sdac ORDER BY vlr_saldo_contabil desc) AS ordemCPF,
case when CAP_NPC is null then 0
when CAP_NPC = 'Cliente nao vai estourar o CAP' then 0
else 1 end as CAP_NPC_Ultrapassa,
CASE WHEN recebNejBPF = 1 or recebNejNCOR = 1 THEN 1
        else 0
        END AS recebNej
from base_ab7
where ordemCapNPC = 1
or dt_rfrc_carg_NPC is null
and (telefone is not null or Email is not null)
order by recebNej
)
where recebNej = 1 
-- and filtra contratos que não estouraram o CAP NPC e CAP VQ
and CAP_NPC_Ultrapassa = 0 
and CAP_VQ_Ultrapassa = 0 
and ordemCPF = 1
--and (hdg >= 1 and hdg <= 2000)
),

-- seleciona base final e retira contratos cujo a comunicação cartoraria ja ocorreram
-- cruzando com a base de comunicacao cartoraria para evitar duplicidade de comunicação
baseFinal1 as (
select a.*,
-- adiciona zeros à esquerda no CPF para padronização
LPAD(cast(num_cpf_cnpj_sdac as varchar), 9, '0') AS cpf_com_zeros
from baseFinal as a left join
database_db_compartilhado_consumer_recupanalyticsi.comunicacao_cartoraria as b
on cast(a.chpras as varchar) = b.documento_de_origem
-- filtra contratos que ainda não receberam comunicação cartoraria
where b.documento_de_origem is null
),


--codigo para gerar numero de DAC do 
-- CPF para inclusão na base final e envio para a PGMais com CPF completo
base_digits AS (
    SELECT
        cpf_com_zeros,
        CAST(SUBSTRING(cpf_com_zeros, 1, 1) AS INTEGER) * 10 AS d1,
        CAST(SUBSTRING(cpf_com_zeros, 2, 1) AS INTEGER) * 9 AS d2,
        CAST(SUBSTRING(cpf_com_zeros, 3, 1) AS INTEGER) * 8 AS d3,
        CAST(SUBSTRING(cpf_com_zeros, 4, 1) AS INTEGER) * 7 AS d4,
        CAST(SUBSTRING(cpf_com_zeros, 5, 1) AS INTEGER) * 6 AS d5,
        CAST(SUBSTRING(cpf_com_zeros, 6, 1) AS INTEGER) * 5 AS d6,
        CAST(SUBSTRING(cpf_com_zeros, 7, 1) AS INTEGER) * 4 AS d7,
        CAST(SUBSTRING(cpf_com_zeros, 8, 1) AS INTEGER) * 3 AS d8,
        CAST(SUBSTRING(cpf_com_zeros, 9, 1) AS INTEGER) * 2 AS d9
    FROM
        baseFinal1
),

-- codigo para gerar numero de DAC de CPF
first_digit AS (
    SELECT
        cpf_com_zeros,
        CASE
            WHEN (11 - (d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9) % 11) >= 10 THEN 0
            ELSE (11 - (d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9) % 11)
        END AS first_dac
    FROM
        base_digits
),

-- codigo para gerar numero de DAC de CPF
base_digits1 AS (
    SELECT cpf_com_zeros,
           first_dac,
           CAST(SUBSTRING(cpf_com_zeros, 1, 1) AS INTEGER) * 11 AS d1,
           CAST(SUBSTRING(cpf_com_zeros, 2, 1) AS INTEGER) * 10 AS d2,
           CAST(SUBSTRING(cpf_com_zeros, 3, 1) AS INTEGER) * 9 AS d3,
           CAST(SUBSTRING(cpf_com_zeros, 4, 1) AS INTEGER) * 8 AS d4,
           CAST(SUBSTRING(cpf_com_zeros, 5, 1) AS INTEGER) * 7 AS d5,
           CAST(SUBSTRING(cpf_com_zeros, 6, 1) AS INTEGER) * 6 AS d6,
           CAST(SUBSTRING(cpf_com_zeros, 7, 1) AS INTEGER) * 5 AS d7,
           CAST(SUBSTRING(cpf_com_zeros, 8, 1) AS INTEGER) * 4 AS d8,
           CAST(SUBSTRING(cpf_com_zeros, 9, 1) AS INTEGER) * 3 AS d9,
           first_dac * 2 AS d10
    FROM first_digit
),

-- codigo para gerar numero de DAC de CPF
-- concatena CPF com os dois digitos verificadores e gera o CPF completo
second_digit AS (
select cpf_com_zeros,
    concat(cpf_com_zeros,first_dac,second_dac) as cpf_com_dac from (
    SELECT
        a.cpf_com_zeros,
        cast(first_dac as varchar) as first_dac,
        cast(CASE
            WHEN (11 - (d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 + d10) % 11) >= 10 THEN 0
            ELSE (11 - (d1 + d2 + d3 + d4 + d5 + d6 + d7 + d8 + d9 + d10) % 11)
        END as varchar) AS second_dac
    FROM
        base_digits1 as a
)
),
-- gera base final com CPF completo (com DAC) e sem duplicidade de contratos ja comunicados 
-- e pronta para envio à PGMais com o numero de CPF correto onde junta com a base de digitos 
baseFinal2 as (
SELECT distinct
*
-- a baseFinal1 está com os contratos que ainda não receberam comunicação cartoraria
FROM baseFinal1 as a inner join second_digit as b
on a.cpf_com_zeros = b.cpf_com_zeros

)


-- Este é o arquivo final a ser mandado para a PGMais
-- Necessario filtrar HDG entre 1 a 2000 para publico de ação
-- Armazerar publico de controle em arquivo para inserir no S3 para mensuração de resultados
select
-- Adiciona campos fixos conforme layout da PGMais
'1-SOCIO MAJORITARIO' as RELACIONAMENTO,
CASE WHEN nom is null  THEN nom1
        else nom
        END AS RAZAO_SOCIAL,
dat_ulti_atrs as DTA_ATU,
cpf_com_dac as NUM_CPF,
'S' as REPRESENTANTE_LEGAL,
'S' as AVALISTA,
-- Concatena nome completo do cliente, usando nome da base NEJ NCOR caso o nome da base NEJ BPF esteja nulo
CASE WHEN nom is null  THEN nom1
        else nom
        END AS NOME_COMPLETO,
cpf_com_dac as CGCCPF,
Email as EMAIL_CLIENTE,
telefone as TELEFONE_COMPLET,
cpf_com_dac as NUM_CNPJ_RAIZ,
chpras as DOCUMENTO_DE_ORIGEM,
vlr_saldo_gerencial as VALOR_DIVIDA,
-- Adiciona faixa de notificação conforme regra definida
'31 - 40' as NOTIFICA,
dat_ulti_atrs as DATA_VALOR,
-- Adiciona campo de controle para mensuração de resultados de comunicação cartoraria
CASE WHEN hdg BETWEEN 1 AND 2000  THEN 1
        else 0
        END AS JA_RECEBEU,
-- Segmentação por HDG para público de ação e controle 
hdg as HDG_APROX_AVAP,
2 as grupo_APROX_AVAP
from
baseFinal2