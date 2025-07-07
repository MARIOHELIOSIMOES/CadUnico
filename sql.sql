WITH 
        proprietarios_usuarios AS (
            SELECT 
                pr.pro_imv_cod AS cdc,
                pr.pro_nom_pro AS usuario,
                pr.pro_reg AS RG_USUARIO,
                NVL(pr.pro_cnpj,pr.pro_cic) AS CPF_USUARIO,
                pr.pro_tel AS TELEFONE_USUARIO,
                pr.pro_ema AS EMAIL_USUARIO,
                pr.pro_ano_ref_ini * 100 + pr.pro_mes_ref_ini AS PERIODO_INICIO_USUARIO,
                pr.pro_ano_ref_fim * 100 + pr.pro_mes_ref_fim AS PERIODO_FIM_USUARIO
            FROM proprietarios pr
        ),
        categorias_ativas AS (
            SELECT 
                cl.ctl_lig_imv_cod AS cdc,
                cl.ctl_cat_cod AS COD,
                cl.ctl_cat_cod||' - '||hc.hct_dsc AS CATEGORIA,
                cl.ctl_nro_eco AS ECONOMIAS     
            FROM categorias_ligacaos cl
            JOIN historicos_categorias hc ON hc.hct_cod = cl.ctl_cat_cod
        )
    SELECT 
        l.lig_imv_cod AS CDC,
        l.lig_ins_lig AS IDENTIFICACAO_CRU,
        i.imv_inc_mun AS NUMERO_IPTU,
        lo.lgd_nom AS RUA_IMOVEL,
        i.imv_num AS NUMERO_IMOVEL,
        b.bai_nom AS BAIRRO,
        i.imv_cep AS CEP,
        cat.COD AS COD_CATEGORIA,
        cat.CATEGORIA AS CATEGORIA_IMOVEL,
        a.atv_cod||' - '||a.atv_dsc AS ATIVIDADE_COMERCIAL,
        cat.ECONOMIAS AS QUANTIDADE_ECONOMIAS,
        c.cvl_dsc AS DESCRICAO_CAVALETE,
        pi.dsc_pad_imv AS PADRAO_IMOVEL,
        i.imv_nom_pro AS NOME_PROPRIETARIO,
        i.imv_cic_pro AS CPF_PROPRIETARIO,
        usu.usuario AS NOME_USUARIO,
        usu.CPF_USUARIO AS CPF_USUARIO,
        usu.PERIODO_INICIO_USUARIO AS PERIODO_INICIO_USUARIO,
        usu.PERIODO_FIM_USUARIO AS PERIODO_FIM_USUARIO,
        DECODE(l.lig_cod_exc,'S','INATIVA','ATIVA') AS STATUS_LIGACAO,
        DECODE(l.lig_sag_cod,'1','COM AGUA','4','CORTADA PEDIDO','5','CORTADA INADIMP','6','PROVISORIA','8','SEM AGUA') AS STATUS_AGUA,
        DECODE(l.lig_ses_cod,'1','COM ESGOTO','SEM ESGOTO') AS STATUS_ESGOTO
    FROM ligacaos l
    LEFT JOIN imovels i ON l.lig_imv_cod = i.imv_cod
    LEFT JOIN logradouros lo ON i.imv_lgd_cod = lo.lgd_cod
    LEFT JOIN bairros b ON i.imv_bai_cod = b.bai_cod
    LEFT JOIN categorias_ativas cat ON l.lig_imv_cod = cat.cdc
    LEFT JOIN atividades a ON l.lig_atv_cod = a.atv_cod
    LEFT JOIN cavaletes c ON l.lig_cav = c.cvl_cod
    LEFT JOIN padrao_imovel pi ON l.lig_cod_pad_imv = pi.cod_pad_imv
    LEFT JOIN proprietarios_usuarios usu ON l.lig_imv_cod = usu.cdc
    ORDER BY CDC