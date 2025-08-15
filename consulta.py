import csv
import cx_Oracle
import getpass
import os
from datetime import datetime
from collections import defaultdict




#Função auxiliar para limpar CPF
def clean_cpf(cpf):
    """Padroniza CPF removendo caracteres não numéricos e completando com zeros à esquerda"""
    if cpf is None:
        return ""
    cpf = str(cpf).strip()
    # Remove tudo que não for dígito
    cpf = ''.join(filter(str.isdigit, cpf))
    # Completa com zeros à esquerda se necessário
    return cpf.zfill(11)

#Função auxiliar para obter dados do Oracle Cebi
def get_data_from_oracle(usuario, senha):
    """Consulta o Oracle para obter dados de imóveis, proprietários e usuários"""
    dsn_tns = cx_Oracle.makedsn(server, porta, service_name=banco)
    conn = cx_Oracle.connect(user=usuario, password=senha, dsn=dsn_tns)
    
    cursor = conn.cursor()
    
    query = '''
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
    '''
    
    cursor.execute(query)
    # Obter a descrição das colunas para mapeamento
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Converter para lista de dicionários com nomes de colunas
    return [dict(zip(columns, row)) for row in rows]

def gravar_cpf_log(output_cpf_log, cpf, encontrado, cdc_match, cdc_cadastro_social, cdc_estimado_cadastro_social, cdc, cdc_estimado):
    with open(output_cpf_log, 'a') as log_file:
        log_file.write(f"""
CPF: {cpf}
    Encontrado na base de dados do SAAE: {encontrado if encontrado.lower() != 'sim' else f'sim, CDC: {cdc}'}
    CDC estimado pelo CEP do CadUnico confere com o do SAAE: {cdc_match}
    CDC na base do SAAE com Cadastro Social: {cdc_cadastro_social}
    CDC Estimado fornecido pelo Cadunico com Cadastro Social na base do SAAE: {cdc_estimado_cadastro_social}, CDC_ESTIMADO: {cdc_estimado}
                       
                       """)

def gravar_cadastro_social_saae_log2(output_cadastro_social_saae_log, lista_cadastro_social_saae):
    with open(output_cadastro_social_saae_log, 'a') as log_file:
        for cdc, cadastro_social in lista_cadastro_social_saae.items():
            log_file.write(f"""
CDC: {cdc} - {"Atualizado\n     CPF localizado na base do SAAE com cep e numero corretos com o CADUNICO" if cadastro_social else "Remover\n     CPF do Proprietario/Usuario não fazem parte do Cadastro do CADUNICO"}
                
                """)


def gravar_cadastro_social_saae_log(output_base_path, lista_cadastro_social_saae):
    # Adiciona data/hora ao nome do arquivo
    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_txt = f"{output_base_path}_{data_hora}.txt"
    output_csv = f"{output_base_path}_{data_hora}.csv"
    
    # Escreve no arquivo TXT (como originalmente)
    with open(output_txt, 'a') as log_file:
        for cdc, cadastro_social in lista_cadastro_social_saae.items():
            log_file.write(f"""
                CDC: {cdc} - {"Atualizado\nCPF localizado na base do SAAE com cep e numero corretos com o CADUNICO" if cadastro_social else "Remover\nCPF do Proprietario/Usuario nao fazem parte do Cadastro do CADUNICO"}
                
                """)
    
    # Escreve no arquivo CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csv_file:
        csv_file.write("CDC;STATUS;MENSAGEM\n")  # Cabeçalho
        
        for cdc, cadastro_social in lista_cadastro_social_saae.items():
            status = "Atualizado" if cadastro_social else "Remover"
            mensagem = "CPF localizado na base do SAAE com cep e numero corretos com o CADUNICO" if cadastro_social else "CPF do Proprietario/Usuario nao fazem parte do Cadastro do CADUNICO"
            
            csv_file.write(f"{cdc};{status};{mensagem}\n")

def process_csv_with_oracle_data(csv_file_path, output_file_path_total):
    """Processa o arquivo CSV combinando com dados do Oracle (CPF pode ter múltiplos CDCs)."""
    # --- Entradas de credenciais ---
    inputUsuario = input("Digite o usuário do Banco de Dados: ")
    inputSenha = getpass.getpass('Digite a senha do usuário: ')  

    # --- Obter dados do Oracle ---
    oracle_data = get_data_from_oracle(inputUsuario, inputSenha)

    # --- Estruturas de mapeamento ---
    cpf_to_cdcs = defaultdict(set)   # CPF -> {CDC1, CDC2, ...}
    cdc_to_categoria = {}            # CDC -> categoria
    cdc_count = defaultdict(int)     # CDC -> número de CPFs relacionados (contagem de relacionamentos únicos CPF-CDC)
    cpf_samples_from_oracle = set()  # amostras de CPFs do Oracle
    cep_numero_to_cdc = {}           # (CEP, NUMERO) -> CDC
    cdc_to_cadastro_social = {}      # CDC -> bool (se é categoria 11)

    # --- Estatísticas iniciais (algumas serão calculadas ao final do carregamento) ---
    stats = {
        'total_cpfs_no_csv': 0,
        'cpfs_com_cdc_encontrado': 0,
        'cpfs_sem_cdc_encontrado': 0,
        'cdcs_unicos_encontrados': 0,              # calculado após carregar Oracle
        'total_relacionamentos_cpf_cdc': 0,        # calculado após carregar Oracle
        'cdc_count': cdc_count,
        'cdc_match_count': 0,
        'cdc_cadastro_social_count': 0,
        'cdc_estimado_cadastro_social_count': 0,
        'cep_numero_invalidos': 0,
        'cpfs_nao_encontrados_samples': set(),
        'cpfs_oracle_samples': [],                 # preenchido ao final da carga do Oracle
        'cdcs_estimados_por_cep_numero': 0,
        'cdc_cadastro_social_desatualizado_count': 0,
        'cdc_cadastro_social_correto_count': 0,
        'cdc_incluir_cadastro_social_count': 0
    }

    # --- Carregar mapeamentos a partir do Oracle ---
    for row in oracle_data:
        cdc = row['CDC']
        cpf_proprietario = row['CPF_PROPRIETARIO']
        cpf_usuario = row['CPF_USUARIO']
        cep = row['CEP']
        numero_imovel = row['NUMERO_IMOVEL']
        cod_categoria = row['COD_CATEGORIA']

        # Proprietário
        if cpf_proprietario:
            cpf_proprietario_clean = clean_cpf(cpf_proprietario)
            if len(cpf_proprietario_clean) == 11:
                if cdc not in cpf_to_cdcs[cpf_proprietario_clean]:
                    cpf_to_cdcs[cpf_proprietario_clean].add(cdc)
                    cdc_count[cdc] += 1
                cpf_samples_from_oracle.add(cpf_proprietario_clean)
                cdc_to_categoria[cdc] = cod_categoria

        # Usuário
        if cpf_usuario:
            cpf_usuario_clean = clean_cpf(cpf_usuario)
            if len(cpf_usuario_clean) == 11:
                if cdc not in cpf_to_cdcs[cpf_usuario_clean]:
                    cpf_to_cdcs[cpf_usuario_clean].add(cdc)
                    cdc_count[cdc] += 1
                cpf_samples_from_oracle.add(cpf_usuario_clean)
                cdc_to_categoria[cdc] = cod_categoria

        # (CEP, Número) -> CDC
        if cep and numero_imovel:
            cep_clean = str(cep).strip()
            numero_clean = str(numero_imovel).strip()
            cep_numero_key = (cep_clean, numero_clean)
            cep_numero_to_cdc[cep_numero_key] = cdc

        # Marcar CDCs de categoria 11 como "tem cadastro social"
        if cod_categoria == 11:
            cdc_to_cadastro_social[cdc] = True
        else:
            cdc_to_cadastro_social.setdefault(cdc, False)

    # Estatísticas após carregar Oracle
    todos_cdcs = set()
    total_relacionamentos = 0
    for s in cpf_to_cdcs.values():
        todos_cdcs |= s
        total_relacionamentos += len(s)
    stats['cdcs_unicos_encontrados'] = len(todos_cdcs)
    stats['total_relacionamentos_cpf_cdc'] = total_relacionamentos
    stats['cpfs_oracle_samples'] = list(cpf_samples_from_oracle)[:10]

    # --- Processar CSV de entrada ---
    with open(csv_file_path, mode='r', encoding='utf-8') as csv_file, \
         open(output_file_path_total, mode='w', encoding='utf-8', newline='') as total_file, \
         open(output_match_cdc_csv, mode='w', encoding='utf-8', newline='') as match_file, \
         open(output_cdc_desatualizado, mode='w', encoding='utf-8', newline='') as desatualizado_file, \
         open(output_cdc_estimado, mode='w', encoding='utf-8', newline='') as estimado_file:

        reader = csv.DictReader(csv_file)

        # Cabeçalhos de saída
        fieldnames = [
            'STATUS', 'CDC', 'CPF_ENCONTRADO',
            'CADASTRO SOCIAL', 'CDC_ESTIMADO',
            'CADASTRO SOCIAL ESTIMADO', 'CDC_CONFIRMADO'
        ] + [f for f in reader.fieldnames if f not in ['CDC', 'encontrado', 'CDC_ESTIMADO']]

        total_writer = csv.DictWriter(total_file, fieldnames=fieldnames)
        match_writer = csv.DictWriter(match_file, fieldnames=fieldnames)
        desatualizado_writer = csv.DictWriter(desatualizado_file, fieldnames=fieldnames)
        estimado_writer = csv.DictWriter(estimado_file, fieldnames=fieldnames)

        total_writer.writeheader()
        match_writer.writeheader()
        desatualizado_writer.writeheader()
        estimado_writer.writeheader()

        for row in reader:
            stats['total_cpfs_no_csv'] += 1

            # CPF limpo do CSV
            cpf = clean_cpf(row.get('p.num_cpf_pessoa', ''))

            # CDCs (conjunto) para o CPF
            cdcs_do_cpf = cpf_to_cdcs.get(cpf, set())
            encontrado = 'sim' if cdcs_do_cpf else 'nao'
            #cdc_list_str = '|'.join(sorted(cdcs_do_cpf)) if cdcs_do_cpf else ''
            cdc_list_str = '|'.join(sorted(map(str, cdcs_do_cpf))) if cdcs_do_cpf else ''

            # CEP e número do CSV para estimar CDC
            cep_csv = row.get('d.num_cep_logradouro_fam', '').strip() if 'd.num_cep_logradouro_fam' in row else ''
            numero_csv = row.get('d.num_logradouro_fam', '').strip() if 'd.num_logradouro_fam' in row else ''

            cdc_estimado = ''
            cdc_match = 'nao'
            cdc_cadastro_social = 'nao'
            cdc_estimado_cadastro_social = 'nao'

            # Estimar CDC via (CEP, Número)
            if cep_csv and numero_csv:
                cep_numero_key = (cep_csv, numero_csv)
                cdc_estimado = cep_numero_to_cdc.get(cep_numero_key, '')
                if cdc_estimado:
                    if cdc_estimado in cdcs_do_cpf:
                        stats['cdc_match_count'] += 1
                        cdc_match = 'sim'
                        # marcar como cadastro social se aplicável
                        if cdc_to_categoria.get(cdc_estimado, 0) == 11:
                            cdc_to_cadastro_social[cdc_estimado] = True
                    else:
                        stats['cdcs_estimados_por_cep_numero'] += 1
            else:
                stats['cep_numero_invalidos'] += 1

            # Atualizar estatísticas básicas de encontrado/não
            if cdcs_do_cpf:
                stats['cpfs_com_cdc_encontrado'] += 1
            else:
                stats['cpfs_sem_cdc_encontrado'] += 1
                if len(stats['cpfs_nao_encontrados_samples']) < 10:
                    stats['cpfs_nao_encontrados_samples'].add(cpf)

            # Categorias
            algum_cdc_social = any(cdc_to_categoria.get(c, 0) == 11 for c in cdcs_do_cpf)
            cdc_cadastro_social = 'sim' if algum_cdc_social else 'nao'

            if cdc_estimado:
                cod_cat_cdc_estimado = cdc_to_categoria.get(cdc_estimado, 0)
                cdc_estimado_e_social = (cod_cat_cdc_estimado == 11)
                if cdc_estimado_e_social:
                    cdc_estimado_cadastro_social = 'sim'
                    stats['cdc_estimado_cadastro_social_count'] += 1
            else:
                cdc_estimado_e_social = False

            # Definir STATUS (multi-CDC)
            status = "ANALISAR - NÃO ENCONTRADO"
            if encontrado == 'sim':
                if cdc_estimado:
                    if cdc_estimado in cdcs_do_cpf:
                        if cdc_estimado_e_social:
                            status = "CADASTRO SOCIAL CORRETO"
                            stats['cdc_cadastro_social_correto_count'] += 1
                        else:
                            status = "INCLUIR CDC NO CADASTRO SOCIAL"
                            stats['cdc_incluir_cadastro_social_count'] += 1
                    else:
                        status = "CPF ENCONTRADO. POREM ENDERECO DESATUALIZADO"
                        if algum_cdc_social:
                            stats['cdc_cadastro_social_desatualizado_count'] += 1
                else:
                    status = "CPF ENCONTRADO. SEM ESTIMATIVA DE ENDERECO"
            else:
                if cdc_estimado:
                    if cdc_estimado_e_social:
                        status = "ANALISAR - CPF NAO LOCALIZADO. POREM CDC_ESTIMADO JA POSSUI CADASTRO SOCIAL DO SAAE"
                    else:
                        status = "CPF NAO LOCALIZADO. CDC_ESTIMADO NAO POSSUI CADASTRO SOCIAL DO SAAE"

            # Nova linha de saída
            new_row = {
                'STATUS': status,
                'CDC': cdc_list_str,  # todos os CDCs do CPF (se houver)
                'CPF_ENCONTRADO': encontrado,
                'CADASTRO SOCIAL': cdc_cadastro_social,
                'CDC_ESTIMADO': cdc_estimado,
                'CADASTRO SOCIAL ESTIMADO': cdc_estimado_cadastro_social,
                'CDC_CONFIRMADO': cdc_match
            }
            for field in reader.fieldnames:
                new_row[field] = row.get(field, '')

            # Escrever arquivos
            total_writer.writerow(new_row)
            if cdc_match == 'sim':
                match_writer.writerow(new_row)
            if cdcs_do_cpf and cdc_estimado and (cdc_estimado not in cdcs_do_cpf):
                desatualizado_writer.writerow(new_row)
            if not cdcs_do_cpf and cdc_estimado:
                estimado_writer.writerow(new_row)

            # Log por CPF
            gravar_cpf_log(
                output_cpf_log,
                cpf,
                encontrado,
                cdc_match,
                cdc_cadastro_social,
                cdc_estimado_cadastro_social,
                cdc_list_str,
                cdc_estimado
            )

    # --- Geração do LOG detalhado ---
    log_file_path = output_file_path_total.replace('.csv', '_log.txt')
    with open(log_file_path, 'w', encoding='utf-8') as log_file:
        log_file.write(f"=== LOG DE PROCESSAMENTO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
        log_file.write(f"Arquivo de entrada: {csv_file_path}\n")
        log_file.write(f"Arquivo de saída: {output_file_path_total}\n\n")
        log_file.write(f"Arquivo de saída com Match SAAE e Cadunico: {output_match_cdc_csv}\n")
        log_file.write(f"Arquivo de saída com CDCs desatualizados: {output_cdc_desatualizado}\n")
        log_file.write(f"Arquivo de saída com CDCs estimados: {output_cdc_estimado}\n\n")
        log_file.write(f"Arquivo de log de CPFs: {output_cpf_log}\n\n")

        log_file.write("=== ESTATÍSTICAS GERAIS ===\n")
        log_file.write(f"Total de CPFs processados no CSV: {stats['total_cpfs_no_csv']}\n")
        if stats['total_cpfs_no_csv']:
            log_file.write(f"CPFs com CDC encontrado: {stats['cpfs_com_cdc_encontrado']} ({stats['cpfs_com_cdc_encontrado']/stats['total_cpfs_no_csv']:.2%})\n")
            log_file.write(f"CPFs sem CDC encontrado: {stats['cpfs_sem_cdc_encontrado']} ({stats['cpfs_sem_cdc_encontrado']/stats['total_cpfs_no_csv']:.2%})\n")
        else:
            log_file.write("CPFs com CDC encontrado: 0 (0.00%)\n")
            log_file.write("CPFs sem CDC encontrado: 0 (0.00%)\n")

        log_file.write(f"CDC Match SAAE e Cadunico: {stats['cdc_match_count']}\n")
        log_file.write(f"CDCs únicos encontrados: {stats['cdcs_unicos_encontrados']}\n")
        log_file.write(f"Total de relacionamentos CPF-CDC encontrados: {stats['total_relacionamentos_cpf_cdc']}\n")
        log_file.write(f"CDCs estimados por CEP+NÚMERO: {stats['cdcs_estimados_por_cep_numero']}\n\n")

        log_file.write("=== AMOSTRAS PARA DIAGNÓSTICO ===\n")
        log_file.write("\n10 CPFs encontrados no Oracle (amostra):\n")
        for cpf in stats['cpfs_oracle_samples']:
            log_file.write(f"{cpf}\n")

        log_file.write("\n10 CPFs do CSV não encontrados no Oracle (amostra):\n")
        for cpf in stats['cpfs_nao_encontrados_samples']:
            log_file.write(f"{cpf}\n")

        log_file.write("\n=== DISTRIBUIÇÃO DE CPFs POR CDC ===\n")
        for cdc, count in sorted(stats['cdc_count'].items(), key=lambda x: x[1], reverse=True)[:20]:
            log_file.write(f"CDC {cdc}: {count} CPF(s) associado(s)\n")

    # --- Log da base SAAE (cadastro social por CDC) ---
    gravar_cadastro_social_saae_log(output_base_saae_social, cdc_to_cadastro_social)

    # --- Resumo no console ---
    print(f"\n=== RESUMO ESTATÍSTICO ===")
    print(f"Total de CPFs processados: {stats['total_cpfs_no_csv']}")
    if stats['total_cpfs_no_csv']:
        print(f"CPFs com CDC encontrado: {stats['cpfs_com_cdc_encontrado']} ({stats['cpfs_com_cdc_encontrado']/stats['total_cpfs_no_csv']:.2%})")
        print(f"CPFs sem CDC encontrado: {stats['cpfs_sem_cdc_encontrado']} ({stats['cpfs_sem_cdc_encontrado']/stats['total_cpfs_no_csv']:.2%})")
    else:
        print("CPFs com CDC encontrado: 0 (0.00%)")
        print("CPFs sem CDC encontrado: 0 (0.00%)")
    print(f"CDC Match SAAE e Cadunico: {stats['cdc_match_count']}")
    print(f"Total de CDC Estimados com cadastro social: {stats['cdc_estimado_cadastro_social_count']}")
    print(f"CDCs únicos encontrados: {stats['cdcs_unicos_encontrados']}")
    print(f"CDCs estimados por CEP+NÚMERO: {stats['cdcs_estimados_por_cep_numero']}")
    print(f"\nArquivo processado com sucesso. Resultado salvo em: {output_file_path_total}")
    print(f"Arquivo de log detalhado gerado em: {log_file_path}")
    print(f"Arquivo de log pelo CPF gerado em: {output_cpf_log}")

    # Sugestões de diagnóstico
    if stats['cpfs_com_cdc_encontrado'] == 0:
        print("\n=== SUGESTÕES PARA DIAGNÓSTICO ===")
        print("1. Verifique se os CPFs do CSV e do Oracle estão no mesmo formato")
        print("2. Confira se a consulta ao Oracle está retornando os CPFs esperados")
        print("3. Compare manualmente alguns CPFs das amostras no log")
        print("4. Verifique se há CPFs nulos/vazios na base Oracle")


   

def atualizarCaminho(caminho):
    datahora = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Cria o diretório se não existir
    os.makedirs(datahora, exist_ok=True)
    # Retorna o novo caminho completo
    return os.path.join(datahora, caminho)

if __name__ == "__main__":
    # nome dos arquivos de saida
    input_csv = 'cadunico.csv'
    output_cpf_log = atualizarCaminho('cpf_log.txt')
    output_csv = atualizarCaminho('dados_com_cdc_total.csv')
    output_match_cdc_csv = atualizarCaminho('cdc_confirmados.csv')
    output_cdc_desatualizado = atualizarCaminho('cpf_com_cdc_desatualizados.csv')
    output_cdc_estimado = atualizarCaminho('cpf_com_cdc_estimado_pelo_cep.csv')
    output_base_saae_social = atualizarCaminho('base_saae_social')
    server = '192.168.3.250'
    porta = '1521'
    banco = 'ORCL'
    #Chamada da função principal para processar o CSV
    process_csv_with_oracle_data(input_csv, output_csv)