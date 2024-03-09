import configparser
import conecta as conn
import pandas as pd
import re

config = configparser.ConfigParser()
config.read('C:\\automacoes\\conf\\conf.ini')
protocolo = ''
id_acao = ''
id_bd = 0
obs = ''

def selectCountAgtFilaTrat():

    """
        Essa função serve para fazer um count na tabela AG_Tabela_TRAT

    Args:
        argumento: não recebem argumento

    Returns:
        O resultado e estabelecer um paramento para um laço e se a execução deve ser realizada

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

    select = config.get('SELECIONA_CONTAGEM', 'SELECT')
    subselect1 = config.get('SELECIONA_CONTAGEM', 'SUBSELECT1')
    subselect2 = config.get('SELECIONA_CONTAGEM', 'SUBSELECT2')
    join = config.get('SELECIONA_CONTAGEM', 'JOIN')
    condicao = config.get('SELECIONA_CONTAGEM', 'CONDICAO')
    consulta = select+' '+subselect1+' '+subselect2+' '+join+' '+condicao
    
    cursor = conn.conectaBanco()
    cursor.execute(consulta)

    # Recupere os resultados
    for row in cursor.fetchall():
       validacao = row[0]

    cursor.commit()
    cursor.close()   
  
    return validacao

def selectAgtFilaTrat():

    """
        Essa função serve para fazer um select na tabela AG_Tabela_TRAT

    Args:
        argumento: variavel contador que será usada para selecionar qual elemento da tabela tratar

    Returns:
        O resultado é um dataframe que servirá seus dados para realização da consulta no SOM

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

    select = config.get('SELECIONA_DADOS', 'SELECT')
    subselect1 = config.get('SELECIONA_DADOS', 'SUBSELECT1')
    subselect2 = config.get('SELECIONA_DADOS', 'SUBSELECT2')
    join = config.get('SELECIONA_DADOS', 'JOIN')
    condicao = config.get('SELECIONA_DADOS', 'CONDICAO')
    consulta = select +' '+subselect1+' '+subselect2+' '+join+' '+condicao 
    
    cursor = conn.conectaBanco()
    cursor.execute(consulta)

    df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
   

    cursor.commit()
    cursor.close()

    return df

def selectAgvTransbordoSom():

    """
        Essa função serve para fazer um select na tabela AG_V_Tabela_OS_SOM

    Args:
        argumento: variavel contador que será usada para selecionar qual elemento da tabela tratar

    Returns:
        O resultado é um dataframe que servirá seus dados para realização da consulta no SOM

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

    select = config.get('SELECIONA_DADOS_SOM', 'SELECT')
    consulta = select
    
    cursor = conn.conectaBanco()
    cursor.execute(consulta)

    df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
   

    cursor.commit()
    cursor.close()

    return df

def updateTransbordo(id_bd):
    """
        Essa função serve para fazer um UPDATE da coluna ID_STATUS na tabela AG_Tabela_OS

    Args:
        argumento: recebe id_bd para selecionar o valor correto para atualizar

    Returns:
        O resultado e estabelecer um paramento para um laço e se a execução deve ser realizada

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

# Informações de conexão a partir do arquivo .ini
    update = config.get('FAZ_UPDATE_TRANSBORDO', 'UPDATE')
    update = update.replace("{id_bd}",f'{id_bd}')

    cursor = conn.conectaBanco()
    cursor.execute(update)

    cursor.commit()
    cursor.close()

def updateDataReserva(id_bd):
    """
        Essa função serve para fazer um UPDATE da coluna DATA_HORA_RESERVA na tabela AG_Tabela_TRAT

    Args:
        argumento: recebe id_bd para selecionar o valor correto para atualizar

    Returns:
        O resultado e estabelecer um paramento para um laço e se a execução deve ser realizada

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

# Informações de conexão a partir do arquivo .ini
    update = config.get('FAZ_UPDATE', 'UPDATE')
    update = update.replace("{id_bd}",f'{id_bd}')

    cursor = conn.conectaBanco()
    cursor.execute(update)

    cursor.commit()
    cursor.close()


def updateTentativa(id_bd):
    """
        Essa função serve para fazer um UPDATE da coluna TENTATIVAS na tabela AG_Tabela_TRAT 

    Args:
        argumento: recebe o numero da linha do protocolo que será atualizado na função selectAgtFilaTrat

    Returns:
        Sem retorno

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

# Informações de conexão a partir do arquivo .ini
    update = config.get('FAZ_UPDATE_TENTATIVAS', 'UPDATE')
    update = update.replace("{ID_BD}",f'{id_bd}')
    select = config.get('FAZ_UPDATE_TENTATIVAS', 'SELECT')
    select = select.replace("{ID_BD}",f'{id_bd}')
    update = update
    select = select
  
    cursor = conn.conectaBanco()
    cursor.execute(update)
    
    cursor.execute(select)
    df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])

    tentativas =df['TENTATIVAS'].iloc[0]
    tentativas = int(tentativas)

    cursor.commit()
    cursor.close()

    return tentativas


def updateDelete(protocolo,id_acao,id_bd,tentativas):
    """
        Essa função serve para fazer um UPDATE da coluna TENTATIVAS na tabela AG_Tabela_TRAT e também DELETE em caso de falha.

    Args:
        argumento: recebe o numero da linha do protocolo que será atualizado na função selectAgtFilaTrat

    Returns:
        Sem retorno

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

# Informações de conexão a partir do arquivo .ini
    # Condicionais
    
    if  id_acao == 5:
        id_status = 105
    elif id_acao == 13:
        id_status = 124
    elif id_acao == 6: 
        id_status = 108
    elif id_acao == 10:
        id_status = 109
    elif id_acao == 8:
        id_status = 107
    elif id_acao == 7:
        id_status = 106
    
    delete = config.get('FAZ_DELETE_E_UPDATE', 'DELETE')
    delete = delete.replace("{ID_BD}",f'{id_bd}')

    update = config.get('FAZ_DELETE_E_UPDATE', 'UPDATE')
    update = update.replace("{ID_STATUS}",f'{id_status}')
    update = update.replace("{ID_BD}",f'{id_bd}')
  
    cursor = conn.conectaBanco()
    cursor.execute(delete)
    cursor.execute(update)

    protocolo_atualizado = f'Protocolo {protocolo} removido da tabela AG_Tabela_FILA_TRA pois ultrapassou {tentativas} tentativas de tratamento. Atualizado com ID_STATUS {id_status} na tabela AG_Tabela_OS'

    cursor.commit()
    cursor.close()

    return protocolo_atualizado

def Delete(id_bd,protocolo):
    """
        Essa função serve para fazer um DELETE na tabela AG_Tabela_TRAT 

    Args:
        argumento: recebe o numero da linha do protocolo que será atualizado na função selectAgtFilaTrat

    Returns:
        Sem retorno

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """
   
    delete = config.get('FAZ_DELETE', 'DELETE')
    delete = delete.replace("{ID_BD}",f'{id_bd}')

  
    cursor = conn.conectaBanco()
    cursor.execute(delete)

    protocolo_atualizado = f'Protocolo {protocolo} removido da tabela AG_Tabela_TRAT  pois foi executado com sucesso'

    cursor.commit()
    cursor.close()

    return protocolo_atualizado


def DeleteSemOs(id_bd,protocolo):
    """
        Essa função serve para fazer um DELETE na tabela AG_Tabela_TRAT  

    Args:
        argumento: recebe o numero da linha do protocolo que será atualizado na função selectAgtFilaTrat

    Returns:
        Sem retorno

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

   
    delete = config.get('FAZ_DELETE_E_UPDATE', 'DELETE')
    delete = delete.replace("{ID_BD}",f'{id_bd}')

  
    cursor = conn.conectaBanco()
    cursor.execute(delete)

    protocolo_atualizado = f'Protocolo {protocolo} removido da tabela AG_Tabela_TRAT  pois não foi foi localizada no SOM'

    cursor.commit()
    cursor.close()

    return protocolo_atualizado

def selectTabelaAcessoSom():

    """
        Essa função serve para fazer select na tabela AG_Tabela_ACESSOS e coletar informações como senha, id_acao, login, etc.

    Args:
        argumento: não recebe argumentos

    Returns:
        O resultado retorna df

    Raises:
        ValueError: Se dados de acesso estiverem errado ou bloqueado.
    
    """

    select = config.get('SELECIONA_ACESSOS_SOM', 'SELECT')
    consulta = select
    
    cursor = conn.conectaBanco()
    cursor.execute(consulta)

    column_names = [column[0] for column in cursor.description]

    for row in cursor.fetchall():
        dados = dict(zip(column_names, row))

    cursor.commit()
    cursor.close()   
  
    return dados


def buscaInsert():
    insert = config.get('FAZ_INSERT', 'INSERT')
    cursor = conn.conectaBanco()
    cursor.execute(insert)
    #insert = cursor.fetchall()
    
    df = pd.DataFrame.from_records(cursor.fetchall(),
        columns = [desc[0] for desc in cursor.description])
    
    return df



def fazInsert(df_final):
    insert1 = buscaInsert()
    cursor = conn.conectaBanco()
    insert = insert1['INSERT1'][0]
    

    for index, row in df_final.iterrows():
        insert = insert.replace("{row}",f'{row}')
        try:
            cursor.execute(f'{insert}')
        except Exception as e:
            print(f"Erro ao executar o INSERT: {e}")
    
    cursor.commit()
    cursor.close()


def maxIdLote():
    """
        Essa função serve para pegar o MAX(ID_LOTE) na tabela AG_Tabela_OS.

    Args:
        Não recebe argumento

    Returns:
        O resultado o resultado do MAX(ID_LOTE).

    Raises:
        ValueError: Falha caso banco não conecte
    
    """
    select = config.get('SELECIONA_MAX_ID_LOTE','SELECT')
    cursor = conn.conectaBanco()
    cursor.execute(select)

    df = pd.DataFrame.from_records(cursor.fetchall(),
        columns = [desc[0] for desc in cursor.description])
    
    if df['ID_LOTE'][0] == None:
        df['ID_LOTE'][0] = 1
    
    
    return df['ID_LOTE'][0]

def chavesApi(chave):
    """
        Essa função serve para retornar as credenciais de acesso a API algar

    Args:
        Recebe uma string com o nome da chave solicitada

    Returns:
        Retorna a chave solicitada

    Raises:
        ValueError: Se o argumento for inválido, verificar campos de entrada dos elementos
    
    """

    if chave == 'CLIENT_ID':
        select = config.get('SELECIONA_CHAVE', 'SELECT')
        select = select.replace("{CHAVE}",f'{chave}')
        cursor = conn.conectaBanco()
        cursor.execute(select)
        
        df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
        
        return df['CLIENT_ID'][0]
    elif chave == 'CLIENT_S':
        select = config.get('SELECIONA_CHAVE', 'SELECT')
        select = select.replace("{CHAVE}",f'{chave}')
        cursor = conn.conectaBanco()
        cursor.execute(select)
        
        df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
        
        return df['CLIENT_S'][0]
    elif chave == 'BASE64':
        select = config.get('SELECIONA_CHAVE', 'SELECT')
        select = select.replace("{CHAVE}",f'{chave}')
        cursor = conn.conectaBanco()
        cursor.execute(select)
        
        df = pd.DataFrame.from_records(cursor.fetchall(),
            columns = [desc[0] for desc in cursor.description])
        
        return df['BASE64'][0]
    


def fazMerge(df_final):
    """
        Essa função serve para fazer MERGE/INSERT na tabela AG_Tabela_OS.

    Args:
        argumento: Recebe o df_final (data_frame) e insere atraves deste merge no banco. Insere tanto as colunas do DF quando valores vazios ou zero.

    Returns:
        O resultado retorna uma lista (merge_commands)

    Raises:
        ValueError: Falha caso banco não conecte
    
    """
    merge_commands = []
    for index, row in df_final.iterrows():
        merge_command = f"""
            MERGE INTO AG_T_OS AS Target
            USING (VALUES ({row['PROTOCOLO']}, '{row['ID_OS']}')) AS Source(PROTOCOLO, ID_OS)
            ON Target.PROTOCOLO = Source.PROTOCOLO AND Target.ID_OS = Source.ID_OS
            WHEN MATCHED THEN
                UPDATE SET
                    DATA_ATUALIZACAO = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT
                (
                    COLUNA1,
                    COLUNA2,
                    COLUNA3,
                    COLUNA4,
                    COLUNA5,
                    COLUNA6,
                    COLUNA7,
                    COLUNA8,
                    COLUNA9,
                    COLUNA10,
                    COLUNA11,
                    COLUNA12,
                    COLUNA13,
                    COLUNA14,
                    COLUNA15,
                    COLUNA16,
                    COLUNA17,
                    COLUNA18,
                    COLUNA19,
                    COLUNA20,
                    COLUNA21,
                    COLUNA22,
                    COLUNA23,
                    COLUNA24,
                    COLUNA25,
                    COLUNA26,
                    COLUNA27,
                    COLUNA28,
                    COLUNA29,
                    COLUNA30,
                    COLUNA31,
                    COLUNA32,
                    COLUNA33

                )
                VALUES 
                (
                    (SELECT ISNULL(MAX(ID_BD)+1, 1) FROM AG_Tabela_OS),
                    '{2}',
                    GETDATE(),
                    GETDATE(),
                    GETDATE(),
                    GETDATE(),
                    '{row['COLUNA7']}',
                    'DADOS',
                    '{0}',
                    '{0}',
                    '{row['COLUNA11']}',
                    '{row['COLUNA12']}',
                    '',
                    '0',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '{row['COLUNA20']}',
                    '',
                    '{row['COLUNA22']}',
                    '',
                    '',
                    '{row['COLUNA25']}',
                    '{row['COLUNA26']}',
                    '{row['COLUNA27']}',
                    NULL,
                    '',
                    '',
                    '',
                    '',
                    ''
                    );
        """ 
        merge_commands.append(merge_command)
        
    
    return merge_commands
