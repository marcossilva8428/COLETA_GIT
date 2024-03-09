import pyodbc
import configparser

config = configparser.ConfigParser()
config.read('C:\\automacoes\\conf\\conf.ini')

def conectaBanco():

    """
        Essa função responsavel por estabelecer conexão com bando de dados.

    Args:
        Não recebe argumento

    Returns:
        Retorna o cursor para realização da consulta.

    Raises:
        ValueError: Falha caso banco não conecte, caso o banco esteja offline ou então caso os dados de acesso estejam incorretos
    
    """
    # Informações de conexão a partir do arquivo .ini
    user = config.get('BANCO_ALGAR', 'USUARIO')
    password = config.get('BANCO_ALGAR', 'SENHA')
    server = config.get('BANCO_ALGAR', 'IP')
    database = config.get('BANCO_ALGAR', 'SID')

    connection = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={user};'
        f'PWD={password};'
    )

    cursor = connection.cursor()

    return cursor

