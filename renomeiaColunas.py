

def renomeiaColunasApiDetalhe(df):
    df = df.rename(columns={"COLUNA1" : "COLUNA1_NOVO_NOME",
                            'COLUNA2' : "COLUNA2_NOVO_NOME",
                            'COLUNA3' : "COLUNA3_NOVO_NOME"})
    
    # Copiando DATA_HORA_ABERTURA para SOLICITACAO
    df["SOLICITACAO"] = df["DATA_HORA_ABERTURA"].copy()
    df["SIGLA_REGIONAL"] = df["LOCALIDADE"].copy()
    
    return df

def renomeiaColunasApiList(df1):
    tamanho_fixo = 30
    df1['COLUNA'] = df1['COLUNA'].str.slice(0, tamanho_fixo)
    df1['COLUNA'] = df1['COLUNA'].str.slice(0, 5)

    df1 = df1.rename(columns={'COLUNA' : "COLUNA1_NOVO_NOME",
                             'COLUNA2' : "COLUNA2_NOVO_NOME",
                             'COLUNA3' : "COLUNA3_NOVO_NOME"})
    
    # Copiando colunas
    df1["COLUNA3"] = df1["COLUNA_COPIADA1"].copy()
    df1["COLUNA2"] = df1["COLUNA_COPIADA2"].copy()
    df1["COLUNA3"] = df1["COLUNA_COPIADA3"].copy()

    return df1