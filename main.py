import conecta as conn
import pandas as pd
from fazSelect import fazMerge,maxIdLote,chavesApi
from apiSom import getgrant_code,get_access_token,coletaDetalhe,coletaListarOsPrd


if __name__ == '__main__':
    
    try:
        
        base64 = chavesApi('BASE64')
        clientId = chavesApi('CLIENT_ID')
        clientS = chavesApi('CLIENT_S')
        
        if base64 is None or clientId is None or clientS is None:
            raise ValueError("Falha ao gerar dados de acesso a API na função : chavesApi")

        grantCode = getgrant_code(base64,clientId)
        accessToken = get_access_token(base64, grantCode)
        
        df = coletaDetalhe(clientId,clientS,accessToken)

        if grantCode is None or accessToken is None or df is None:
            raise ValueError("Falha ao obter resultado em alguma das funções : [getgrant_code][get_access_token][coletaDetalhe]")
        elif df.empty:
            raise ValueError("O resultado da [coletaDetalhe] está vazio. Sem coleta para realizar.")
        
        df1 = pd.DataFrame()

        for protocolo in df['PROTOCOLO']:
            consulta = coletaListarOsPrd(clientId,clientS,accessToken,protocolo)
            df1 = df1.append(consulta, ignore_index=True)
            if consulta is None or df1.empty:
                raise ValueError("Possivel Falha ao obter resultado na função : [coletaListarOsPrd] \n Ou \n Possivel falha ao gerar df1.append")
            
        df_final = pd.merge(df, df1, on='PROTOCOLO')
        df_final['ID_LOTE'] = maxIdLote()
        merge_commands = []
        merge_commands = fazMerge(df_final)

        cursor = conn.conectaBanco()
    
        for merge_command in merge_commands:
            cursor.execute(merge_command)

        cursor.commit()
        cursor.close()
    
    except Exception as e:

        print(f"Erro: \n {e}")

    
