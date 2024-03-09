import ast
import json
import requests
from pandas import json_normalize
from converteFormatoPdDf import colunaStrEmData
from renomeiaColunas import renomeiaColunasApiDetalhe as renomeiaDetalhes, renomeiaColunasApiList as renomeiaApi
   
def get_access_token(BASE64, grant_code: str):
        
        """
            Essa função serve para gerar o access token da API do cliente.

        Args:
            Recebe BASE64 e grant_code

        Returns:
            Retorna o o access token

        Raises:
            ValueError: Falha caso haja falha na comunicação com a api ou os parametros não estejam mais corretos.
    
        """
    
        print("\nRequisitando access_token...")
        link = 'https://LINK.LINK.com.br/oauth/access-token'
        options = {
            'Authorization': BASE64,
            'Content-type': 'application/json'
        }
        data = {"grant_type": "authorization_code",
                "code": grant_code,
                "refresh_token": 'null'
                }
        r = fazer_post_get(link,data,options)
        if r is not None:
            print(str(r.status_code)) 
            print(r.content)
        else:
             return None
        
        if r.status_code >= 200 and r.status_code <= 399:
            byte_str = r.content
            dict_str = byte_str.decode("UTF-8")
            my_data = ast.literal_eval(dict_str)
            access_token = my_data.get("access_token")
            return access_token
        else:
            return None


def fazer_post_get(link,data,options):
     
     try:
         if data is not None:
            r = requests.post(link, data=json.dumps(data), verify=False, headers=options)
            r.raise_for_status()
            return r
         else:
              r = requests.get(link,verify=False, headers=options)
              r.raise_for_status()
              return r
     except Exception as e:
          print(f"Erro durante a solicitação: {e}")
          return None
     
def getgrant_code(BASE64,CLIENT_ID):

        """
            Essa função serve para gerar o grant code da API do cliente.

        Args:
            Recebe BASE64 e CLIENT_ID

        Returns:
            Retorna o grant code

        Raises:
            ValueError: Falha caso haja falha na comunicação com a api ou os parametros não estejam mais corretos.
    
        """

        print("\nRequisitando grant_code...")
        link = 'https://LINK.LINK.com.br/oauth/grant-code'
        options = {
            'Authorization': BASE64,
            'Content-type': 'application/json'
        }

        data = {"client_id": CLIENT_ID,
                "redirect_uri": "http://localhost"
                }

        r = fazer_post_get(link,data,options)
        if r is not None:
            print(str(r.status_code)) 
            print(r.content)
        else:
             return None

        if r.status_code >= 200 and r.status_code <= 399:
            grant_code = str(r.content)
            grant_code = grant_code.replace(
                """b'{"redirect_uri":"http://localhost/?code=""", "")
            grant_code = grant_code.replace(""""}'""", "")
            return grant_code
        else:
            return None


def coletaDetalhe(CLIENT_ID,CLIENT_S,access_token):
        
        """
            Essa função serve para coletar os's na api de detalhes do cliente.

        Args:
            Recebe CLIENT_ID,CLIENT_S e access_token

        Returns:
            Retorna um DataFrame com as O's

        Raises:
            ValueError: Falha caso haja falha na comunicação com a api ou os parametros não estejam mais corretos.
    
        """
        
        print("\nColetando OS...coletaDetalhe")
        options = {
            "CLIENT_ID": CLIENT_ID,
            "APPLICATION_ID": CLIENT_S,
            "access_token": access_token,
            "Content-type": "application/json"
        }

        
        link = 'https://LINK.LINK.com.br/telecom/service-ordering-management/order/v1/orders-service/status-open'

        data = {
            "service": "EXEMPLO DE SERVIÇO",
            "status": "ABERTO",
            "pageSize": 9999,
            "pageNumber": 1
                }

        try:
            
            r = fazer_post_get(link,data,options)
            info = json.loads(r.content)
            df = json_normalize(info)
            colunas = ['COLUNADATA','COLUNAVENCIMENTO']
            df = colunaStrEmData(df,colunas)
            df = df[df['COLUNA_ATIVIDADE'].isin(['SERVICO1', 'SERVICO3'])] #FILTRO
            df = df[['COLUNA1','COLUNA2','COLUNA3','COLUNA4','COLUNA5','COLUNA6','COLUNA7']]
            df = renomeiaDetalhes(df)
            
            if r is not None or info is not None or df is not None or colunas is not None:
                print(str(r.status_code))  
                print(r.content)
            else:
             return None
    
            if not (r.status_code >= 200 and r.status_code <= 399):
                raise Exception(f"Erro ao fazer coleta.")
            else:
                print("Inserir OS: \33[104m"+str(r.status_code))
                return df#df_do_dia
        except Exception as e:
            print(e)

def coletaListarOsPrd(CLIENT_ID,CLIENT_S,access_token,i):
        """
            Essa função serve para coletar os's na api simplificada do cliente.

        Args:
            Recebe CLIENT_ID,CLIENT_S,access_token e protocolo

        Returns:
            Retorna um DataFrame com as O's

        Raises:
            ValueError: Falha caso haja falha na comunicação com a api ou os parametros não estejam mais corretos.
    
        """
        print("\nColetando OS...coletaListarOsPrd")
        options = {
            "CLIENT_ID": CLIENT_ID,
            "APPLICATION_ID": CLIENT_S,
            "access_token": access_token,
            "Content-type": "application/json"
        }

        
        link= f'https://LINK.LINK.com.br/telecom/service-ordering-management/order/v1/detailed-orders/LINK-LINK/{i}'
        

        try:
            r = fazer_post_get(link,None,options)
            print(str(r.status_code))
            info = json.loads(r.content)
         
            df1 = json_normalize(info)
            print(df1['protocol'])
            colunas_desejadas = ['COL1','COL2','COL3','COL4','COL5','ORDEM_SERVICO']
            
            link_listar_telefone= f'https://LINK.LINK.com.br/telecom/service-ordering-management/order/v1/orders/{df1["ORDEM_SERVICO"][0]}/CONTATO'
            r = fazer_post_get(link_listar_telefone,None,options)
            info = json.loads(r.content)
            df2 = json_normalize(info)
            
            for coluna in colunas_desejadas:
                if coluna not in df1.columns:
                    df1[coluna] = ''
            
            # TESTE DE EXPRESSÃO LAMBDA, VERIFICA SE O CAMPO POSSUI INFORMAÇÃO, SE NÃO TIVER, PREENCHE CAMPO VAZIO
                    
            df1['newAgendaDtSlot'] = df1['clientAudits'].apply(lambda x: x[0]['newAgendaDtSlot'] if x and len(x) > 0 and 'newAgendaDtSlot' in x[0] else '') 

            
            # DEFININDO A COLUNA DE CONTATOS TELEFONICOS

            if df2.empty:
                df1['telefone'] = ''
            else:
                df1['telefone'] = df2['CONTATO.TELEFONE'][0]        
            
            df1 = renomeiaApi(df1)
                     
            if r is not None or info is not None or df1.empty :
                print(str(r.status_code))  
                print(r.content)
            else:
             return None

            if not (r.status_code >= 200 and r.status_code <= 399):
                return None
            else:
                print("Inserir OS: \33[104m"+str(r.status_code))
                return df1
        except Exception as e:
            print(e)
            return None