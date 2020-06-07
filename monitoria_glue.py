import boto3 
from datetime import *
import pandas as pd
from pytz import timezone
import datetime
import sqlalchemy
import json

# cria uma sessão de cliente para uso da api da aws

# aqui especifico que quero acessar o Glue
client = boto3.client('glue')

# passando como parâmetro o secrets(onde tem as credenciais do glue)
client_secrets = boto3.client('secretsmanager')

# passando como parâmetro o 'identificador' referente ao secrets q contém as credenciais q eu preciso
response = client_secrets.get_secret_value(SecretId='rds_secrets') # nome fake

# response é um dict e SecretString é um dict dentro do response, porém q está entre '', fazendo com q seja string

# aqui eu transformo o valor do dict SecretString, q está como string(um dict entre ''), em dict(tiro as '' q o transfrmava em string)
secrets = json.loads(response['SecretString'])

# pegando de forma isolada as credenciais
database_username = secrets['username']
database_password = secrets['password']
database_ip       = secrets['host'] # endpoint de conexão com o rds 
database_name     = secrets['dbInstanceIdentifier']

#criando uma conexão com o mysql a partir das credenciais
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
# o format faz com q cada variável passada como parâmetro equivala a um dos numeros especificados na conection

def get_etl():
    
    # request no web service glue da AWS - retorna uma json com as informações dos trabalhos
    response = client.get_jobs()
    
    # Recebe a quantidade de elementos da lista jobs
    contador = len(response['Jobs'])

    names = []
    
    #para cada job no indice i, adicione na lista names se diferente de ('_test', '_Test', 'teste')
    for i in range(contador):
        etl = response['Jobs'][i]['Name']
        if etl[-5:] != "_test" and etl[-5:] != '_Test' and etl != 'Aws_Control' and etl[-6:] != '_teste':
            names.append(response['Jobs'][i]['Name'])
    return names

 # chamada da função q rotorna uma lista com os nomes dos ETL´s
etl = get_etl()

# request no web service glue da AWS
client = boto3.client('glue')

etl_dados = {}

current_date_time = (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) 
current_date = (datetime.datetime.now().strftime("%Y-%m-%d")) 

# criando um df vazio, só com as colunas
df = pd.DataFrame(columns = ['job_name','Timestamp_Inicio', 'Timestamp_Fim', 'Tempo_Inicio', 'TempoExec', 'TempoTotal','Status', 'data','update_d_time'])
try:
    # para cada etl, verifico o status de cada um
    for job in etl:
        response = client.get_job_runs( # metodo q verifica o estatus
            JobName= job,
            MaxResults=1 # esse 1 faz com q seja pego o último status de execução
        )

        # pega o tempo de execução do etl
        ExecutionTime = timedelta(seconds=response['JobRuns'][0]['ExecutionTime'])
        # pega a hora q começou o etl
        StartedOn = response['JobRuns'][0]['StartedOn']
        # a hora q acabou
        LastModifiedOn = response['JobRuns'][0]['LastModifiedOn']
        Status = response['JobRuns'][0]['JobRunState']
        # a diferença entre o tempo de fim e começo sera usada para pegar quanto tempo demorou pra começar o etl
        Delta = LastModifiedOn - StartedOn
        # tempo q demorou pra começar a execução do etl
        StartUpTime = Delta - ExecutionTime
        # tempo total de execuçao
        totalTime = LastModifiedOn - StartedOn
        # compilo as infos em um dict 
        etl_dados.update({job:{'job_name': job,'Timestamp_Inicio': str(StartedOn),'Timestamp_Fim': str(LastModifiedOn), 'Tempo_Inicio': str(StartUpTime),'TempoExec': str(ExecutionTime), 'TempoTotal': str(totalTime),'Status': str(Status), 'data': current_date,'update_d_time': current_date_time}})

        # preencho o df com o dict de cada etl
        df.loc[len(df)] = etl_dados[job]
        
        #insert data na tabela control_runs
        df.to_sql(con=database_connection, name='table_control', if_exists='append', index=False)
        
except Exception as error:
    print(error)

print("FIM")