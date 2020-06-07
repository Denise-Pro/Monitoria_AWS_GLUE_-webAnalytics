
<h1 align="center"> Monitoria AWS Glue </h1>


# Descrição do Projeto :eyes:
Esse projeto foi desenvolvido após o desenvolvimento de vários trablahos (ETL´S) no AWS Glue, com o intuito de controlar através do tempo de execução se o funcionamento estava occorendo como programado. Dias em que o tempo está muito maior ou muito menor do que a mádia servem como indicativos de problemas que podem ser provenientes da carga de dados(ou ausência dela), de sobrecarga no cluster etc. 

# Objetivo principal :dart:
Controlar o tempo de execução dos ETL´s

# Status do Projeto: :running:	
Concluído :muscle::trophy:

# Resumo das Funcionalidades

Conexão com o web servece aws via api :heavy_check_mark:

extração de dados do AWs Glue via api :heavy_check_mark:

Cálculos entre timestamps para obeter o tempo de execução :heavy_check_mark:

Carga dos resultados no banco relacional MYsQL :heavy_check_mark:


# Requisitos Técnicos :warning:
Para executar esse projeto é preciso baixar e configurar o client para conseguir usar a api da aws
adapte para o contexto do seu(s) Job/ ETL e banco de dados
Lembrar de mudar o nome da tabela fim e a conexão com a database
