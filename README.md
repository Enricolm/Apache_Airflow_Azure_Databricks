# Apache Airflow e Azure Databricks

Visão Geral do Projeto:
  Este projeto tem como base a integração de tecnologias poderosas para otimizar o processamento de dados. 
  Nossa arquitetura é composta por três componentes principais:
  

  * Databricks na Azure:
      Utilizaremos o Databricks como nossa plataforma de análise de dados na nuvem. 
      A integração com a Azure oferece uma infraestrutura robusta para a execução eficiente de tarefas de processamento.
    
  * Apache Airflow para Orquestração:
      A orquestração de tarefas será gerenciada pelo Apache Airflow, permitindo uma execução coordenada e agendada das operações.
      Isso proporciona uma visão clara e controle sobre o fluxo de trabalho.

  * Armazenamento em Camadas (Silver e Gold) com Azure Datalake:
      Para a persistência dos dados, adotaremos o Azure Datalake.
      A arquitetura de camadas, dividida em "Silver" e "Gold", aprimora a organização e a qualidade dos dados.
      A camada "Silver" representa dados brutos e sem processamento, enquanto a camada "Gold" contém dados refinados e prontos para análises avançadas.
