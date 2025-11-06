# Criação das Camadas do Data Lakehouse

Este bloco SQL inicializa toda a estrutura de armazenamento utilizada no pipeline Delta Lake, garantindo a separação das camadas de dados conforme a arquitetura **Bronze → Silver → Gold**.  


```
%sql

CREATE SCHEMA IF NOT EXISTS workspace.landing
COMMENT 'Schema/Database para dados bronze (delta)';

CREATE VOLUME IF NOT EXISTS workspace.landing.dados
COMMENT 'Volume para dados brutos criados no schema/database landing';

CREATE SCHEMA IF NOT EXISTS workspace.bronze
COMMENT 'Schema/Database para dados bronze (delta)';

CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'Schema/Database para dados silver (delta)';

CREATE SCHEMA IF NOT EXISTS workspace.gold
COMMENT 'Schema/Database para dados gold (delta) - modelagem dimensional';
```

- **workspace.landing** → Camada de *aterrissagem* dos dados, usada para receber e armazenar arquivos brutos.  
- **workspace.landing.dados** → *Volume* associado ao schema *landing*, onde os arquivos físicos são salvos antes de serem processados.  
- **workspace.bronze** → Camada que armazena dados brutos no formato Delta, servindo como base de ingestão.  
- **workspace.silver** → Camada de dados refinados e padronizados, prontos para análises intermediárias.  
- **workspace.gold** → Camada final, com dados modelados para consumo analítico e visualizações (ex.: BI, dashboards).  

> **Observação:** Esses schemas garantem organização, versionamento e rastreabilidade dos dados em todo o fluxo do pipeline.
