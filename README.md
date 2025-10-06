# Armazenamento e análise de dados um CSV utilizando PySpark + Apache Iceberg

Projeto da disciplina de Engenharia de Dados da UNISATC que visa criar um ambiente local com PySpark e Apache Iceberg e evidenciar as funcionalidades de ambas as ferramentas, importando um arquivo CSV e manipulando os dados e colunas, além do armazenamento e realização de análises do arquivo.

Repositório da segunda parte do projeto utilizando Delta Lake: https://github.com/AlexandreSartor/Spark-Delta-lake.git

# Setup do ambiente de desenvolvimento

Foi utilizado Linux Ubuntu 24.04 para este laboratório.

Comandos para setup do ambiente de desenvolvimento:

```python
uv init
uv venv
source .venv/bin/activate
uv add pyspark==3.5.3 jupyterlab ipykernel
```

## Uma documentação mais completa se encontra no MkDocs na pasta sparkiceberg-docs.

Podemos rodar o MkDocs com:

Após clonar o repositório e estar no diretório correto:
```python
cd sparkiceberg-docs (garantir que está no diretório do MkDocs)
mkdocs build
mkdocs serve
```
