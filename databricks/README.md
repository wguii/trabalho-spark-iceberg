# Projeto Databricks

Recomenda-se utilizar o WSL no Windows 11 ou outra distribuição Linux. Evite rodar diretamente no Windows, pois seriam necessárias diversas configurações adicionais para que tudo funcione corretamente.

Neste laboratório, foi adotado o Ubuntu 24.04 como sistema operacional.

<img width="981" height="444" alt="Image" src="https://github.com/user-attachments/assets/aff64b42-434c-4ca7-a84f-c7b11396bfd7" />

---

Este projeto demonstra um **pipeline de dados usando Databricks e Delta Lake**, aplicado a dados de Airbnb de oito cidades europeias. Ele transforma dados brutos (landing/bronze) em uma camada estruturada para análise (silver e gold).



## Estrutura do Projeto

- **landing**: pasta/volume de dados brutos (`CSV`)
- **bronze**: tabelas Delta com dados ingeridos, incluindo colunas de auditoria
- **silver**: tabelas Delta com dados limpos, renomeados e com colunas de auditoria atualizadas
- **gold**: tabelas Delta finais, com dados preparados para análise e indicadores calculados

---


## Cidades Processadas

- Amsterdam  
- Athens  
- Barcelona  
- Berlin  
- Lisbon  
- London  
- Paris  
- Rome  

---
## 1. Clonar o repositório via HTTPS

No terminal, execute o comando abaixo para clonar o repositório:

```bash
git clone https://github.com/AlexandreSartor/Projeto_databricks.git
```
Execute ```uv sync``` para sincronizar dependências

---

Versão do python utilizada: 3.13.7.

Projeto python inicializado com o UV .

Comandos utilizados para setup do ambiente:

```bash 
uv init .
pyenv local 3.13.7
uv venv
source .venv/bin/activate
```

Comandos para vizualizar a documentação localmente:

```bash
uv add mkdocs mkdocs-material

Normalmente acessível em http://127.0.0.1:8000/
```
**Nota:** Entrar na pasta "docs" para rodar os comandos mkdocs
