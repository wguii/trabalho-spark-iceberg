## Projeto Apache Spark com Delta Lake

Este projeto foi criado para ilustrar o uso do Apache Spark em ambiente local (pyspark), realizando a gravação de arquivos no formato Delta Lake, também de forma local.

Recomenda-se utilizar o WSL no Windows 11 ou outra distribuição Linux. Evite rodar diretamente no Windows, pois seriam necessárias diversas configurações adicionais para que tudo funcione corretamente.

Neste laboratório, foi adotado o Ubuntu 24.04 como sistema operacional.

(<img width="981" height="444" alt="Image" src="https://github.com/user-attachments/assets/a891416a-8018-4f5d-945b-5a2abb2194a6" />)

Versão do python utilizada: 3.13.7.

Projeto python inicializado com o UV .

Comandos utilizados para setup do ambiente:

```bash 
uv init .
pyenv local 3.13.7
uv venv
source .venv/bin/activate
uv add pyspark==3.5.3 delta-spark==3.2.0 jupyterlab ipykernel
pip install ipykernel
```

Comandos para vizualizar a documentação localmente:

```bash
uv add mkdocs mkdocs-material

uv run mkdocs serve

Normalmente acessível em http://127.0.0.1:8000/
```
**Nota:** Entrar na pasta "docs" para rodar os comandos mkdocs

**Nota 2:** O arquivo `.ipynb` (jupyter notebook) foi executado dentro do VS Code.



Links para aprendizado sobre Apache/Spark e Delta Lake


https://www.youtube.com/watch?v=eOrWEsZIfKU&t=2316s

https://www.youtube.com/watch?v=WwrX1YVmOyA&t=853s







