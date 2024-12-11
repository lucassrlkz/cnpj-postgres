
# cnpj-postgres

### Fork do projeto do rictom: [github-projeto](https://github.com/rictom/cnpj-mysql/tree/main)

## Tabelas no postgres

Esse fork baixa os arquivos, insere os dados no banco, cria as tabelas do banco com o tipo de dados certos para as colunas e adiciona as seguintes tabelas baseado no arquivo da receita [Dicionário de Dados do Cadastro Nacional da Pessoa Jurídica](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj):<br>

- matriz_filial
- identificador_socio
- opcao_simples_mei
- porte_empresa
- situacao_cadastral
- faixa_etaria_socio

Script em python para carregar os arquivos de cnpj dos dados públicos da Receita Federal em POSTGRESQL. O código é compatível com o layout das tabelas disponibilizadas pela Receita Federal a partir de 2021.

O script usa a função do postgres (copy_expert) para inserir os dados no banco da forma mais performática possível.

## Dados públicos de cnpj no site da Receita:
Os arquivos csv zipados com os dados de CNPJs estão disponíveis em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj ou https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/ (a partir de 28/10/2024.)<br>

## Pré-requisitos:
Python 3.8 ou superior<br>

Se estiver no Windows use o [Anaconda](https://www.anaconda.com/download#downloads) que já configura o Python e ferramentas de programação.

Se estiver no linux(Ubuntu) instale o python e o python-venv: [python-venv](https://www.geeksforgeeks.org/how-to-install-python-on-linux/)

Bibliotecas: pandas, dask, sqlalchemy e psycopg2/psycopg2-binary.<br>

Para instalar a biblioteca, use o comando<br>

```shell
pip install nome-biblioteca
```

Recomenda-se psycopg2-binary para instalação mais simples. (testado no Ubuntu)<br>

```shell
pip install psycopg2-binary 
```

Baixe o projeto como zip pelo botão "Download ZIP" no menu Code.

## Utilizando o script:
No windows abra o Anaconda Prompt e navega ate a pasta do projeto.

Para instalar todas as bibliotecas necessárias use o comando dentro da pasta do projeto:<br>

```shell
pip install -r requirements.txt
```

Dentro do arquivo <b>requirements.txt</b> está apenas a biblioteca psycopg2-binary.<br>
Se quiser a outra biblioteca troque no arquivo ou baixe atraves do comando pip.

Para obter relação dos arquivos disponíveis no site da Receita Federal ou baixar os arquivos, faça o seguinte comando no Anaconda prompt:<br>

```shell
python dados_cnpj_baixa.py
```

Isto irá baixar os arquivos zipados do site da Receita na pasta "dados-publicos-zip".<br>

Se o download estiverm muito lento, sugiro utilizar um gerenciador de downloads, como o https://portableapps.com/apps/internet/free-download-manager-portable.<br>

No linux, utilize o ambiente virtual do python para instalar as bibliotecas. vá ate a pasta do projeto e rode o comando com o pip:

```shell
pip install -r requirements.txt
```
Use o script para baixar os dados, digite:

```shell
python dados_cnpj_baixa.py
```
A pasta com o nome "dados-publicos" deve estar vazia.<br>

No servidor POSTGRES, crie um banco de dados, por exemplo: cnpj.<br>
Especifique os parâmetros no começo do script (dados_cnpj_postgres):<br>
dbname = 'cnpj'<br>
username = 'root'<br>
password = ''<br>
host ='127.0.0.1:5432' <br>

Para iniciar esse script digite:<br>
```shell
python dados_cnpj_postgres.py
```

## Performance

Tempo para inserir todos os dados que são em torno de 30 GB. São em torno de 190 milhôes de linhas.

- testado em um i7 3770 com SSD =>  1H 20M.
- testado em um servidor linux => 1H 30m.

Uma opção é usar o projeto em https://github.com/rictom/cnpj-sqlite para gerar o arquivo em sqlite e usar uma ferramenta como o pgloader ou o DBeaver para converter depois em postgres.

Este colega usou o pgloader com um bom desempenho: https://github.com/rictom/cnpj-mysql/issues/5

## Outras referências:

Para trabalhar com os dados de cnpj no formato SQLITE, use o projeto do rictom: (https://github.com/rictom/cnpj-sqlite).<br>

O projeto (https://github.com/rictom/rede-cnpj) utiliza os dados públicos de CNPJ para visualização de relacionamentos entre empresas e sócios.<br>