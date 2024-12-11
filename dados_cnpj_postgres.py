# -*- coding: utf-8 -*-
"""

@author: lucassrlkz
https://github.com/lucassrlkz/cnpj-postgres

"""
#%%
import os, sys
import csv, zipfile

import pandas as pd, sqlalchemy, glob, time
from io import StringIO
from sqlalchemy import text

#%% DEFINA os parâmetros do servidor.

tipo_banco = 'postgres'
dbname = ''
username = 'postgres'
password = ''
host = ''

pasta_compactados = r"dados-publicos-zip"
pasta_saida = r"dados-publicos" #esta pasta deve estar vazia. 
dataReferencia = 'dd/mm/2024' #input('Data de referência da base dd/mm/aaaa: ')

ApagaDescompactadosAposUso = True

resp = input(f'Isto irá CRIAR TABELAS ou REESCREVER TABELAS no database {dbname.upper()} no servidor {tipo_banco} {host} e MODIFICAR a pasta {pasta_saida}. Deseja prosseguir? (S/N)?')
if not resp or resp.upper()!='S':
    sys.exit()

#%%
def connect_db(strConn):
    engine = None
    try:
        print("Conectando ao banco de dados...")
        engine_ = sqlalchemy.create_engine(strConn)
        engine = engine_.connect()
    except (error):
        print(error)
        sys.exit()
    print("Conectado com sucesso!")
    return engine
#%%

arquivos_a_zipar = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))

for arq in arquivos_a_zipar:
    print('descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)

dataReferenciaAux = list(glob.glob(os.path.join(pasta_saida, '*.EMPRECSV')))[0].split('.')[2] #formato DAMMDD, vai ser usado no final para inserir na tabela  _ref
if len(dataReferenciaAux)==len('D30610') and dataReferenciaAux.startswith('D'):
    dataReferencia = dataReferenciaAux[4:6] + '/' + dataReferenciaAux[2:4] + '/202' + dataReferenciaAux[1]
	
#%%

if tipo_banco=='mysql':
    engine = connect_db(f'mysql+pymysql://{username}:{password}@{host}/{dbname}')
    
elif tipo_banco=='postgres':
    engine = connect_db(f'postgresql://{username}:{password}@{host}/{dbname}')
    
else:
    print('tipo de banco de dados não informado')
    sys.exit()

# criacao de tabelas principais e tabelas de apoio
sqlTabelas = '''
    DROP TABLE if exists identificador_socio;

    CREATE TABLE identificador_socio(
	codigo SMALLINT PRIMARY KEY UNIQUE NOT NULL,
	identificador VARCHAR(16) NOT NULL
    );

    INSERT INTO identificador_socio VALUES
    ('1','PESSOA JURIDICA'), ('2','PESSOA FISICA'), ('3','ESTRANGEIRO');

    DROP TABLE if exists matriz_filial;

    CREATE TABLE matriz_filial(
        codigo SMALLINT PRIMARY KEY UNIQUE NOT NULL,
        IDENTIFICADOR VARCHAR(8)
    );

    INSERT INTO matriz_filial VALUES ('1','MATRIZ'),('2','FILIAL');

    DROP TABLE if exists opcao_simples_mei;

    CREATE TABLE opcao_simples_mei (
	codigo VARCHAR(1) PRIMARY KEY NOT NULL,
	descricao VARCHAR(6)
	);

    INSERT INTO opcao_simples_mei VALUES
    ('S','SIM'),('N','NÂO'),('','OUTROS');

    DROP TABLE if exists porte_empresa;

    CREATE TABLE porte_empresa(
	codigo VARCHAR(2) PRIMARY KEY UNIQUE NOT NULL,
	descricao VARCHAR(25) DEFAULT NULL
    );

    INSERT INTO porte_empresa VALUES
    ('00','NÃO INFORMADO'), ('01','MICRO EMPRESA'), ('03','EMPRESA DE PEQUENO PORTE'), ('05','DEMAIS');

    DROP TABLE if exists situacao_cadastral;

    CREATE TABLE situacao_cadastral (
    codigo VARCHAR(2) PRIMARY KEY UNIQUE NOT NULL,
    descricao VARCHAR(8)
    );

    INSERT INTO situacao_cadastral VALUES
    ('01','NULA'),
    ('02','ATIVA'),
    ('03','SUSPENSA'),
    ('04','INAPTA'),
    ('08','BAIXADA');

    DROP TABLE if exists faixa_etaria_socio;

    CREATE TABLE faixa_etaria_socio(
	codigo SMALLINT PRIMARY KEY UNIQUE NOT NULL,
	faixa_etaria VARCHAR(18)
    );

    INSERT INTO faixa_etaria_socio VALUES
    ('0','NÂO SE APLICA'),
    ('1','0-12 anos'),
    ('2','13-20 anos'),
    ('3','21-30 anos'),
    ('4','31-40 anos'),
    ('5','41-50 anos'),
    ('6','51-60 anos'),
    ('7','61-70 anos'),
    ('8','71-80 anos'),
    ('9','81 anos ou mais');

    DROP TABLE if exists cnae;

    CREATE TABLE cnae (
    codigo VARCHAR(7)
    ,descricao VARCHAR(200)
    );

    DROP TABLE if exists empresas;

    CREATE TABLE empresas (
    cnpj_basico VARCHAR(8)
    ,razao_social VARCHAR(255)
    ,natureza_juridica VARCHAR(4)
    ,qualificacao_responsavel VARCHAR(2)
    ,capital_social VARCHAR(22)
    ,porte_empresa VARCHAR(2)
    ,ente_federativo_responsavel VARCHAR(50)
    );

    DROP TABLE if exists estabelecimento;

    CREATE TABLE estabelecimento (
    cnpj_basico VARCHAR(8)
    ,cnpj_ordem VARCHAR(4)
    ,cnpj_dv VARCHAR(2)
    ,matriz_filial SMALLINT
    ,nome_fantasia VARCHAR(200)
    ,situacao_cadastral VARCHAR(2)
    ,data_situacao_cadastral VARCHAR(8)
    ,motivo_situacao_cadastral VARCHAR(2)
    ,nome_cidade_exterior VARCHAR(200)
    ,pais VARCHAR(3)
    ,data_inicio_atividades VARCHAR(8)
    ,cnae_fiscal VARCHAR(7)
    ,cnae_fiscal_secundaria VARCHAR(1000)
    ,tipo_logradouro VARCHAR(20)
    ,logradouro VARCHAR(200)
    ,numero VARCHAR(10)
    ,complemento VARCHAR(200)
    ,bairro VARCHAR(200)
    ,cep VARCHAR(8)
    ,uf VARCHAR(2)
    ,municipio VARCHAR(4)
    ,ddd1 VARCHAR(4)
    ,telefone1 VARCHAR(8)
    ,ddd2 VARCHAR(4)
    ,telefone2 VARCHAR(8)
    ,ddd_fax VARCHAR(4)
    ,fax VARCHAR(8)
    ,correio_eletronico VARCHAR(255)
    ,situacao_especial VARCHAR(200)
    ,data_situacao_especial VARCHAR(8)
    );

    DROP TABLE if exists motivo;
    
    CREATE TABLE motivo (
    codigo VARCHAR(2) PRIMARY KEY NOT NULL
    ,descricao VARCHAR(200) DEFAULT NULL
    );
    
    DROP TABLE if exists municipio;
    
    CREATE TABLE municipio (
    codigo VARCHAR(4) PRIMARY KEY NOT NULL
    ,descricao VARCHAR(255) DEFAULT NULL
    );
    
    DROP TABLE if exists natureza_juridica;
    
    CREATE TABLE natureza_juridica (
    codigo VARCHAR(4) PRIMARY KEY NOT NULL
    ,descricao VARCHAR(200) DEFAULT NULL
    );
    
    DROP TABLE if exists pais;
    
    CREATE TABLE pais (
    codigo VARCHAR(3) PRIMARY KEY NOT NULL
    ,descricao VARCHAR(200) DEFAULT NULL
    );
    
    DROP TABLE if exists qualificacao_socio;
    
    CREATE TABLE qualificacao_socio (
    codigo VARCHAR(2) PRIMARY KEY NOT NULL
    ,descricao VARCHAR(200) DEFAULT NULL
    );
    
    DROP TABLE if exists simples;
    
    CREATE TABLE simples (
    cnpj_basico VARCHAR(8)
    ,opcao_simples VARCHAR(1)
    ,data_opcao_simples VARCHAR(8)
    ,data_exclusao_simples VARCHAR(8)
    ,opcao_mei VARCHAR(1)
    ,data_opcao_mei VARCHAR(8)
    ,data_exclusao_mei VARCHAR(8)
    );
    
    DROP TABLE if exists socios;
    
    CREATE TABLE socios (
     cnpj_basico VARCHAR(8)
    ,identificador_socio SMALLINT
    ,nome_socio VARCHAR(200)
    ,cnpj_cpf_socio VARCHAR(14)
    ,qualificacao_socio VARCHAR(2)
    ,data_entrada_sociedade VARCHAR(8)
    ,pais VARCHAR(3)
    ,representante_legal VARCHAR(11)
    ,nome_representante VARCHAR(200)
    ,qualificacao_representante_legal VARCHAR(2)
    ,faixa_etaria SMALLINT
    );
    '''
#%%

# separa as querys por ; enumera e executa colocando mostrando o tempo no inicio e fim de cada execução 
def sqlQuery(querys, name):
    print(f'Inicio {name}:', time.asctime())
    
    for k, sql in enumerate(querys.split(';')):
        if not sql.strip():
            continue
        print('-'*20 + f'\nexecutando parte {k}:\n', sql)

        engine.execute(text(sql))
        print('fim parcial...', time.asctime())

    print(f'fim {name}...', time.asctime())

sqlQuery(sqlTabelas, 'sqlTabelas')
#%%


""" 
carrega as tabela mais leves direto com pandas e 
cria o index no banco para performance de consulta e 
apaga o arquivo depois de ser usado
"""
def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    # usando pandas pd
    arquivo = list(glob.glob(os.path.join(pasta_saida, '*' + extensaoArquivo)))[0]
    print('carregando tabela '+arquivo)
    
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
    dtab.to_sql(nomeTabela, engine, if_exists='append', index=None)

    engine.execute(text(f'CREATE INDEX idx_{nomeTabela} ON {nomeTabela}(codigo);'))
    if ApagaDescompactadosAposUso:
        print('apagando arquivo '+arquivo)
        os.remove(arquivo)

carregaTabelaCodigo('.MUNICCSV', 'municipio')
carregaTabelaCodigo('.CNAECSV','cnae')
carregaTabelaCodigo('.MOTICSV', 'motivo')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
carregaTabelaCodigo('.PAISCSV', 'pais')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

#%%
colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
              'nome_fantasia',
              'situacao_cadastral','data_situacao_cadastral', 
              'motivo_situacao_cadastral',
              'nome_cidade_exterior',
              'pais',
              'data_inicio_atividades',
              'cnae_fiscal',
              'cnae_fiscal_secundaria',
              'tipo_logradouro',
              'logradouro', 
              'numero',
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial'] 

colunas_empresas = ['cnpj_basico', 'razao_social',
           'natureza_juridica',
           'qualificacao_responsavel',
           'capital_social',
           'porte_empresa',
           'ente_federativo_responsavel']

colunas_socios = [
            'cnpj_basico',
            'identificador_socio',
            'nome_socio',
            'cnpj_cpf_socio',
            'qualificacao_socio',
            'data_entrada_sociedade',
            'pais',
            'representante_legal',
            'nome_representante',
            'qualificacao_representante_legal',
            'faixa_etaria'
          ]

colunas_simples = [
    'cnpj_basico',
    'opcao_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei']


# usando função do banco copy expert, muito mais rapido que metodo multi no pandas.to_sql()
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data
    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """

    # pega a conexao dbapi e usa o cursor para executar a funcao copy_expert
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)
        
        columns = ', '.join(['"{}"'.format(k) for k in keys])
        
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
    s_buf.truncate(0)

# carrega as tabelas grandes pelo tipo do arquivo: *.EMPRECSV
def carregaTipo(nome_tabela, tipo, colunas):
    # usando pandas pd, to_sql() junto com copy_expert é mto mais rápido 
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        
        # cria pedaços de 1 milhao de linhas para ler arquivo muito grande sem dar problema de falta de memória
        with pd.read_csv(arq, sep=';', header=None, names=colunas,
            encoding='latin1', dtype=str, na_filter=None, chunksize=1000000) as csv_reader:
            print('lendo chunk csv ...', time.asctime())
            
            # insere cada pedaço no banco e enumera os pedaços com horario 
            for index, chunk in enumerate(csv_reader):
                print(f'chunk {index} to_sql...', time.asctime())
                chunk.to_sql(nome_tabela, engine, index=False, if_exists='append', method=psql_insert_copy, chunksize=10000)
        
        # apaga arquivo depois de usado
        if ApagaDescompactadosAposUso:
            print(f'apagando o arquivo {arq=}')
            os.remove(arq)
        print('fim parcial...', time.asctime())


carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)
carregaTipo('socios', '.SOCIOCSV', colunas_socios)
#%%


# query de criação de index para performance de pesquisa no banco
sqls = '''
    CREATE INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
    CREATE INDEX idx_empresas_razao_social ON empresas (razao_social);
    CREATE INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);

    CREATE INDEX idx_socios_cnpj_basico ON socios(cnpj_basico);
    CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
    CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);

    CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);

    DROP TABLE IF EXISTS _referencia;
    CREATE TABLE _referencia (
        referencia	VARCHAR(100),
        valor		VARCHAR(100)
    );
'''

sqlQuery(sqls, 'sqls')   

#%% inserir na tabela referencia_

qtde_cnpjs = engine.execute(text('select count(cnpj_basico) as contagem from estabelecimento;')).fetchone()[0]

engine.execute(text(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')"))
engine.execute(text(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')"))
#%%

print('-'*20)
print(f'As tabelas foram criadas no servidor {tipo_banco}.')
print('Qtde de empresas (matrizes):', engine.execute(text('SELECT COUNT(*) FROM empresas')).fetchone()[0])

print('Qtde de estabelecimentos (matrizes e fiiais):', engine.execute(text('SELECT COUNT(*) FROM estabelecimento')).fetchone()[0])
print('Qtde de sócios:', engine.execute(text('SELECT COUNT(*) FROM socios')).fetchone()[0])

engine.commit()
engine.close()

print('FIM!!!', time.asctime())