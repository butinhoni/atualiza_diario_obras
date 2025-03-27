import psycopg2
from util import segredos
import pandas as pd
from sqlalchemy import create_engine

def open_conn():
    conn = psycopg2.connect(
    host = segredos.endereco,
    user = segredos.user,
    password = segredos.passwd,
    port = segredos.port,
    database = segredos.database
    )
    return(conn)

def _createTableDiario(conn):

    conn = psycopg2.connect(
    host = segredos.endereco,
    user = segredos.user,
    password = segredos.passwd,
    port = segredos.port,
    database = segredos.database
    )

    cur = conn.cursor()
    cur.execute('''
                CREATE TABLE IF NOT EXISTS public.visitasdiariosinfra
                (
                contrato varchar(8) NOT NULL,
                data date,
                status varchar(16),
                clima_manha varchar(16),
                clima_tarde varchar(16),
                clima_noite varchar(16),
                data_aceite_supervisora date,
                data_aceite_fiscal date,
                data_finalizado_rt date,
                obra varchar(32),
                empresa varchar(32),
                atividades int,
                fotos int,
                equipamentos int,
                mao_de_obra int,
                anotacao_supervisora int,
                anotacao_fiscal int,
                comentario_foto int,
                servico_foto int,
                numero_fotos int,
                numero_comentario_fotos int,
                numero_servicos_fotos int,
                numero_fotos2 int,
                numero_comentario_fotos2 int,
                numero_servicos_fotos2 int
                )
                ''')
    cur.close()
    conn.commit()
    conn.close()
    print('tabela criada')

def readDiarioObras(tipo = 'contrato'):

    conn = psycopg2.connect(
    host = segredos.endereco,
    user = segredos.user,
    password = segredos.passwd,
    port = segredos.port,
    database = segredos.database
    )

    cur = conn.cursor()
    cur.execute('SELECT * FROM public.visitasdiariosinfra')
    dados = cur.fetchall()
    colunas = []
    for item in cur.description:
        colunas.append(item[0])
    
    if tipo == 'colunas':
        return(colunas)
    df = pd.DataFrame(dados,columns = colunas)
    df = df.set_index('id')
    cur.close()
    conn.close()
    return(df)
    

def uparDiarios(df = pd.DataFrame):

    db_config = {
        'dbname':segredos.database,
        'user':segredos.user,
        'password':segredos.passwd,
        'host':segredos.endereco,
        'port':segredos.port
    }

    connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(connection_string)
    table_name = 'visitasdiariosinfra'

    df.to_sql(table_name, engine, if_exists='replace', index=False)


def ler_contr():
    conn = psycopg2.connect(database = segredos.database,
                            user = segredos.user,
                            host = segredos.endereco,
                            password = segredos.passwd,
                            port = 5432)
    cur = conn.cursor()

    colunas = ["IC", "EMPRESA", "RODOVIA", "EMPRESA2", "REEF", "RPFO", "PREV", "ATIVO"]

    cur.execute('SELECT * FROM public.contratos')
    data = cur.fetchall()

    df = pd.DataFrame(data, columns= colunas)
    cur.close()
    conn.close()
    return(df)