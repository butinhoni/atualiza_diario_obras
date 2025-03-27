import pandas as pd
from util import database

#função pra ler e devolver
def readDiario(contratos, pasta, sheet):
    df = pd.read_excel(pasta, sheet)

#nomeia as colunas
    colunas = [
    'CONTRATO', #A
    'OBRA',
    'EMPRESA',
    'D',
    'DATA', #E
    'F', #F
    'G', #G
    'FINALIZADO', #H
    'ACEITE_SUP', #I
    'ACEITE_FISCAL', #J
    'CLIMA_MANHÃ', #K
    'CLIMA_TARDE', #L
    'CLIMA_NOITE', #M
    'EQUIPAMENTOS', #N
    'MÃO DE OBRA', #O
    'ATIVIDADES', #P
    'ANOTAÇÃO_SUPERVISORA', #Q
    'ANOTAÇÃO_FISCAL', #R
    'FOTOS', #S
    'COMENTARIO_FOTO', #T
    'SERVICO_FOTO', #U
    'NUMERO_FOTOS', #V
    'NUMERO_COMENTARIO_FOTOS', #W
    'NUMERO_SERVICOS_FOTOS', #X
    'NUMERO_FOTOS2', #Y
    'NUMERO_COMENTARIOS_FOTOS2', #Z
    'NUMERO_SERVICOS_FOTOS2', #AA
    'AB'
    ] 

    colunasUsaveis = []
    colunasmin = []

    for item in colunas:
        colunasmin.append(item.lower())
        if len(item) > 1:
            colunasUsaveis.append(item.lower())

    df = df.iloc[:,:37]


    df.columns = colunasmin
    df = df[colunasUsaveis]

#filtra os contratos
    listShow = []
    df['contrato'] = df['contrato'].apply(lambda x: str(x)[:8])

    for i, row in df.iterrows():
        ic = row['contrato']
        if row['contrato'] in contratos:
            listShow.append('mostra')
        else:
            listShow.append('não mostra')

    df['show'] = listShow

    df = df[df['show'] == 'mostra']
    df = df.drop(columns = ['show'])
    return(df)

sheet_url = 'https://docs.google.com/spreadsheets/d/1GK5fgGRY8VH4NwkD4aTVem_wi2Tp9rBp/export?format=xlsx'

#funcao para ler contratos
def lerContratos():
    contr = database.ler_contr()
    contratos = contr['IC'].apply(lambda x: x.replace('-','/')[:8])
    contratos = contratos.to_list()
    return (contratos)


contratos = lerContratos()

#lê o arquivo e as planilhas
pasta = pd.ExcelFile(sheet_url)
sheets = pasta.book.worksheets
sheetsDados = []

#filtra a lista de planilhas
for item in sheets:
    if 'DADOS' in item.title:
        sheetsDados.append(item)

#vou deixar só dados por enquanto pq a outra parece que é teste deles

'''
sheetsDados = ['DADOS']

tabelas = []

#junta tudo
for sheet in sheetsDados:
    name = sheet.title
    print(f'lendo {name}')
    tabelas.append(readDiario(contratos, sheet_url, name))

df = pd.concat(tabelas)
'''

df = readDiario(contratos, sheet_url, 'DADOS')
df = df.fillna(0)



#colunas = database.readDiarioObras('colunas')

#df.columns = colunas
df['obra'] = df['obra'].astype(str)[:1024]


database.uparDiarios(df)



