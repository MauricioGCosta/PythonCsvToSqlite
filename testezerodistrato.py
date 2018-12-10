import pandas as pd
import colorsys
import warnings
from tkinter import filedialog
from tkinter import *
import os

#Definições das Tabelas
def Cria_Tabela(tabelaCliente, tabelaConjuge, tabelaCep, tabelaClienteCep, tabelaEmpreendimento, tabelaClienteEmpreendimento, \
                tabelaCategoriaEmpr, tabelaImovel, tabelaFinanciamento, tabelaIndiceCorrecao, tabelaParcela):
    #Cria tabela Sql Cliente
    sql_create = 'create table ' + tabelaCliente + \
            ' (codcliente integer primary key autoincrement not null, '\
            ' codigocliente integer, nomecliente varchar(50), datanascimento date, numcpf varchar(50),' \
            ' numinscrest varchar(30), sexo varchar(30), estadocivil varchar(30), codconjuge integer, foreign key(codconjuge) references Conjuge(codconjuge)' \
            ')'
    cur.execute(sql_create)

    #Cria tabela Sql Conjuge
    sql_create = 'create table ' + tabelaConjuge + \
            ' (codconjuge integer primary key autoincrement not null, '\
            ' nomeconjuge varchar(50), numcpfconjuge varchar(50), datanascimentoConjuge varchar(50))'
    cur.execute(sql_create)

    #Cria tabela Sql Cep
    sql_create = 'create table ' + tabelaCep + \
            ' (codcep integer primary key autoincrement not null, '\
            ' cepcliente varchar(15), endcliente varchar(100), bairrocliente varchar(50), municipiocliente varchar(50))'
    cur.execute(sql_create)

    #Cria tabela Sql ClienteCep
    sql_create = 'create table ' + tabelaClienteCep + \
            ' (codCliente integer not null, codcep integer not null, '\
            ' compcliente varchar(50),' \
            ' primary key (codCliente, codcep))'
    cur.execute(sql_create)

    #Cria tabela Sql Empreendimento
    sql_create = 'create table ' + tabelaEmpreendimento + \
            ' (codempreendimento integer primary key autoincrement not null, cdempreendimento varchar(15), ' \
            ' nomeempreendimento varchar(50), endempreendimento varchar(100), bairroempreendimento varchar(50), ' \
            ' cepempreendimento varchar(15), municipioempreendimento varchar(30), codcategoria  integer, ' \
            ' foreign key(codcategoria) references CategoriaEmpr(codcategoria) )'
    cur.execute(sql_create)

    #Cria tabela Sql ClienteEmpreendimento
    sql_create = 'create table ' + tabelaClienteEmpreendimento + \
            ' (codCliente integer not null, codEmpreendimento integer not null, primary key(codCliente, codEmpreendimento)) '
    cur.execute(sql_create)

    #Cria tabela Sql CategoriaEmpr
    sql_create = 'create table ' + tabelaCategoriaEmpr + \
            ' (codCategoria integer primary key autoincrement not null, descricaoCategoria varchar(20)) '
    cur.execute(sql_create)

    #Cria tabela Sql Imovel
    sql_create = 'create table ' + tabelaImovel + \
            ' (codimovel integer not null, codEmpreendimento integer, ' \
            ' tipoimovel varchar(25), area varchar(40), numeroUnidade varchar(40), Titulo varchar(15), primary key(codimovel, codEmpreendimento) )'
    cur.execute(sql_create)

    #Cria tabela Sql Financiamento
    sql_create = 'create table ' + tabelaFinanciamento + \
            ' (codFinanciamento integer primary key autoincrement not null, '\
            ' codImovel integer, dataaniversario varchar(20), tipobaixa varchar(30),' \
            ' datadistrato varchar(20), valorcontratototal varchar(20), mesbase varchar(10),' \
            ' foreign key(codImovel) references Imovel(codImovel))'
    cur.execute(sql_create)

    #Cria tabela Sql Índice Correção
    sql_create = 'create table ' + tabelaIndiceCorrecao + \
            ' (codIndiceCorrecao integer primary key autoincrement not null, '\
            ' indicecorrecao varchar(50), tipocorrecao varchar(70))'
    cur.execute(sql_create)

    #Cria tabela Sql Parcela
    sql_create = 'create table ' + tabelaParcela + \
            ' (codParcela integer primary key autoincrement not null, '\
            ' codIndiceCorrecao integer, codFinanciamento integer, parcela integer, ' \
            ' percentualjuros varchar(20), tipojuros varchar(10), dataVencimento varchar(20),' \
            ' valororiginalparcela varchar(20), saldodevedorparcela varchar(20), valorbruto varchar(20), valordesconto double(5,2), ' \
            ' valorliquido double(7,2), jurosmora varchar(15), multamora varchar(15), tipobaixa varchar(20), databaixa varchar(20), ' \
            ' correcaomonetaria varchar(15), jurospricesac varchar(15), '\
            ' foreign key(codIndiceCorrecao) references IndiceCorrecao(codIndiceCorrecao), ' \
            ' foreign key(codFinanciamento) references Financiamento(codFinanciamento) )'
    cur.execute(sql_create)

#Select padronizado para receber como variavel, o nome da tabela e dois campos chaves
def selectTabela(nomeTabela, campo1, indice1, campo2, indice2):
    retorno = "nao achou"
    sql_select = "select * from " + nomeTabela + " where " + campo1 + " = " + str(indice1) + " and " + campo2 + " = " + str(indice2)
    cur.execute(sql_select)
    registro = cur.fetchone()
    if (registro is not None):
        retorno = "achou"
    return retorno

#Funções de CRUD - no caso, somente o insert
def InsereRegistros():
    #Variáveis - Definição
    indice = indiceCliente = indiceConjuge = indiceCep = indiceEmpreendimento = indiceCategoriaDescr = indiceImovel = indiceFinanciamento = 0
    indiceCorrecao = indiceParcela = codigoClienteAnterior = codFinanciamentoAtual = 0
    #Arrays
    ceps = []
    conjuges = []
    empreendimentos = []
    categorias = []
    #imoveis = []
    indices = []
    #Dicionários
    dictCliente = {}
    dictCep = {}
    dictEmpreendimento = {}
    dictCategoria = {}
    dictConjuges = {}
    dictIndices = {}

    #Insere registros Conjuge
    sql_insert_Conjuge = 'insert into conjuge values (?, ?, ?, ?)'
    #Preenche campos Conjuge
    nomeConjuge = df.CONJUGE
    numCPFConjuge = df.NUCPFCONJUGE
    dataNascimentoConjuge = df.DATANASCIMENTOCONJUGE

    #Insere registros Cliente
    sql_insert_Cliente = 'insert into cliente values (?, ?, ?, ?, ?, ?, ?, ?, ?)'
    #Preenche campos Cliente
    codigoCliente = df.CODIGO_CLIENTE
    #codCliente = int(codigoCliente[0])
    nomeCliente = df.NOME_CLIENTE
    dataNascimento = df.DATANASCIMENTO
    numeroCPF = df.NUCPF
    numInscrEst = df.NUINCREST
    sexo = df.FLSEXO
    estadoCivil = df.ESTADOCIVIL

    #Insere registros Cep
    sql_insert_Cep = 'insert into cep values (?, ?, ?, ?, ?)'
    #Preenche campos Cep
    cepCliente = df.CEP_CLIENTE
    endCliente = df.ENDCLIENTE
    bairroCliente = df.BAIRROCLIENTE
    municipioCliente = df.MUNICIPIOCLIENTE

    #Insere registros ClienteCep
    sql_insert_ClienteCep = 'insert into clientecep values (?, ?, ?)'
    #Preenche campos ClienteCep
    compCliente = df.COMPCLIENTE

    #Insere registros Empreendimento
    sql_insert_Empreendimento = 'insert into empreendimento values (?, ?, ?, ?, ?, ?, ?, ?)'
    #Preenche campos Empreendimento
    cdEmpreendimento = df.CDEMPREEND
    nomeEmpreendimento = df.NOME_EMPREENDIMENTO
    endEmpreendimento = df.END_EMPREEND
    bairroEmpreendimento = df.BAIRRO_EMPREEND
    cepEmpreendimento = df.CEP_EMPREEND
    municipioEmpreendimento = df.MUNICIPIO_EMPREEND
    codCategoria = 0

    #Insere registros ClienteEmpreendimento
    sql_insert_ClienteEmpreendimento = 'insert into clienteempreendimento values (?, ?)'

    #Insere registros CategoriaEmpreendimento
    sql_insert_CategoriaEmpr = 'insert into categoriaEmpr values (?, ?)'
    #Preenche campos CategoriaEmpr
    descricaoCategoria = df.CATEGORIA

    #Insere registros Imovel
    sql_insert_Imovel = 'insert into imovel values (?, ?, ?, ?, ?, ?)'
    #Preenche campos Imovel
    tipoImovel = df.TIPOIMOVEL
    area = df.AREA
    numeroUnidade = df.NUUNIDADE
    titulo = df.TITULO

    #Insere registros Índices Correção
    sql_insert_IndiceCorrecao = 'insert into indicecorrecao values (?, ?, ?)'
    #Preenche campos Índices Correção
    indiceCorrecaoPorExtenso = df.INDICE_CORRECAO
    tipoCorrecao = df.TIPOCORRECAO

    while (indice < len(df.index)):

        #Preenche tabela Conjuge evitando duplicidade
        if (numCPFConjuge[indice] not in conjuges):
            cur.execute(sql_insert_Conjuge, (indiceConjuge, nomeConjuge[indice], numCPFConjuge[indice], dataNascimentoConjuge[indice]))
            conjuges.append(numCPFConjuge[indice])
            dictConjuges[numCPFConjuge[indice]] = indiceConjuge
            indiceConjuge += 1

        #Preenche tabela Cliente Evitando duplicidade
        if (int(codigoClienteAnterior) != int(codigoCliente[indice])):

            cur.execute(sql_insert_Cliente, (indiceCliente, int(codigoCliente[indice]), nomeCliente[indice], \
                                     dataNascimento[indice], numeroCPF[indice], numInscrEst[indice], sexo[indice], \
                                     estadoCivil[indice], dictConjuges[numCPFConjuge[indice]] \
                                     ))

            #Acrescenta no Dicionário que relaciona "codCliente" ou "indiceCliente" com "codigoCliente", evitando selects desnecessários
            dictCliente[codigoCliente[indice]] = indiceCliente

            indiceCliente += 1
            codigoClienteAnterior = codigoCliente[indice]

        #Preenche tabela Cep evitando duplicidade
        #verifica se já existe o cep cadastrado (utilizando Array para economizar recursos, evitando selects desnecessários)
        if (cepCliente[indice] not in ceps):
            cur.execute(sql_insert_Cep, (indiceCep, cepCliente[indice], endCliente[indice], bairroCliente[indice], municipioCliente[indice]))
            ceps.append(cepCliente[indice])
            dictCep[cepCliente[indice]] = indiceCep
            indiceCep += 1

        #Busca na tabela ClienteCep para verificar se já existe, caso não, insere na mesma
        if (selectTabela("ClienteCep", "codcliente", dictCliente[codigoCliente[indice]], "codcep", dictCep[cepCliente[indice]]) == "nao achou"):
            cur.execute(sql_insert_ClienteCep, (dictCliente[codigoCliente[indice]], dictCep[cepCliente[indice]], compCliente[indice]))

        #Preenche tabela CategoriaEmpr evitando duplicidade
        if (descricaoCategoria[indice] not in categorias):
            cur.execute(sql_insert_CategoriaEmpr, (indiceCategoriaDescr, descricaoCategoria[indice]))
            categorias.append(descricaoCategoria[indice])
            dictCategoria[descricaoCategoria[indice]] = indiceCategoriaDescr
            indiceCategoriaDescr += 1

        #Preenche tabela Empreendimento evitando duplicidade
        #verifica se já existe o empreendimento cadastrado (utilizando Array para economizar recursos, evitando selects desnecessários)
        if (nomeEmpreendimento[indice] not in empreendimentos):
            cur.execute(sql_insert_Empreendimento, (indiceEmpreendimento, str(cdEmpreendimento[indice]), nomeEmpreendimento[indice], endEmpreendimento[indice], \
                                                    bairroEmpreendimento[indice], cepEmpreendimento[indice], municipioEmpreendimento[indice], \
                                                    dictCategoria[descricaoCategoria[indice]]))
            empreendimentos.append(nomeEmpreendimento[indice])
            dictEmpreendimento[nomeEmpreendimento[indice]] = indiceEmpreendimento
            indiceEmpreendimento += 1

        #Busca na tabela ClienteEmpreendimento para verificar se já existe, caso não, insere na mesma
        if (selectTabela("ClienteEmpreendimento", "codcliente", dictCliente[codigoCliente[indice]], "codEmpreendimento", \
                         dictEmpreendimento[nomeEmpreendimento[indice]]) == "nao achou"):
            cur.execute(sql_insert_ClienteEmpreendimento, (dictCliente[codigoCliente[indice]], dictEmpreendimento[nomeEmpreendimento[indice]]))

        #Preenche tabela Índice Correção evitando duplicidade
        #verifica se já existe o índice cadastrado (utilizando Array para economizar recursos, evitando selects desnecessários)
        if (indiceCorrecaoPorExtenso[indice] not in indices):
            cur.execute(sql_insert_IndiceCorrecao, (indiceCorrecao, indiceCorrecaoPorExtenso[indice], tipoCorrecao[indice]))
            indices.append(indiceCorrecaoPorExtenso[indice])
            dictIndices[indiceCorrecaoPorExtenso[indice]] = indiceCorrecao
            indiceCorrecao += 1

        #Busca pelo codigo de imóvel, codigo de empreendimento, imóvel, área e numero da unidade
        sql_select = "select * from Imovel where codempreendimento = " + str(dictEmpreendimento[nomeEmpreendimento[indice]]) + \
            " and tipoimovel = '" + str(tipoImovel[indice]) + "' and area = '" + str(area[indice]) + "' and numerounidade =  '" + str(numeroUnidade[indice]) + "'"
        cur.execute(sql_select)
        registro = cur.fetchone()
        if (registro is None):
            cur.execute(sql_insert_Imovel, (indiceImovel, dictEmpreendimento[nomeEmpreendimento[indice]], tipoImovel[indice], area[indice], numeroUnidade[indice], \
                                            str(titulo[indice])))

            #Preenche junto novo Financiamento, pois relacionamento é de 1/1
            InsereFinanciamento(indiceImovel, indice, indiceFinanciamento)
            #Preenche a primeira das parcelas, com seus índices de correção
            codFinanciamentoAtual = indiceFinanciamento
            InsereParcela(indice, indiceParcela, dictIndices[indiceCorrecaoPorExtenso[indice]], codFinanciamentoAtual)

            indiceImovel += 1
            indiceFinanciamento += 1
            indiceParcela += 1
        else:
            #Preenche nova parcela, com seus índices de correção
            InsereParcela(indice, indiceParcela, dictIndices[indiceCorrecaoPorExtenso[indice]], codFinanciamentoAtual)
            indiceParcela += 1

        #VERSÃO DESCONTINUADA UTILIZANDO ARRAYS DE TUPLAS, POR TORNAR O PROCESSAMENTO MUITO LENTO
        #Preenche tabela Imovel evitando duplicidade e utilizando "Arrays de Tuplas"
        #if ((indiceImovel, dictEmpreendimento[cdEmpreendimento[indice]], tipoImovel[indice], area[indice], numeroUnidade[indice]) not in imoveis):
        #cur.execute(sql_insert_Imovel, (indiceImovel, dictEmpreendimento[cdEmpreendimento[indice]], tipoImovel[indice], area[indice], numeroUnidade[indice], str(titulo[indice])))
        #Crio a Tupla e a insiro no array de imoveis
        #imoveis.append( (indiceImovel, dictEmpreendimento[cdEmpreendimento[indice]], tipoImovel[indice], area[indice], numeroUnidade[indice]) )

        indice += 1

def InsereFinanciamento(indiceImovel, indice, indiceFinanciamento):
    #Insere registros Financiamento
    sql_insert_Financiamento = 'insert into financiamento values (?, ?, ?, ?, ?, ?, ?)'
    #Preenche campos Financiamento
    dataAniversario = df.DATAANIVERSARIO
    tipoBaixa = df.TIPO_BAIXA
    dataDistrato = df.DATADISTRATO
    valorContratoTotal = df.VALOR_CONTRATO_TOTAL
    mesBase = df.MES_BASE

    #Preenche tabela Financiamento evitando duplicidade
    cur.execute(sql_insert_Financiamento, (indiceFinanciamento, indiceImovel, dataAniversario[indice], tipoBaixa[indice], dataDistrato[indice], \
                                           valorContratoTotal[indice], str(mesBase[indice])))

def InsereParcela(indice, indiceParcela, codIndiceCorrecao, codFinanciamentoAtual):
    #Insere registros Parcela
    sql_insert_Parcela = 'insert into parcela values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    #Preenche campos Parcela
    parcela = df.PARCELA
    percentualJuros = df.PERCENTUAL_JUROS
    tipoJuros = df.TIPOJUROS
    dataVencimento = df.DATAVENCIMENTO
    valorOriginalParcela = df.VALORORIGINALPARCELA
    saldoDevedorParcela = df.SALDODEVEDORPARCELA
    valorBruto = df.VALORBRUTO
    valorDesconto = df.VALORDESCONTO
    valorLiquido = df.VALORLIQUIDO
    jurosMora = df.JUROSMORA
    multaMora = df.MULTAMORA
    tipoBaixa = df.TIPO_BAIXA
    dataBaixa = df.DATABAIXA
    correcaoMonetaria = df.CORRECAOMONETARIA
    jurosPriceSac = df.JUROSPRICESAC

    #Preenche tabela Parcela evitando duplicidade
    cur.execute(sql_insert_Parcela, (indiceParcela, codIndiceCorrecao, codFinanciamentoAtual, \
                                     str(parcela[indice]), percentualJuros[indice], tipoJuros[indice], dataVencimento[indice], \
                                     valorOriginalParcela[indice], saldoDevedorParcela[indice], \
                                     valorBruto[indice], valorDesconto[indice], valorLiquido[indice], \
                                     jurosMora[indice], multaMora[indice], tipoBaixa[indice], dataBaixa[indice], \
                                     correcaoMonetaria[indice], jurosPriceSac[indice] ))

def LeRegistros():
    #Lê os registros
    sql_select = 'select * from parcela'
    cur.execute(sql_select)

#----------------------------------------------------- Início da Execução -----------------------------------------------------------

arquivoSelecionado = False
root = Tk()
root.filename = filedialog.askopenfilename(initialdir = "/", title = 'Selecione o arquivo',
                                           filetypes= (("csv", "*.csv"),("Todos", "*.*")))
try:
    with open(root.filename, 'r') as UseFile:
        arquivoSelecionado = True
except:
    print ("Nenhum arquivo selecionado")

if (arquivoSelecionado):

    arquivoSaida = root.filename[:-4]
    arquivoSaida + ".bd"

    #Lê o .csv
    df = pd.read_csv(root.filename, error_bad_lines=False , encoding='latin-1')

    #Remove o arquivo com o Banco de Dados SQLite (caso exista)
    import os
    os.remove(arquivoSaida) if os.path.exists(arquivoSaida) else None

    #Importa o módulo de acesso ao SQLite
    import sqlite3
    #cria uma conexão com o banco de dados
    con = sqlite3.connect(arquivoSaida)
    #Criando um cursor
    cur = con.cursor()

    Cria_Tabela('Cliente', 'Conjuge', 'Cep', 'ClienteCep', 'Empreendimento', 'ClienteEmpreendimento', 'CategoriaEmpr', 'Imovel', 'Financiamento', 'IndiceCorrecao', 'Parcela')

    InsereRegistros()

    #Comita a transação
    con.commit()

    #LeRegistros()
    #dados = cur.fetchall()
    ##mostra os registros
    #for linha in dados:
        #print ('ÍndiceCliente: %i, CodigoCliente: %i, NomeCliente: %s, DataNascimento: %s, NúmeroCPF: %s, NumInscrEst: %s, Sexo: %s, EstadoCivil: %s, CodConjuge %i \n' %linha )
        #print ('ÍndiceConjuge: %i, NomeConjuge: %s, NumCPFConjuge %s, DataNascimentoConjuge %s' %linha)
        #print ('ÍndiceCep: %i, CepCliente: %s, EndCliente: %s, BairroCliente %s, MunicipioCliente: %s' %linha)
        #print ('CodCliente: %i, CodCep: %i, CompCliente: %s' %linha)
        #print ('ÍndiceEmpreendimento: %i, CdEmpreendimento: %s, NomeEmpreendimento: %s, EndEmpreendimento: %s, BairroEmpreendimento: %s, \
        #CepEmpreendimento: %s, MunicipioEmpreendimento %s, CodEmpreendimento %i \n' %linha )
        #print ('CodCliente: %i, CodEmpreendimento: %i' %linha)
        #print ('CodCategoria: %i, DescricaoCategoria: %s' %linha)
        #print ('ÍndiceImovel: %i, CodEmpreendimento: %s, TipoImovel: %s, Área: %s, NumeroUnidade: %s, Titulo: %s' %linha )
        #print ('CodigoFinanciamento: %i, CodImóvel: %i, DataAniversario: %s, TipoBaixa: %s, DataDistrato: %s, ValorContratoTotal: %s, MesBase: %s' %linha )
        #print ('CodigoÍndiceCorreção: %i, ÍndiceCorreção: %s, TipoCorreção: %s' %linha )
        #print ('IndParcela: %i, CodIndCorrecao: %i, Codfinanc: %i, Parcela: %s, PercentualJuros: %s, TipoJuros: %s, DataVcto: %s, ValorOrig: %s, SaldoDev: %s, \
        #ValorBruto: %s, ValorDesc: %s, ValorLiq: %s, JurosMora: %s, MultaMora: %s, TipoBaixa: %s, DataBaixa: %s, CorrMonet: %s, JurosPriceSac: %s, ' %linha )

    #Fechar a conexão
    con.close()

    print ("Execução finalizada com sucesso!")
