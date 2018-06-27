
# coding: utf-8

# # Desafio Semantix !!!

# # Desenvolvimento da Segunda Parte do Desafio:
# Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.
#     
#     
#     1) Número de hosts únicos.
#     r) 161884

# In[2]:


#testando a variavel Spark
sc


# In[3]:


#Acessando o arquivo de Julho
base = sc.textFile("C:/Users/thiag/Desktop/Semantix/access_log_Jul95.txt")
base


# In[4]:


#Checando assinatura da row
base.first()


# In[5]:


#Splitando por ' - - ', e retiramos as partes da string diferente dos holts
temp1 = base.flatMap(lambda k: k.split(" - - "))
temp1


# In[20]:


#filtrando partes Holts e crindo um mapa já re
temp2 = temp1.filter(lambda line: 'HTTP' not in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b)
Holtsjul = temp2.count()
Holtsjul


# In[13]:


#Checando o map
temp2.first()


# In[18]:


#Repitindo os mesmos procedimentos para agosto
base2 = sc.textFile("C:/Users/thiag/Desktop/Semantix/access_log_Aug95.txt")
temp3 = base2.flatMap(lambda k: k.split(" - - ")).filter(lambda line: 'HTTP' not in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b)
HoltsAug = temp3.count()


# In[21]:


#Resultado da Promeira Questão
Resposta1 = Holtsjul + HoltsAug
Resposta1


# # Quetão 2
#     2. O total de erros 404.
#     r)20901

# In[23]:


#Buscando linha a linha erros 404
Q404jul = base.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).count()
Q404Aug = base2.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).count()


# In[26]:


#Resultado da Segunda Questão
Resposta2 = Q404jul + Q404Aug
Resposta2


# # Questão 3
#     Os 5 URLs que mais causaram erro 404.
#     R)[('dialip-217.den.mmc.com', 56),
#      ('155.148.25.4', 40),
#      ('ts8-1.westwood.ts.ucla.edu', 37),
#      ('204.62.245.32', 36),
#      ('maz3.maz.net', 36)]

# In[65]:


#Filtrando separando e contanto os campeões de erro 404
Q404topJul = base.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).flatMap(lambda k: k.split(" - - ")).filter(lambda line: 'HTTP' not in line).filter(lambda line: ' ' not in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b)
Q404topAug = base2.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).flatMap(lambda k: k.split(" - - ")).filter(lambda line: 'HTTP' not in line).filter(lambda line: ' ' not in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b)


# In[66]:


#Ordena do maior para o menor
SortedAug = Q404topAug.sortBy(lambda x: x[1],False)
SortedJul = Q404topJul.sortBy(lambda x: x[1],False)


# In[67]:


#União e seleção dos 5 primeiros
tempFim = SortedAug + SortedJul
Resposta3 = tempFim.take(5)
Resposta3


# # Questão 4
#     Quantidade de erros 404 por dia
#     r)[('[28/Jul', 93),
#      ('[27/Jul', 319),
#      ('[26/Jul', 329),
#      ('[25/Jul', 439),
#      ('[24/Jul', 323),
#      ('[23/Jul', 224),
#      ('[22/Jul', 191),
#      ('[21/Jul', 316),
#      ('[20/Jul', 408),
#      ('[19/Jul', 623),
#      ('[18/Jul', 445),
#      ('[17/Jul', 394),
#      ('[16/Jul', 252),
#      ('[15/Jul', 239),
#      ('[14/Jul', 386),
#      ('[13/Jul', 514),
#      ('[12/Jul', 454),
#      ('[11/Jul', 453),
#      ('[10/Jul', 381),
#      ('[09/Jul', 338),
#      ('[08/Jul', 291),
#      ('[07/Jul', 541),
#      ('[06/Jul', 614),
#      ('[05/Jul', 472),
#      ('[04/Jul', 341),
#      ('[03/Jul', 452),
#      ('[02/Jul', 279),
#      ('[01/Jul', 304),
#      ('[31/Aug', 513),
#      ('[30/Aug', 546),
#      ('[29/Aug', 408),
#      ('[28/Aug', 383),
#      ('[27/Aug', 364),
#      ('[26/Aug', 357),
#      ('[25/Aug', 403),
#      ('[24/Aug', 406),
#      ('[23/Aug', 336),
#      ('[22/Aug', 270),
#      ('[21/Aug', 296),
#      ('[20/Aug', 295),
#      ('[19/Aug', 202),
#      ('[18/Aug', 245),
#      ('[17/Aug', 261),
#      ('[16/Aug', 252),
#      ('[15/Aug', 316),
#      ('[14/Aug', 281),
#      ('[13/Aug', 212),
#      ('[12/Aug', 187),
#      ('[11/Aug', 249),
#      ('[10/Aug', 308),
#      ('[09/Aug', 274),
#      ('[08/Aug', 371),
#      ('[07/Aug', 366),
#      ('[06/Aug', 206),
#      ('[05/Aug', 227),
#      ('[04/Aug', 330),
#      ('[03/Aug', 289),
#      ('[01/Aug', 235)]

# In[74]:


#fiktrando e Map
diaJul = base.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).flatMap(lambda k: k.split(' ')).filter(lambda line: '[' in line).flatMap(lambda line: line.split('/1995')).filter(lambda line: '/' in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[0],False)
diaAug = base2.flatMap(lambda k: k.split('/n')).filter(lambda line: ' 404 ' in line).flatMap(lambda k: k.split(' ')).filter(lambda line: '[' in line).flatMap(lambda line: line.split('/1995')).filter(lambda line: '/' in line).map(lambda m: (m,1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[0],False)


# In[ ]:


#juntando
Resposta4 = diaJul + diaAug
Resposta4.take(80)


# # Quetão 5
#     O total de bytes retornados.
#     r) 65123227715 bytes

# In[109]:


#Tratando e separando dados
#Mapeador split o resultado e elimina não inteiros
def pLine(l):
    campos = l.split(" ")  
    test = 1    
    numBytes = 0 
    try:
        numBytes = int(campos[1])
        return(test, numBytes)
    except:
        return(test, numBytes)
#filtra e concatena
bytesJul = base.flatMap(lambda k: k.split('/n')).flatMap(lambda k: k.split(" - - ")).filter(lambda line: '/1995' in line).flatMap(lambda k: k.split('HTTP')).filter(lambda line: '/1995' not in line).flatMap(lambda k: k.split('" ')).filter(lambda line: r'^[0-9]' not in line).filter(lambda line: '.' not in line)
bytesAug = base2.flatMap(lambda k: k.split('/n')).flatMap(lambda k: k.split(" - - ")).filter(lambda line: '/1995' in line).flatMap(lambda k: k.split('HTTP')).filter(lambda line: '/1995' not in line).flatMap(lambda k: k.split('" ')).filter(lambda line: r'^[0-9]' not in line).filter(lambda line: '.' not in line)
bytesJoin = bytesJul + bytesAug
tete = bytesJoin.map(pLine).reduceByKey(lambda a, b: a + b)


# In[110]:


tete.take(10)

