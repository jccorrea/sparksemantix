"""
#Julio Cesar Correa
# 06 Apr 2020 
"""
# -*- coding: utf-8 -*-
#python 3

"""
#STEP 1 -- Trabalhar o dataframe de JUL  AUG 95 de forma separada
"""

# Import modules
import pandas as pd
import shlex
import re
import fileinput

# Set ipython's max row display
pd.set_option('display.max_row', 1000)
# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)

# JUL95
# removo dash character and coloco espaço
with fileinput.FileInput('/Users/dal/Documents/Semantix/desafio/NASA_access_log_Jul95', inplace=True, backup='.bak') as file:
     for line in file:
     	print(line.replace(' - - ', ' '), end='')

#quebro por espaço pulando os caracteres de espaço dentro da string com aspas 
file1 = open('/Users/dal/Documents/Semantix/desafio/NASA_access_log_Jul95')
li_log_Jul = []
for line in file1:
	li_log_Jul.append(shlex.split(line))

#trabalhando com dataset no pandas
df_Jul = pd.DataFrame(li_log_Jul) 
df_Jul.columns = ['host','timestamp','gmt','url','code','bytes']


# AUG95
# removo dash character and coloco espaço
with fileinput.FileInput('/Users/dal/Documents/Semantix/desafio/NASA_access_log_Aug95', inplace=True, backup='.bak') as file:
     for line in file:
     	print(line.replace(' - - ', ' '), end='')
#quebro por espaço pulando os caracteres de espaço dentro da string com aspas 
file2 = open('/Users/dal/Documents/Semantix/desafio/NASA_access_log_Aug95')
li_log_Aug = []
for line in file2:
	li_log_Aug.append(re.split(' ', line))

# ajustes, limpeza e formatação no dataframe temporario
# A ideia é deixar os dois dataframes parecidos para a concatenacao

df_1 = pd.DataFrame(li_log_Aug) 
df_1 = df_1.iloc[:, 0:8]
df_1.columns = ['host','timestamp','gmt','url','url2','url3','code','bytes']
df_1['url'] = df_1['url'] + ' ' + df_1['url2']+ ' ' + df_1['url3']
df_1.drop(['url2', 'url3'], axis=1,inplace=True)
df_1['url'] = df_1['url'].str.replace('"', '')
df_1['bytes'] = df_1.bytes.str.replace('[^0-9]+', '', regex=True)

df_Aug = df_1

"""
#STEP 2 -  Concatenate, standardizing
"""

#concatena os dataframes
df = df_Jul.append(df_Aug)

#padrozina a coluna de codigos
df['code'] = df_1.code.str.replace('[^0-9]+', '', regex=True)

#coluna bytes para numerico
df['bytes'] = pd.to_numeric(df_1.bytes.str.replace('[^0-9]+', '', regex=True))
# cria uma coluna de dia
df['day'] = df.timestamp.str[2:10] 

"""
#STEP 3 - Reponder as 5 questões
"""

### Q1 - Número​ ​de​ ​hosts​ ​únicos

df.host.nunique()
#R: 42141


### Q2 -​total​ ​de​ ​erros​ ​404.

# usando filtro boleano baseado em series

seriesFilter = df.apply(lambda x: True if x['code'] =='404' else False , axis=1)
# Count number of True in series
numRows = len(seriesFilter[seriesFilter == True].index)
print(numRows)
#R: 4.587

### Q3 - Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​404.

#filtra 404
df_404 = df[df['code']=="404"]
print(medal_counts.head(15))

"""
df_404_url.groupby(['url']).agg('count')
df_404_url['count'] = df_404_url.groupby('url')['code'].value_counts()
df_404_url["rank"] = df_404_url.groupby("url").rank("dense", ascending=False)
df["rank"] = df.groupby("group_ID")["value"].rank("dense", ascending=False)



### Q4 - Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia.

df_404_1 = df_404[['day','code']].copy()
df_404_1['day'] = pd.to_datetime(df_404_1.day,errors='coerce')

#R: df_404_1.groupby('day').count() 

### Q5 - O​ ​total​ ​de​ ​bytes​ ​retornados.

np.sum(df['bytes'])
#R: 12851849323.0
"""
df['bytes'] = df['bytes'].astype(str)
df['bytes'] = df['bytes'].astype('str')
df['bytes'] = df['bytes'].str.replace('-', '')
df['bytes'] = pd.to_numeric(df['bytes'], errors='coerce')
"""


