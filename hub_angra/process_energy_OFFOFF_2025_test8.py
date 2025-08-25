import pandas as pd  #pacotes
import numpy as np   #calculo numerico
import glob          #listas
import pyarrow.parquet as pq
import os, sys
from os import path
import datetime


# para 30 dias


list_OFF1  = [
            '20241117','20241118','20241119','20241120','20241121','20241122','20241123',
            '20241124','20241125','20241126','20241127','20241128','20241129','20241130',
            '20241201'
			]


##
## Somente calcula OFF2
list_OFF2  = [
            '20241208','20241209','20241210','20241211','20241212','20241213',
            '20241214','20241215','20241216','20241217','20241218','20241219',
            '20241220','20241221','20241222'
           ]


		   
bins_interval = [63.134941, 66.91160302, 70.68826504, 74.46492707, 78.24158909,
 82.01825112, 85.79491314, 89.57157517, 93.34823719, 97.12489922,
 100.90156124, 104.67822326, 108.45488529, 112.23154731, 116.00820934, 
 119.78487136, 123.56153339, 127.33819541, 131.11485744, 134.89151946,
 138.66818149, 142.44484351, 146.22150553, 149.99816756, 153.77482958,
 157.55149161, 161.32815363, 165.10481566, 168.88147768, 172.65813971,
 176.43480173, 180.21146375, 183.98812578, 187.7647878, 191.54144983,
 195.31811185, 199.09477388, 202.8714359, 206.64809793, 210.42475995,
 214.20142197, 217.978084, 221.75474602, 225.53140805, 229.30807007,
 233.0847321, 236.86139412, 240.63805615, 244.41471817, 248.19138019]

#print(bins_interval)

def fnt_search_pos (f_pe):
    arr = [i for i, j in enumerate(bins_interval) if j >= f_pe]
    if len(arr)==0:
        res=-1
    else:
        res = arr[0]
    return res

	
def extract_epoch(filename):
  initial = str(filename).find('AngraRun') + 6
  final = str(filename).find('AngraRun') + 24
  sub = str(filename)[initial:final]
  epoch = ''.join(i for i in sub if i.isdigit())
  #print(epoch)
  return(epoch)

def find_interval_date_pair(datein, datefi, sdir):  #enviar formato yyyy-mm-dd
    
    ts_datein = int(datetime.datetime(int(datein[0:4]),int(datein[5:7]),int(datein[8:10]),0,0).timestamp())
    ts_datefi = int(datetime.datetime(int(datefi[0:4]),int(datefi[5:7]),int(datefi[8:10]),23,59).timestamp())
    
    fileparq  = sdir+'/*.parq'  #nova pasta dos processados - 05/04/2021
    listparq = glob.glob(fileparq)
    interval_list = []
    i_fator = 1000  #novo v4 divide por 1000 - 05/04/2021
    
    for file in listparq:
        #print(file)
        i_filedate = int(extract_epoch(file))/i_fator  # somente se não for v2 divide por 1000 (por que?)
        day_filedate = datetime.datetime.fromtimestamp(i_filedate).day
        month_filedate = datetime.datetime.fromtimestamp(i_filedate).month
        year_filedate = datetime.datetime.fromtimestamp(i_filedate).year
        ts_filedate = int(datetime.datetime(year_filedate,month_filedate,day_filedate,0,0).timestamp())
        #print('{:s} >> data/hora run: {:d}-{:d}-{:d}'.format(file,year_filedate,month_filedate,day_filedate))
        if ts_filedate >= ts_datein and ts_filedate <= ts_datefi: 
            interval_list.append(file)
        
    return(interval_list)

def fnt_pe2MeV(x):
    return(3.77E-2*x + 0.62)

def fnt_DUQ2MeV(x):
    return(4.84E-4*x + 0.62)

def fnt_MeV2DUQ(x):
    return((x - 0.62)/4.84E-4)
	
def fnt_process_list_PD (p_list, p_h_PDP, p_h_PDD, p_h_PDT, p_list_bins):
    i_day = 0
    p_totalPositrons = 0
    p_totalNonSat = 0
    p_count_bin = [ [] for y in range(len(p_list)) ]
    p_dp_bin = [ [] for y in range(len(p_list)) ]
    p_dpr_bin = [ [] for y in range(len(p_list)) ]                      

    for sListDt in p_list:
        yyyy=int(sListDt[0:4]); mm=int(sListDt[4:6]); dd=int(sListDt[6:8])
    
        sDate = '{0:d}-{1:0>2d}-{2:0>2d}'.format(yyyy,mm,dd)
        sDir='pair/{0:d}{1:0>2d}{2:0>2d}'.format(yyyy,mm,dd)
        print(sDir)
        lista = find_interval_date_pair(sDate,sDate,sDir)
        p_h_NonSat = []

        for file in lista:
            print(' >>> '+file)
            df = pd.read_parquet(file)
            dff = df[(df['p_satured']==0)&(df['d_satured']==0)
                            &(fnt_pe2MeV(df['p_Total_pe'])>=3)&(fnt_pe2MeV(df['p_Total_pe'])<=10)
                            &(fnt_pe2MeV(df['d_Total_pe'])>=1.6)&(fnt_pe2MeV(df['d_Total_pe'])<=7.12)
                            &(df['d_timeInverval']>=8)&(df['d_timeInverval']<=50)
                            &(df['d_mPMTs']>=10)
                    ]
            p_h_PDP.extend(dff['p_Total_pe'].values)
            p_h_PDD.extend(dff['d_Total_pe'].values)
            p_h_PDT.extend(dff['d_timeInverval'].values)

            dfns = df[(df['p_satured']==0)&(df['d_satured']==0)]

            p_totalPositrons += df.count()[0]
            p_totalNonSat += dfns.count()[0]

            p_h_NonSat.extend(dff['p_Total_pe'].values)            

            #print(p_totalPositrons)
            #break
                
        print('calculando bin')
                      
        p_bin_NonSat = [ [] for y in range(len(p_list_bins)) ]
                      
        ## 1- Navegar na lista de prompt dos eventos filtrados e separá-los em bins de pe
        for iw in range(len(p_h_NonSat)):
            i_b_pos = fnt_search_pos(p_h_NonSat[iw])
            if (i_b_pos!=-1):
                p_bin_NonSat[i_b_pos].append(p_h_NonSat[iw])
        
        ## 2- Navegar na lista de bins e contar quantos eventos ocorreram no dia
        for sb in range(len(p_list_bins)):
            p_count_bin[i_day].append(len(p_bin_NonSat[sb]))

        i_day += 1
        #break   ## Somente 1 dia de runs

    print('calculando sigmas')
    p_sigma_bin = []
    for sbr in range(len(p_list_bins)):
        cday_NSat = []
        for dd in range(i_day):
            cday_NSat.append(p_count_bin[dd][sbr])
         
        p_sigma_bin.append(np.sum(cday_NSat))  # contagem de eventos/dia

    return p_totalPositrons, p_totalNonSat, p_sigma_bin #, p_sigmar_bin, p_std_bin

	
##save values
def save_PD (dh_PDP, dh_PDD, dh_PDT, dh_Sig, sName):
    # dictionary of lists
    dict = {'dh_PDP': dh_PDP,
            'dh_PDD': dh_PDD,
            'dh_PDT': dh_PDT}
    
    dict2 = {'dh_Sig': dh_Sig}
 
    # creating a dataframe from dictionary
    dfw = pd.DataFrame(dict)
    dfw.to_parquet('pair/Analise_ONOFF/'+sName+'.parq', compression='gzip')

    dfw2 = pd.DataFrame(dict2)
    dfw2.to_parquet('pair/Analise_ONOFF/Sig_'+sName+'.parq', compression='gzip')

    del dict2
    del dict
    
##load values
def load_PD (sName): #(dh_PDP, dh_PDD, dh_PDT, dh_Sig, sName):
    dfr = pd.read_parquet('pair/Analise_ONOFF/'+sName+'.parq')
    dfr2 = pd.read_parquet('pair/Analise_ONOFF/Sig_'+sName+'.parq')
    return(dfr['dh_PDP'].values, dfr['dh_PDD'].values, dfr['dh_PDT'].values, dfr2['dh_Sig'].values)

print('Inicio',datetime.datetime.now())

h_PDP_OFF1 = []
h_PDD_OFF1 = []
h_PDT_OFF1 = []
h_Sig_OFF1 = []

totalPositrons_OFF1 = 0
totalNonSat_OFF1 = 0

#totalPositrons_OFF1, totalNonSat_OFF1, h_Sig_OFF1 = fnt_process_list_PD(list_OFF1, h_PDP_OFF1, h_PDD_OFF1, h_PDT_OFF1, bins_interval)

print(len(h_PDP_OFF1))
print(len(h_PDD_OFF1))
print(len(h_PDT_OFF1))
print('Em ',len(list_OFF1),' dias: ',totalPositrons_OFF1)
print('Por dia: ',totalPositrons_OFF1/len(list_OFF1))
print('Em Hz: ',totalPositrons_OFF1/len(list_OFF1)/86400) 
print('Em ',len(list_OFF1),' dias (NS): ',totalNonSat_OFF1)
print('Por dia (NS): ',totalNonSat_OFF1/len(list_OFF1))
print('Em Hz (NS): ',totalNonSat_OFF1/len(list_OFF1)/86400) 

hg_Sig_OFF1 = [(x**0.5) for x in h_Sig_OFF1]
print(hg_Sig_OFF1)

print('Fim',datetime.datetime.now())


print('Inicio',datetime.datetime.now())

h_PDP_OFF2 = []
h_PDD_OFF2 = []
h_PDT_OFF2 = []
h_Sig_OFF2 = []

totalPositrons_OFF2 = 0
totalNonSat_OFF2 = 0

totalPositrons_OFF2, totalNonSat_OFF2, h_Sig_OFF2 = fnt_process_list_PD(list_OFF2, h_PDP_OFF2, h_PDD_OFF2, h_PDT_OFF2, bins_interval)

print(len(h_PDP_OFF2))
print(len(h_PDD_OFF2))
print(len(h_PDT_OFF2))
print('Em ',len(list_OFF2),' dias: ',totalPositrons_OFF2)
print('Por dia: ',totalPositrons_OFF2/len(list_OFF2))
print('Em Hz: ',totalPositrons_OFF2/len(list_OFF2)/86400) 
print('Em ',len(list_OFF2),' dias (NS): ',totalNonSat_OFF2)
print('Por dia (NS): ',totalNonSat_OFF2/len(list_OFF2))
print('Em Hz (NS): ',totalNonSat_OFF2/len(list_OFF2)/86400) 

hg_Sig_OFF2 = [(x**0.5) for x in h_Sig_OFF2]
print(hg_Sig_OFF2)

print('Fim',datetime.datetime.now())


i_day = 15

hg_Sig_OFF1 = [((x*i_day)**0.5) for x in h_Sig_OFF1]     ## x é a quantidade do bin / quantidade de dias 
hg_Sig_OFF2 = [((x*i_day)**0.5) for x in h_Sig_OFF2] 

#save_PD (h_PDP_OFF1, h_PDD_OFF1, h_PDT_OFF1, hg_Sig_OFF1, 'PD_OFF1_15_m10_test')
save_PD (h_PDP_OFF2, h_PDD_OFF2, h_PDT_OFF2, hg_Sig_OFF2, 'PD_OFF8_15_m10_test')

print('FINAL',datetime.datetime.now())

