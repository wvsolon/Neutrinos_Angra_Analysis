import pandas as pd  #pacotes
import numpy as np   #calculo numerico
import glob          #listas
import pyarrow.parquet as pq
import os, sys       #funcoes para gravação de arquivos
from os import path
import datetime      #funcao datetime


#find a y value in list patches from x value
def fnc_find_y(patches, value_x):
    value_y = 0
    for tupla in patches:
        #print(tupla[0],tupla[1],tupla)
        if (value_x < tupla[0]):
            value_y = tupla[1]
            break
    #print(value_x, value_y)
    return value_y

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

pat_prompt = np.array([[5.00000000e+00, 0.00000000e+00],
 [5.00000000e+00, 1.83494293e-01],
 [2.71000000e+01, 1.83494293e-01],
 [2.71000000e+01, 2.49341528e-01],
 [4.92000000e+01, 2.49341528e-01],
 [4.92000000e+01, 2.27392450e-01],
 [7.13000000e+01, 2.27392450e-01],
 [7.13000000e+01, 1.61545215e-01],
 [9.34000000e+01, 1.61545215e-01],
 [9.34000000e+01, 9.04302019e-02],
 [1.15500000e+02, 9.04302019e-02],
 [1.15500000e+02, 5.17998244e-02],
 [1.37600000e+02, 5.17998244e-02],
 [1.37600000e+02, 1.93151888e-02],
 [1.59700000e+02, 1.93151888e-02],
 [1.59700000e+02, 9.65759438e-03],
 [1.81800000e+02, 9.65759438e-03],
 [1.81800000e+02, 6.14574188e-03],
 [2.03900000e+02, 6.14574188e-03],
 [2.03900000e+02, 8.77963126e-04],
 [2.26000000e+02, 8.77963126e-04],
 [2.26000000e+02, 0.00000000e+00]])


pat_delay = [[5.00000000e+00, 0.00000000e+00],
 [5.00000000e+00, 2.32890705e-01],
 [3.53750000e+01, 2.32890705e-01],
 [3.53750000e+01, 2.06332993e-01],
 [6.57500000e+01, 2.06332993e-01],
 [6.57500000e+01, 1.69560776e-01],
 [9.61250000e+01, 1.69560776e-01],
 [9.61250000e+01, 1.24616956e-01],
 [1.26500000e+02, 1.24616956e-01],
 [1.26500000e+02, 1.04187947e-01],
 [1.56875000e+02, 1.04187947e-01],
 [1.56875000e+02, 8.98876404e-02],
 [1.87250000e+02, 8.98876404e-02],
 [1.87250000e+02, 5.00510725e-02],
 [2.17625000e+02, 5.00510725e-02],
 [2.17625000e+02, 2.24719101e-02],
 [2.48000000e+02, 2.24719101e-02],
 [2.48000000e+02, 0.00000000e+00]]

NB_time = [0.0282730710376791, 0.028049907071015356, 0.027814099748956822, 0.027565197876110822, 0.027302769346325185, 
  0.02702640664409396, 0.026735732760965858, 0.026430407487387453, 0.026110134024874627, 0.02577466584656794, 
  0.025423813716469464, 0.02505745275948967, 0.02467552945650048, 0.024278068421670688, 0.023865178804345574, 
  0.023437060145601204, 0.022994007511378867, 0.022536415720799133, 0.022064782490803334, 0.021579710327455686, 
  0.021081907010613167, 0.020572184542474267, 0.020051456461619228, 0.019520733461995, 0.018981117299874267, 
  0.018433793019686253, 0.017880019579944523, 0.017321119011140713, 0.01675846428609889, 0.01619346612751042, 
  0.015627559014923728, 0.015062186682347538, 0.014498787416264678, 0.013938779471210246, 0.013383546915730763, 
  0.012834426205753747, 0.01229269375603857, 0.011759554744896705, 0.01123613334466454, 0.010723464522700283, 
  0.01022248750732744, 0.009734040962522074, 0.00925885986644426, 0.008797574044069556, 0.00835070826473334, 
  0.007918683782479981, 0.007501821171382814, 0.007100344289708897, 0.006714385195794519, 0.006343989834294592, 
  0.005989124313341934, 0.00564968160019672, 0.005325488474183615, 0.0050163125900771965, 0.004721869521611717, 
  0.004441829672540728, 0.004175824960847236, 0.003923455199624276, 0.0036842941152637936, 0.0034578949594965243, 
  0.003243795686231693, 0.0030415236768787054, 0.002850600008816709, 0.002670543270912977, 0.0025008729375404857, 
  0.002341112318517168, 0.0021907911069228257, 0.002049447550001747, 0.0019166302704952576, 0.0017918997669333048, 
  0.0016748296218072067, 0.0015650074462949372, 0.0014620355894510483, 0.0013655316386258097, 0.0012751287364472304, 
  0.001190475738074836, 0.0011112372306907267, 0.0010370934353929795, 0.0009677400098485296, 0.000902887768286059, 
  0.0008422623336934618, 0.0007856037354507559, 0.000732665964092614, 0.0006832164934642748, 0.0006370357792154441, 
  0.0005939167413701697, 0.0005536642376152341, 0.0005160945329620025, 0.0004810347705519109, 0.00044832244758807806, 
  0.00041780489967827315, 0.0003893387962608198, 0.0003627896492480046, 0.000338031336554269, 0.0003149456417721949, 
  0.00029342181091169324, 0.00027335612682082374, 0.00025465150165467776, 0.00023721708754662228, 0.0002209679054590724]

##########################################
##  PROCESSO PRINCIPAL ##
##########################################

#yyyy=2024; mm=10; dd=1


isDate = '{0:d}-{1:0>2d}-{2:0>2d}'.format(2024,10,1)
fsDate = '{0:d}-{1:0>2d}-{2:0>2d}'.format(2025,2,28)

sDir = '/dados/Angra/processed/2024_stop'
#sDir = './2024_stop'
#sDir='data/{0:d}{1:0>2d}{2:0>2d}'.format(yyyy,mm,dd)
#os.mkdir(sDir)

lista = find_interval_date_pair(isDate,fsDate,sDir)
#print(list)

indf=1 ###

prompt_mean=61.45215100965759
prompt_var=1337.7332240825515
prompt_std=36.57503553084469

delay_mean=87.81511746680286
delay_var=3577.5317020035664 
delay_std=59.81247112436976

time_mean = 13 #(em us)
time_var = 5**2
time_std = 5

pair=[[] for x in range(80)]  
event_pair = 0 
subtotal_regs = 0 
total_regs = 0 
pairset = 1  

print(datetime.datetime.now())


for file in lista:
    #print(file)
    df = pd.read_parquet(file)
    i_epoc = int(extract_epoch(file))/1000
    dd=datetime.datetime.fromtimestamp(i_epoc).day
    mm=datetime.datetime.fromtimestamp(i_epoc).month
    yyyy=datetime.datetime.fromtimestamp(i_epoc).year
    print(indf,file,dd,mm,yyyy)
    #print(file[28:60]+'_pair.parq')
    #break
    
    vd = str(datetime.datetime.fromtimestamp(i_epoc).day)+'-'+str(datetime.datetime.fromtimestamp(i_epoc).month)
    
    ev2 = [0 for x in range(80)] #[0,0,0,0,0]
    
    dfl = df.values.tolist()
    subtotal_regs += len(dfl)
    total_regs += len(dfl)
    
    for index1 in range(len(dfl)):
        if (index1!=0):
            
            regfix = dfl[index1].copy()
            ev1 = ev2.copy()
            
            #ev2[0] = regfix['Total_p_charge']/77.96 #em  pe
            ev2[0] = regfix[7]/77.96 #em  pe
            #ev2[1] = regfix['Timestamp1']*16/1E3 #em us
            ev2[1] = regfix[1]*16/1E3 #em us

            #ev2[2] = fnc_find_y(p[0].get_xy(),ev1[0]) #probabilidade de ocorrência nesta energia (pelo PDF prompt)
            ev2[2] = fnc_find_y(pat_prompt,ev1[0]) #probabilidade de ocorrência nesta energia (pelo PDF prompt)
            #ev2[3] = fnc_find_y(d[0].get_xy(),ev2[0]) #probabilidade de ocorrência nesta energia (pelo PDF delay)
            ev2[3] = fnc_find_y(pat_delay,ev2[0]) #probabilidade de ocorrência nesta energia (pelo PDF delay)
            ev2[4] = 0
            #totalTime_ON += ev2[1]
            if (int(ev2[1])<100):
                #ev2[4] = y_dataNB[int(ev2[1])] #probabilidade temporal (pelo densidade de tempo)
                ev2[4] = NB_time[int(ev2[1])] #probabilidade temporal (pelo densidade de tempo)
            
            #print('patches: ',ev2[2],ev2[3],ev2[4])
            #ev2[5] = regfix['PMT01']; ev2[6] = regfix['PMT02']; ev2[7] = regfix['PMT03']; ev2[8] = regfix['PMT04']
            #ev2[9] = regfix['PMT05']; ev2[10] = regfix['PMT06']; ev2[11] = regfix['PMT07']; ev2[12] = regfix['PMT08']
            #ev2[13] = regfix['PMT09']; ev2[14] = regfix['PMT10']; ev2[15] = regfix['PMT11']; ev2[16] = regfix['PMT12']
            #ev2[17] = regfix['PMT13']; ev2[18] = regfix['PMT14']; ev2[19] = regfix['PMT15']; ev2[20] = regfix['PMT16']
            #ev2[21] = regfix['PMT17']; ev2[22] = regfix['PMT18']; ev2[23] = regfix['PMT19']; ev2[24] = regfix['PMT20']
            #ev2[25] = regfix['PMT21']; ev2[26] = regfix['PMT22']; ev2[27] = regfix['PMT23']; ev2[28] = regfix['PMT24']
            #ev2[29] = regfix['PMT25']; ev2[30] = regfix['PMT26']; ev2[31] = regfix['PMT27']; ev2[32] = regfix['PMT28']
            #ev2[33] = regfix['PMT29']; ev2[34] = regfix['PMT30']; ev2[35] = regfix['PMT31']; ev2[36] = regfix['PMT32']
            ev2[5] = regfix[8]; ev2[6] = regfix[9]; ev2[7] = regfix[10]; ev2[8] = regfix[11]
            ev2[9] = regfix[12]; ev2[10] = regfix[13]; ev2[11] = regfix[14]; ev2[12] = regfix[15]
            ev2[13] = regfix[16]; ev2[14] = regfix[17]; ev2[15] = regfix[18]; ev2[16] = regfix[19]
            ev2[17] = regfix[20]; ev2[18] = regfix[21]; ev2[19] = regfix[22]; ev2[20] = regfix[23]
            ev2[21] = regfix[24]; ev2[22] = regfix[25]; ev2[23] = regfix[26]; ev2[24] = regfix[27]
            ev2[25] = regfix[28]; ev2[26] = regfix[29]; ev2[27] = regfix[30]; ev2[28] = regfix[31]
            ev2[29] = regfix[32]; ev2[30] = regfix[33]; ev2[31] = regfix[34]; ev2[32] = regfix[35]
            ev2[33] = regfix[36]; ev2[34] = regfix[37]; ev2[35] = regfix[38]; ev2[36] = regfix[39]

            #ev2[37] = regfix['Saturated']; ev2[38] = regfix['Event_Number']
            ev2[37] = regfix[40]; ev2[38] = regfix[0]
            
            #print('2',datetime.datetime.now())
            qtde_PMTs = 0
            for ill in range(32):
                #campoPMT = 'PMT{0:0>2d}'.format(ill+1)
                #if (regfix[campoPMT]>77):
                if (ev2[5+ill]>77):
                    qtde_PMTs += 1
            ev2[39] = qtde_PMTs
            
            if (ev1[0]==0):
                continue
                    
            #print('3',datetime.datetime.now())
            i=int(ev1[0]) #prompt int energy
            j=int(ev2[0]) #delay int energy
            t=int(ev2[1]) #time interval - 13/09/2021
   
            ### chi sqr
            chi_sq_calc = ((i - prompt_mean)**2)/prompt_mean + ((j - delay_mean)**2)/delay_mean + ((t - time_mean)**2)/time_mean
                       
            event_pair+=1

            pair[0].append(event_pair)

            pair[1].append(ev1[5]); pair[2].append(ev1[6]);  pair[3].append(ev1[7]);  pair[4].append(ev1[8])
            pair[5].append(ev1[9]); pair[6].append(ev1[10]); pair[7].append(ev1[11]); pair[8].append(ev1[12])
            pair[9].append(ev1[13]);pair[10].append(ev1[14]);pair[11].append(ev1[15]);pair[12].append(ev1[16])
            pair[13].append(ev1[17]);pair[14].append(ev1[18]);pair[15].append(ev1[19]);pair[16].append(ev1[20])
            pair[17].append(ev1[21]);pair[18].append(ev1[22]);pair[19].append(ev1[23]);pair[20].append(ev1[24])
            pair[21].append(ev1[25]);pair[22].append(ev1[26]);pair[23].append(ev1[27]);pair[24].append(ev1[28])
            pair[25].append(ev1[29]);pair[26].append(ev1[30]);pair[27].append(ev1[31]);pair[28].append(ev1[32])
            pair[29].append(ev1[33]);pair[30].append(ev1[34]);pair[31].append(ev1[35]);pair[32].append(ev1[36])
            
            pair[33].append(ev1[0] * 77.96)  # em DUQ
            pair[34].append(ev1[0])  #em pe
            pair[35].append(ev1[2])  #PDF do prompt
            pair[36].append(ev1[39])  #multiplicidade > 77DUQ
            pair[37].append(ev1[37])  #satured
            
            pair[38].append(ev2[5]); pair[39].append(ev2[6]);  pair[40].append(ev2[7]);  pair[41].append(ev2[8])
            pair[42].append(ev2[9]); pair[43].append(ev2[10]); pair[44].append(ev2[11]); pair[45].append(ev2[12])
            pair[46].append(ev2[13]);pair[47].append(ev2[14]);pair[48].append(ev2[15]);pair[49].append(ev2[16])
            pair[50].append(ev2[17]);pair[51].append(ev2[18]);pair[52].append(ev2[19]);pair[53].append(ev2[20])
            pair[54].append(ev2[21]);pair[55].append(ev2[22]);pair[56].append(ev2[23]);pair[57].append(ev2[24])
            pair[58].append(ev2[25]);pair[59].append(ev2[26]);pair[60].append(ev2[27]);pair[61].append(ev2[28])
            pair[62].append(ev2[29]);pair[63].append(ev2[30]);pair[64].append(ev2[31]);pair[65].append(ev2[32])
            pair[66].append(ev2[33]);pair[67].append(ev2[34]);pair[68].append(ev2[35]);pair[69].append(ev2[36])
            
            pair[70].append(ev2[0] * 77.96)  # em DUQ
            pair[71].append(ev2[0])  #em pe
            pair[72].append(ev2[3])  #PDF do delay
            pair[73].append(ev2[39])  #multiplicidade > 77DUQ
            pair[74].append(ev2[37])  #satured
            
            pair[75].append(ev2[1])  #tempo
            pair[76].append(chi_sq_calc)  #chi^2
            pair[77].append(ev2[4])  #PDF do tempo

            pair[78].append(file)  #arquivo AngraRun
            #pair[78].append('')  #arquivo AngraRun
            pair[79].append(ev1[38])  #EventRun
            
            #print('4',datetime.datetime.now())
            #if (event_pair>10):
                #break

    if ((indf % 1)==0)or(indf>=len(lista)):  # mod 1 (antes era mod 6), pois quero gerar individuais agora - 06/10/2021
        # dictionary of lists
        dict = {'Pair': pair[0],
                'p_PMT01': pair[1],  'p_PMT02': pair[2],  'p_PMT03': pair[3],  'p_PMT04': pair[4], 
                'p_PMT05': pair[5],  'p_PMT06': pair[6],  'p_PMT07': pair[7],  'p_PMT08': pair[8], 
                'p_PMT09': pair[9],  'p_PMT10': pair[10], 'p_PMT11': pair[11], 'p_PMT12': pair[12], 
                'p_PMT13': pair[13], 'p_PMT14': pair[14], 'p_PMT15': pair[15], 'p_PMT16': pair[16], 
                'p_PMT17': pair[17], 'p_PMT18': pair[18], 'p_PMT19': pair[19], 'p_PMT20': pair[20], 
                'p_PMT21': pair[21], 'p_PMT22': pair[22], 'p_PMT23': pair[23], 'p_PMT24': pair[24], 
                'p_PMT25': pair[25], 'p_PMT26': pair[26], 'p_PMT27': pair[27], 'p_PMT28': pair[28], 
                'p_PMT29': pair[29], 'p_PMT30': pair[30], 'p_PMT31': pair[31], 'p_PMT32': pair[32], 
                'p_Total_charge': pair[33],'p_Total_pe': pair[34],'p_PDF': pair[35], 'p_mPMTs': pair[36], 'p_satured': pair[37],
                'd_PMT01': pair[38], 'd_PMT02': pair[39], 'd_PMT03': pair[40], 'd_PMT04': pair[41], 
                'd_PMT05': pair[42], 'd_PMT06': pair[43], 'd_PMT07': pair[44], 'd_PMT08': pair[45], 
                'd_PMT09': pair[46], 'd_PMT10': pair[47], 'd_PMT11': pair[48], 'd_PMT12': pair[49], 
                'd_PMT13': pair[50], 'd_PMT14': pair[51], 'd_PMT15': pair[52], 'd_PMT16': pair[53], 
                'd_PMT17': pair[54], 'd_PMT18': pair[55], 'd_PMT19': pair[56], 'd_PMT20': pair[57], 
                'd_PMT21': pair[58], 'd_PMT22': pair[59], 'd_PMT23': pair[60], 'd_PMT24': pair[61], 
                'd_PMT25': pair[62], 'd_PMT26': pair[63], 'd_PMT27': pair[64], 'd_PMT28': pair[65], 
                'd_PMT29': pair[66], 'd_PMT30': pair[67], 'd_PMT31': pair[68], 'd_PMT32': pair[69], 
                'd_Total_charge': pair[70],'d_Total_pe': pair[71],'d_PDF': pair[72], 'd_mPMTs': pair[73], 'd_satured': pair[74],
                'd_timeInverval': pair[75],'chi_square': pair[76],'t_PDF': pair[77],
                'file_Run': pair[78], 'Event_Number': pair[79]
               }

        sDir_pair = './pair/{0:d}{1:0>2d}{2:0>2d}'.format(yyyy,mm,dd)
        os.makedirs(sDir_pair, exist_ok=True)

        # creating a dataframe from dictionary
        dfw = pd.DataFrame(dict)
        #dfw.to_parquet('data/AngraRun_Pair_{0:d}-{1:0>2d}-{2:0>2d}_{3:d}.parq'.format(yyyy,mm,dd,pairset))
        #dfw.to_parquet(sDir_pair+'/'+file[12:44]+'_pair.parq', compression='gzip')  #./2024_stop/##12
        dfw.to_parquet(sDir_pair+'/'+file[33:65]+'_pair.parq', compression='gzip')  #/dados/Angra/processed/2024_stop/##33

        print('Subotal de registros: '+str(subtotal_regs))
        pair=[[] for x in range(80)]
        event_pair = 0
        subtotal_regs = 0
        pairset += 1
        del dict
        
    
    indf+=1
    
            
    
print('Total de registros: '+str(total_regs))

print(datetime.datetime.now())
print('Fim')