# -*- coding: utf-8 -*-
"""
Created on Sat Feb  6 13:19:11 2021

@author: LJY
"""

import pandas as pd
import random
import numpy as np
import matplotlib.pyplot as plt
import time
import pickle as pk
import gc

import multiprocessing as mp


#%%
def Year_structure(age):#根据年龄定义每个个体的易感性
    """ age: 20m*1,  susc: 20m*1  """
    susc = np.ones(len(age))
    susc[age['age']<=14] = 0.34
    susc[age['age']>64] = 1.47   
    return np.array(susc)

def split(full_list,shuffle,ratio):
    n_total = len(full_list)
    offset = round(n_total * ratio)
    if n_total==0 or offset<1:
        return [],full_list
    if shuffle:
        random.shuffle(full_list)
        sublist_1 = full_list[:offset]
        sublist_2 = full_list[offset:]
        return sublist_1,sublist_2

def patient_zero(N,numI,r):#随机生成初始感染者
    numA = int(numI*(1-r)/r)
    num = numI+numA
    Infecters = random.sample(range(N),num) 
    """ InfectStatus: 20m*1  """
    InfectStatus = np.zeros(N,int)        
    IndexI,IndexA=split(Infecters,True,r)    
    for i in IndexI:
        InfectStatus[i] = 1 
    for i in IndexA:
        InfectStatus[i] = 2                  
    return InfectStatus

def RMij(Mij,Age_struc):#生成3×3的接触矩阵
    
    Mij0_14=Mij['0-4']+Mij['5-9']+Mij['10-14']
    P0_14= Age_struc['P'][8:11]
    P0_14=P0_14/sum(P0_14)
    m11=np.dot(Mij0_14[0:3],P0_14)
    P15_64= Age_struc['P'][11:21]
    P15_64=P15_64/sum(P15_64)
    m21=np.dot(Mij0_14[3:13],P15_64)
    m31=Mij0_14[13]
    
    Mij15_64=Mij['15-19']+Mij['20-24']+Mij['25-29']+Mij['30-34']+Mij['35-39']+Mij['40-44']+Mij['45-49']+Mij['50-54']+Mij['55-59']+Mij['60-64']
    m12=np.dot(Mij15_64[0:3],P0_14)
    m22=np.dot(Mij15_64[3:13],P15_64)
    m32=Mij15_64[13]
    
    Mij65=Mij['65+']
    m13=np.dot(Mij65[0:3],P0_14)
    m23=np.dot(Mij65[3:13],P15_64)
    m33=Mij65[13]
    Mij3=np.array([[m11,m12,m13],[m21,m22,m23],[m31,m32,m33]])
    return Mij3

def Info2SIAR(Info,a):#根据初始信息（Info)生成0时刻的SIARa; 记录了在0时刻,每个子区域内, a(b,c类似)年龄段个体的S,I,A,R的数量和总数
    """ M: number of sub-areas """
    """ Persona: 10m*5 """
    Persona = Info[Info['Susc']==a]
    x=pd.DataFrame(Persona)
    x.loc[:,'Count']=1
    """ place: 170000*1 """
    place = list(range(1,M+1,1))
    """ S: 170000*1, I:170000*1, A:170000*1, R:170000*1 """
    S = np.zeros(M)
    I = np.zeros(M)
    A = np.zeros(M)
    R = np.zeros(M)
    """ SIARa: 170000*5  """
    SIARa = pd.DataFrame({'Area0':place,'S':S,'I':I,'A':A,'R':R})
    """ data1: 170000*6 """
    data1 = Persona.groupby(['Area0','Status']).agg({'Count':sum})
    data1 = data1.reset_index()

    temp = data1[(data1['Status']==0)]
    """ x: 170000*6 """
    x = pd.merge(temp, SIARa,how='right',on=['Area0'],sort='False')    
    x = x.fillna(0)
    SIARa['S'] = x['Count']
    
    temp = data1[(data1['Status']==1)]
    """ x: 170000*6 """
    x = pd.merge(temp, SIARa,how='right',on=['Area0'],sort='False')    
    x = x.fillna(0)
    SIARa['I'] = x['Count']
    
    temp = data1[(data1['Status']==2)]
    """ x: 170000*6 """
    x = pd.merge(temp, SIARa,how='right',on=['Area0'],sort='False')    
    x = x.fillna(0)
    SIARa['A'] = x['Count']
    
    SIARa['NUM'] = SIARa['S']+SIARa['I']+SIARa['A']+SIARa['R']    
    return SIARa

#kind=0,1,2表示第几类人群
def SIAR(SIARkind,kind,susckind,SIARa,SIARb,SIARc,Mij3,beta,gamma,alpha,r,dt):#模拟dt时间段内的感染过程
    m11 = Mij3[kind,0]
    m12 = Mij3[kind,1]
    m13 = Mij3[kind,2]
    #m21 = Mij3[1,0]
    #m22 = Mij3[1,1]
    #m23 = Mij3[1,2]
    #m31 = Mij3[2,0]
    #m32 = Mij3[2,1]
    #m33 = Mij3[2,2]
    """ EdS: 170000*1, EdS2I: 170000*1, EdS2A: 170000*1, EdI2R: 170000*1, EdA2R: 170000*1"""
    EdS = -dt * susckind * beta * SIARkind['S'] * \
          ((m11*SIARa['I']/SIARa['NUM']) + alpha*(m11*SIARa['A']/SIARa['NUM'])\
          +(m12*SIARb['I']/SIARb['NUM']) + alpha*(m12*SIARb['A']/SIARb['NUM'])\
          +(m13*SIARc['I']/SIARc['NUM']) + alpha*(m13*SIARc['A']/SIARc['NUM']))
    EdS2I = -r*EdS
    EdS2A = -(1-r)*EdS
    EdI2R = dt*gamma * SIARkind['I']
    EdA2R = dt*gamma * SIARkind['A']
    
    EdS2I[EdS2I<0]=0
    EdS2A[EdS2A<0]=0
    EdI2R[EdI2R<0]=0
    EdA2R[EdA2R<0]=0
    EdS2I=EdS2I.fillna(0)
    EdS2A=EdS2A.fillna(0)
    EdI2R=EdI2R.fillna(0)
    EdA2R=EdA2R.fillna(0)
    """ dS2I: 170000*1, dS2A: 170000*1, dI2R: 170000*1, dA2R: 170000*1"""
    dS2I = np.random.poisson(EdS2I)
    dS2A = np.random.poisson(EdS2A)
    dI2R = np.random.poisson(EdI2R)
    dA2R = np.random.poisson(EdA2R)
    
    dS2I = np.rint(dS2I)
    dS2A = np.rint(dS2A)
    dI2R = np.rint(dI2R)
    dA2R = np.rint(dA2R)
    
    dI2R = np.minimum(dI2R,SIARkind['I'])
    dA2R = np.minimum(dA2R,SIARkind['A'])
    
    """ dS: 170000*1, dI: 170000*1, dA: 170000*1, dR: 170000*1 """
    dS = -(dS2I+dS2A)
    dI = dS2I-dI2R
    dA = dS2A-dA2R
    dR = dI2R+dA2R
    """ SIARkindnew: 170000*5"""
    #SIARkindnew = pd.DataFrame({'S':SIARkind['S']+np.rint(dS),'I':SIARkind['I']+np.rint(dI),'A':SIARkind['A']+np.rint(dA),'R':SIARkind['R']+np.rint(dR),'NUM':SIARkind['NUM']}) 
    SIARkindnew = pd.DataFrame({'S':SIARkind['S']+dS,'I':SIARkind['I']+dI,'A':SIARkind['A']+dA,'R':SIARkind['R']+dR,'NUM':SIARkind['NUM']})       
      
    return SIARkindnew,dS2I
def Dij(M):
    m = int(M*M)
    D = np.random.rand(m) 
    D = D.reshape(M,M)
    D = D/D.sum(axis=0)
    
    return D

def Move(SIARkind,D):#模拟个体在子区域间的移动
    pI = 1
    pA = 1
    """ Snew: 170000*1, Inew: 170000*1, Anew: 170000*1, Rnew: 170000*1, NUM: 170000*1"""
    Snew = D.dot(SIARkind['S'])
    Inew = D.dot(SIARkind['I']*pI)
    Anew = D.dot(SIARkind['A']*pA)
    Rnew = D.dot(SIARkind['R'])
    NUM = Snew+Inew+Anew+Rnew
    """ SIARkindnew: 170000*5 """
    SIARkindnew = pd.DataFrame({'S':Snew,'I':Inew,'A':Anew,'R':Rnew,'NUM':NUM})
    return SIARkindnew

def Confirm_case(NewCases,step,dt,days):#根据每日新增感染人数生成每日新增确诊病例数
    a = 1.85
    Td = 6
    NewConfirmed_hat = np.zeros(days)
    for t in range(len(NewCases)):
        num = int(NewCases[t])
        if num > 0:
            TD=np.random.gamma(shape=a,scale=Td/a,size=num)
            for td in TD:
                tx = int(t*dt+td)
                if tx < days:
                    NewConfirmed_hat[tx] = NewConfirmed_hat[tx]+1
    return NewConfirmed_hat

def Result(SIARa,SIARb,SIARc,beta,gamma,alpha,r,dt,days,step,Mij3):#根据给定参数模拟传染病传播过程,得到SIAR随时间变化情况与每日新增确诊病例数
    resulta = [(sum(SIARa['S']), sum(SIARa['I']), sum(SIARa['A']), sum(SIARa['R']))]
    resultb = [(sum(SIARb['S']), sum(SIARb['I']), sum(SIARb['A']), sum(SIARb['R']))]
    resultc = [(sum(SIARc['S']), sum(SIARc['I']), sum(SIARc['A']), sum(SIARc['R']))]
    #result = []
    NI = [2]
    Mij3 = Mij3_1
    for k in range(step-1):
        #x = int(k*dt)
        #if (x>=13)&(x<=23):
            #Mij3 = Mij3_1-(Mij3_1-Mij3_0)/10*(x-13)
        #if x>=23:
            #Mij3 = Mij3_0
        """ SIARanew: 170000*5, SIARbnew: 170000*5, SIARcnew: 170000*5 """
        """ NIa: 170000*1, NIb: 170000*1, NIc: 170000*1 """
        SIARanew,NIa = SIAR(SIARa,0,0.34,SIARa,SIARb,SIARc,Mij3,beta,gamma,alpha,r,dt)
        SIARbnew,NIb = SIAR(SIARb,1,1,SIARa,SIARb,SIARc,Mij3,beta,gamma,alpha,r,dt)
        SIARcnew,NIc = SIAR(SIARc,2,1.47,SIARa,SIARb,SIARc,Mij3,beta,gamma,alpha,r,dt)
        NI.append(sum(NIa)+sum(NIb)+sum(NIc))
        """ D: 170000*170000 """
        #D = mobility['2019-07-02 00:00:00']#迁移矩阵,待改
        indexk=keys[(k-71)%168]
        D=mobility_without_zero[indexk]
        #例迁移矩阵(M*M)
        #d11,d21,d31
        #d12,d22,d32
        #d13,d23,d33
        """ SIARa: 170000*5, SIARb: 170000*5, SIARc: 170000*5 """
        SIARa = Move(SIARanew,D)
        SIARb = Move(SIARbnew,D)
        SIARc = Move(SIARcnew,D)
        
        resulta.append((sum(SIARa['S']), sum(SIARa['I']), sum(SIARa['A']), sum(SIARa['R'])))
        resultb.append((sum(SIARb['S']), sum(SIARb['I']), sum(SIARb['A']), sum(SIARb['R'])))
        resultc.append((sum(SIARc['S']), sum(SIARc['I']), sum(SIARc['A']), sum(SIARc['R'])))
        #result.append(((sum(SIARa['S'])+sum(SIARb['S'])+sum(SIARc['S'])), (sum(SIARa['I'])+sum(SIARb['I'])+sum(SIARc['I'])), (sum(SIARa['A'])+sum(SIARb['A'])+sum(SIARc['A'])), (sum(SIARa['R'])+sum(SIARb['R'])+sum(SIARc['R']))))
        #del SIARanew,SIARbnew,SIARcnew,D
        #gc.collect()
    resulta = np.array(resulta)
    resultb = np.array(resultb)
    resultc = np.array(resultc)
    #result = np.array(result)
    NewConfirmed_hat = Confirm_case(NI,step,dt,days)
        
    return resulta,resultb,resultc,NewConfirmed_hat


#%%Initailize
""" area: 20m*2 """
area = pd.read_csv('area_SH_using_mobility1.csv')#初始时刻个体在每个子区域的分布
""" age: 20m*1 """
age = pd.read_csv('ageSH.csv')
N = len(age)
""" susc: 20m*1 """
susc = Year_structure(age)
numI = 2 #初始感染者数量
M = 5080 #子区域数量
dt = 1/24
days = 20
step = int(days/dt)
NC = pd.read_csv('NewCases.csv')
NewConfirmed = NC['NewCase'][0:days]
Age_struc = pd.read_csv('Age_struc2016.csv')
Mij14_1 = pd.read_csv('Mij1.csv')#接触矩阵
Mij3_1 = RMij(Mij14_1,Age_struc)
Mij14_0 = pd.read_csv('Mij0.csv')
Mij3_0 = RMij(Mij14_0,Age_struc)

para1 = pd.read_csv('Para1.csv')
para= para1[para1['Rate']==0.1]
Rate=para1['Rate']
Rate=list(set(Rate))
Rate.sort()
#Beta = para['Beta'].dropna() #感染系数
#Gamma = para['Gamma'].dropna()  #移除率
#Alpha=para['Alpha'].dropna()  #无症状感染者相对于有症状感染者的感染性
#Rate=para['Rate'].dropna()  #有症状的感染者占所有感染者的比例
#Rate=Rate[:11]
#Beta = [0.02] #感染系数
#Gamma = [1/5.1]  #移除率
#Alpha=[0.55]  #无症状感染者相对于有症状感染者的感染性
#Rate=[0.21]  #有症状的感染者占所有感染者的比例
f=open('0525mobility_frac_matrix_max_nozero.pkl','rb')
mobility_without_zero=pk.load(f)
keys = list(mobility_without_zero.keys())
def Initial_Status(N,numI,r,area,age,susc):#生成0时刻,每个子区域内, a(b,c)年龄段个体的S,I,A,R的数量和总数
    """ Status: 20m * 1 """
    """ Info: 20m * 5 """ 
    Status = patient_zero(N,numI,r)
    Info = pd.DataFrame({'Agent':area['agent'],'Status':Status,'Age':age['age'],'Susc':susc,'Area0':area['period0']}) # Info -> 20m * 5
    """ SIARa: 170000*5, SIARb: 170000*5,SIARc: 170000*5 """
    SIARa = Info2SIAR(Info,0.34)  
    SIARb = Info2SIAR(Info,1)
    SIARc = Info2SIAR(Info,1.47)
    return SIARa,SIARb,SIARc

#%%主程序

def main(para,Rate,dt,days,step,Mij3_1,N,numI,area,age,susc,pool):
    #Para = pd.DataFrame({'Beta':[],'Gamma':[],'Alpha':[],'Rate':[],'RMSE':[]})
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    Xunhuan = 30
    alpha=0.55
    for m in range(6):#(len(Rate)):
        r=Rate[m]
        """ SIARa: 170000*5, SIARb: 170000*5,SIARc: 170000*5 """
        SIARa0,SIARb0,SIARc0 = Initial_Status(N,numI,r,area,age,susc)#生成初始分布
        for i in range(len(para)):
            g=m*310+i
            beta=para['Beta'].iloc[i]
            gamma=para['Gamma'].iloc[i]
            pool.apply_async(cal_para, args=(r, SIARa0, SIARb0, SIARc0, g, alpha, beta, gamma, Xunhuan, i))
    return 1
    #Para.to_csv('Para_hat.tsv',encoding='utf_8_sig')
    #return Para,Ave_result,Ave_NewConfirmed_hat
#Para,Ave_result,Ave_NewConfirmed_hat = main(Beta,Gamma,Alpha,Rate,dt,days,step,Mij3_0,Mij3_1,N,numI,area,age,susc)

def cal_para(r, SIARa0, SIARb0, SIARc0, g, alpha, beta, gamma, Xunhuan, i):
    print('Para %s:...' % str(i))
    Ave_resulta = np.zeros([step,4])
    Ave_resultb = np.zeros([step,4])
    Ave_resultc = np.zeros([step,4])
    #Ave_result = np.zeros([step,4])
    Ave_NewConfirmed_hat = np.zeros(days)
    #根据给定参数模拟传染病传播过程
    for xunhuan in range(Xunhuan):
        resulta,resultb,resultc,NewConfirmed_hat = Result(SIARa0,SIARb0,SIARc0,beta,gamma,alpha,r,dt,days,step,Mij3_1)
        Ave_resulta = Ave_resulta + resulta
        Ave_resultb = Ave_resultb + resultb
        Ave_resultc = Ave_resultc + resultc
        #Ave_result = Ave_result + result
        Ave_NewConfirmed_hat = Ave_NewConfirmed_hat + NewConfirmed_hat
    Ave_resulta = Ave_resulta/Xunhuan
    Ave_resultb = Ave_resultb/Xunhuan
    Ave_resultc = Ave_resultc/Xunhuan
    Ave_result = Ave_resulta + Ave_resultb + Ave_resultc
    #Ave_result = Ave_result/Xunhuan
    Ave_NewConfirmed_hat = Ave_NewConfirmed_hat/Xunhuan
    delta=NewConfirmed-Ave_NewConfirmed_hat
    rmse = np.mean(delta*delta)**0.5#计算模拟的每日新增确诊病例数与实际数据之间的均方根误差
    #Para = Para.append([{'Beta':beta,'Gamma':gamma,'Alpha':alpha,'Rate':r,'RMSE':rmse}])
    #保存结果: S,I,A,R(包括总体与分年龄段)随时间变化情况与每日新增确诊病例数
    Para = pd.DataFrame({'Beta':[beta],'Gamma':[gamma],'Alpha':[alpha],'Rate':[r],'RMSE':[rmse]})
    Para.to_csv(str(g)+'Para_hat.tsv',encoding='utf_8_sig')
    Ave_resulta=pd.DataFrame(Ave_resulta)
    Ave_resulta.to_csv(str(g)+'SIARa.tsv',encoding='utf_8_sig')
    Ave_resultb=pd.DataFrame(Ave_resultb)
    Ave_resultb.to_csv(str(g)+'SIARb.tsv',encoding='utf_8_sig')
    Ave_resultc=pd.DataFrame(Ave_resultc)
    Ave_resultc.to_csv(str(g)+'SIARc.tsv',encoding='utf_8_sig')
    Ave_result=pd.DataFrame(Ave_result)
    Ave_result.to_csv(str(g)+'SIAR.tsv',encoding='utf_8_sig')
    Ave_NewConfirmed_hat=pd.DataFrame(Ave_NewConfirmed_hat)
    Ave_NewConfirmed_hat.to_csv(str(g)+'NewConfirmed_hat.tsv',encoding='utf_8_sig')
    print('Para %s done. '%str(i), time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


if __name__ == '__main__':
    pool = mp.Pool(mp.cpu_count())
    main(para,Rate,dt,days,step,Mij3_1,N,numI,area,age,susc,pool)
    pool.close()
    pool.join()
    print('Job done')