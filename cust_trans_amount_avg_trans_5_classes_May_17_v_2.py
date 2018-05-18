#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  9 13:48:50 2018

@author: kamran
"""

# importing Library
import pandas as pd
import numpy as np
import os
import datetime
#from datetime import date
from functools import reduce
# Change the directory
os.chdir('/home/kamran/Link to CVM/AutoML/Test_Part')
# Reading the file
df_trans_data = pd.read_csv('trans_type_amount_date_time_test.csv')
df_trigger_data = pd.read_csv('trigger_data.csv')

### merging Suite
# This suit takes Transaction data and Trigger or campaign data and duration
# Return the final data frame 
# which is going to input of the Aggregate suite i.e "count_trans_type_avg_amount_duration"

def merge_trans_camp(df_trans_data, df_trigger_data, duration_merge):
    """
    This suite inner merge the "df_trans_data" and "df_trigger_data" on "cust_id"
    then to filter on the basis of duration
    """
    df_trans_data = pd.merge(left=df_trans_data,right=df_trigger_data, left_on='cust_id', right_on='Cust_ID')
    # Change the data type of all the date
    df_trans_data['trans_time'] = pd.to_datetime(df_trans_data['trans_time'], errors='coerce')
    df_trans_data['Campaign_date'] = pd.to_datetime(df_trans_data['Campaign_date'], errors='coerce')
    get_duration = duration_merge
    duration_upper = datetime.timedelta(days = get_duration)
    duration_lower = datetime.timedelta(days = 0)
    df_trans_data = df_trans_data[(df_trans_data['Campaign_date']- df_trans_data['trans_time'] > duration_lower) & (df_trans_data['Campaign_date']- df_trans_data['trans_time'] <= duration_upper)]
    return df_trans_data
#End of Merging Suite    

need_trans_classes = ['debit','credit','debit_card_withdrawal','credit_card_withdrawal']
#df.trans_code.value_counts()
# Getting the todays date


#Duration limit of the Transaction i.e before 1 year
#Duration will be pass as the duration to the the count_trans_type_avg_amount suit 
#duration_yearly = datetime.timedelta(days=365)

#Creating the suit for Transaction type and its average amount by 
#customer Id
#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
#Function Return duration yearly
def count_trans_type_avg_amount_duration(df, need_trans_classes, post_fix):
    """This suit get the data farme as its arguement and 
    return the df after doing these type of calculation
    
    0. Filter the rows of the transaction between the duration yearly period.
    0.1 Get only that rows which contains classes what we need 
    1. Count the Transaction type according to the customer 
    2. Then claculate the average transasction amount 
    3. """
    #Get today date
    #now = pd.to_datetime(str(date.today()), format='%Y-%m-%d')
    #Convert the date time column into datetime stamp
    df.loc[:,'trans_time'] = pd.to_datetime(df['trans_time'], errors='coerce')
    #Filter the rows of the transaction between the duration period.
    #duration = datetime.timedelta(days=duration_time)
    #Test
    #duration = datetime.timedelta(days=30)
    #df = df.loc[now - df['trans_time'] <= duration]
    # Get only that rows which contains classes what we need
    #Get all the classes from Transaction Features and save as list
    trans_all_classes = list(df.trans_code.value_counts().index)
    remove_trans_classes = list(set(trans_all_classes) - set(need_trans_classes))
    for i in range(0, len(remove_trans_classes)):
        df = df.drop(df[(df.trans_code == remove_trans_classes[i])].index)
    # Find the count of Credit and withdrawl for each customer - Table name =type_count_raw
    #post_fix = 'monthly'
    #df.loc['trans_code'] = df.loc['trans_code']+post_fix
    # Add pre_fix to all elements of the trans_code 
    # Test part
    df['total_trans_'+post_fix] = np.zeros 
    df_trans_count = df.groupby('cust_id').count().reset_index()[['cust_id', 'total_trans_'+post_fix]]
    #Test Part end
    # For time beign@@@
    df['trans_code'] = post_fix+ str('_') + df['trans_code'].astype(str)
    #df.loc['trans_code'] = df['trans_code'].astype(str) +post_fix
    type_count_raw = df.groupby(['cust_id', 'trans_code'], squeeze = True).count().reset_index()[['cust_id','trans_code','trans_time']]
    #Okay
    type_count = type_count_raw.pivot(index='cust_id', columns='trans_code', values='trans_time').reset_index()
    #Caculate the total number of Transaction
    #test part
    #post_fix = 'monthly'
    #type_count[remove_trans_classes[0]]
    #type_count['total_trans_'+post_fix] = np.nan
    #type_count['total_trans_'+post_fix] = type_count['']
    
    #
    oper_avg = (pd.pivot_table(df, index = 'cust_id', columns= 'trans_code', values = 'trans_amount',aggfunc = np.average)).reset_index()
    type_oper_avg = pd.merge(type_count, oper_avg,suffixes=('_trans_count', '_avg'), how='right', on='cust_id')
    type_oper_avg = pd.merge(type_oper_avg, df_trans_count, how='right', on='cust_id')
    return type_oper_avg

# Calling the Suite 
#"""
# Calling the Suite    
def trans_fun_call():
    df_last_two_years = count_trans_type_avg_amount_duration(merge_trans_camp(df_trans_data, df_trigger_data, 730), need_trans_classes, 'last_two_years')
    df_last_year = count_trans_type_avg_amount_duration(merge_trans_camp(df_trans_data, df_trigger_data, 365), need_trans_classes, 'last_year')
    df_last_half = count_trans_type_avg_amount_duration(merge_trans_camp(df_trans_data, df_trigger_data, 180), need_trans_classes, 'last_six_months')
    df_last_quater = count_trans_type_avg_amount_duration(merge_trans_camp(df_trans_data, df_trigger_data, 90), need_trans_classes, 'last_quater')
    df_last_month = count_trans_type_avg_amount_duration(merge_trans_camp(df_trans_data, df_trigger_data, 30), need_trans_classes, 'last_month')
    dfs = [df_last_two_years, df_last_year, df_last_half, df_last_quater, df_last_month]
    df_final = reduce(lambda left,right: pd.merge(left,right,on='cust_id'), dfs)
    return df_final    


a = trans_fun_call()
print(a)

"""
print(df_last_month)
print('@@@@@')
print(df_last_quater)
print('@@@@@')
print(df_last_half)
print('@@@@@')
print(df_last_year)
print('@@@@@')
print(df_last_two_years)
"""