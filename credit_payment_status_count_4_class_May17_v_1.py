#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 10 18:31:59 2018

@author: kamran
"""
#import the module
import pandas as pd
import numpy as np
import datetime
#from datetime import date
import os
# read the dataset
os.chdir('/home/kamran/Link to CVM/AutoML/Test_Part')
df = pd.read_csv('credit_card_final.csv')

#You can safely disable this new warning with the following assignment.
pd.options.mode.chained_assignment = None  # default='warn'


#Set the duration of the grace period
grace_days = 25
grace_period = datetime.timedelta(days = grace_days)

# Reading the file
df_credit_data = pd.read_csv('credit_card_final_empty.csv')
df_trigger_data = pd.read_csv('trigger_data.csv')
### merging Suite
# This suit takes Transaction data and Trigger or campaign data and duration
# Return the final data frame 
# which is going to input of the Aggregate suite i.e "count_trans_type_avg_amount_duration"

def merge_credit_camp(df_credit_data, df_trigger_data, duration_merge):
    """
    This suite inner merge the "df_credit_data" and "df_trigger_data" on "cust_id"
    then to filter on the basis of duration
    """
    df_credit_data = pd.merge(left=df_credit_data,right=df_trigger_data, left_on='cust_id', right_on='Cust_ID')
    # Change the data type of all the date
    df_credit_data['CC_Due_Date'] = pd.to_datetime(df_credit_data['CC_Due_Date'], errors='coerce')
    df_credit_data['Campaign_date'] = pd.to_datetime(df_credit_data['Campaign_date'], errors='coerce')
    get_duration = duration_merge
    duration_upper = datetime.timedelta(days = get_duration)
    duration_lower = datetime.timedelta(days = 0)
    df_credit_data = df_credit_data[(df_credit_data['Campaign_date']- df_credit_data['CC_Due_Date'] > duration_lower) & (df_credit_data['Campaign_date']- df_credit_data['CC_Due_Date'] <= duration_upper)]
    return df_credit_data
#End of Merging Suite  
# testing part
"""
a = merge_credit_camp(df_credit_data, df_trigger_data, 365)
def test(a):
    if len(a):
        return 0
    else: 
        return 1
b = test(a)
"""
# Testing Part End

# Create the Suite which count the status of the Credit Card Payment
def credit_card_status_count(df_credit, grace_period, post_fix):
    """ This suite take the DF and return DF with Payment status counts depending
    upon Payment date and Due date. 
    Levels of the credit_status_count = ['unpaid', 'delay', 'on_time'] on the 
    basis of duration (which is one of the arguments)
    Note duration must be in datetime.timedelta
    0. Filter the rows of the transaction between the duration yearly period.
    0.0 Convert the data type of the Date columns
    1.Create a column credit_paid_status which contains ['unpaid', 'delay', 'on_time'] 
    1.1 Create a function which takes two arguments CC_Due_Date and CC_Payment_Date and compare 
    this function does these
    CC_Payment_Date - CC_Due_Date if less than duration return on_time
    greater than duration return delay and unpaid if CC_Payment_Date is NaN
    2.Apply the logical operator to the date columns through lambda function  
    3. Apply groupby on cust_id and credit_status_count 
    4. Apply pivot function
    """
    #0
    #Get today date
    #now = pd.to_datetime(str(date.today()), format='%Y-%m-%d')
    #Filter the rows of the transaction between the duration period.
    #duration = datetime.timedelta(days=get_duration)
    #df_credit['CC_Due_Date'] = pd.to_datetime(df_credit['CC_Due_Date'], errors='coerce')
    #df_credit = df_credit.loc[now - df['CC_Due_Date'] <= duration]
    #0.0
    #df_credit['CC_Due_Date'] = df_credit['CC_Due_Date'].apply(lambda x: pd.to_datetime(x))
    df_credit.loc[:,'CC_Payment_Date'] = df_credit.CC_Payment_Date.apply(lambda x: pd.to_datetime(x))
    #1
    df_credit.loc[:,'credit_paid_status_'+post_fix] = np.nan 
    df_credit.loc[:,'temp_col'] = np.zeros # This col is created because it will use in the pivot table for value count
    #1.1
    #g = grace_days
    def count_status(date1, date2, grace_period):
        #duration_days = datetime.timedelta(days = 25)
        duration_lower = datetime.timedelta(days = 0)
        duration_days = grace_period
        if date1 - date2 > duration_days:
            return 'credit_card_delay_'+post_fix + '_count'
        elif duration_lower < date1 - date2 <= duration_days:
            return 'credit_card_on_time_'+post_fix + '_count'
        else:
            return 'credit_card_unpaid_'+post_fix + '_count'
    #2
    df_credit.loc[:,'credit_paid_status_'+post_fix] = df_credit.apply(lambda row: count_status(row['CC_Payment_Date'], row['CC_Due_Date'], grace_period), axis = 1)
    #3
    df_credit = df_credit.groupby(['cust_id', 'credit_paid_status_'+post_fix], squeeze = True).count().reset_index()[['cust_id', 'credit_paid_status_'+post_fix, 'temp_col']]
    df_credit = df_credit.pivot(index = 'cust_id', columns = 'credit_paid_status_'+post_fix, values = 'temp_col').reset_index()
    #Return the dataFrame
    df_credit.fillna(0, inplace = True)
    return df_credit


#Call the function
    
#df_return = credit_card_status_count(df, duration_days)
# Calling the Suite    
df_test_yearly = credit_card_status_count(merge_credit_camp(df_credit_data, df_trigger_data, 365), grace_period, 'yearly')
df_test_half = credit_card_status_count(merge_credit_camp(df_credit_data, df_trigger_data, 180), grace_period, 'half_early')
df_test_quaterly = credit_card_status_count(merge_credit_camp(df_credit_data, df_trigger_data, 90), grace_period, 'quaterly')
df_test_monthly = credit_card_status_count(merge_credit_camp(df_credit_data, df_trigger_data, 30), grace_period, 'monthly')
print('\n\t')
print('\tActuall Data')
print(df)

print('\n\tYearly')
print(df_test_yearly)
print('\n\tHalf - Yearly')
print(df_test_half)
print(df_test_quaterly)

print('\n\tMonthly')
print(df_test_monthly)




