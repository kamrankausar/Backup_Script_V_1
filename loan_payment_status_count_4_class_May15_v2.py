#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 15 14:06:59 2018

@author: kamran
"""
#import the module
import pandas as pd
import numpy as np
import datetime
from datetime import date
import os
from functools import reduce
# read the dataset
os.chdir('/home/kamran/Link to CVM/AutoML/Test_Part')
df = pd.read_csv('loan_pay_final.csv')

#You can safely disable this new warning with the following assignment.
pd.options.mode.chained_assignment = None  # default='warn'


#Set the duration
grace_days = 25
grace_period = datetime.timedelta(days = grace_days)

# Create the Suite which count the status of the Credit Card Payment
def loan_pay_status_count(df_loan, get_duration, grace_days, post_fix):
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
    now = pd.to_datetime(str(date.today()), format='%Y-%m-%d')
    #Filter the rows of the transaction between the duration period.
    duration = datetime.timedelta(days=get_duration)
    df_loan['Loan_Due_Date'] = pd.to_datetime(df_loan['Loan_Due_Date'], errors='coerce')
    df_loan = df_loan.loc[now - df['Loan_Due_Date'] <= duration]
    #0.0
    #df_credit['CC_Due_Date'] = df_credit['CC_Due_Date'].apply(lambda x: pd.to_datetime(x))
    df_loan.loc[:,'Loan_Payment_Date'] = df_loan['Loan_Payment_Date'].apply(lambda x: pd.to_datetime(x))
    #1
    df_loan.loc[:,'loan_paid_status_'+post_fix] = np.nan 
    df_loan.loc[:,'temp_col'] = np.zeros # This col is created because it will use in the pivot table for value count
    #1.1
    def count_status(date1, date2):
        duration_days = datetime.timedelta(days = 25)
        if date1 - date2 > duration_days:
            return 'delay_'+post_fix 
        elif date1 - date2 <= duration_days:
            return 'on_time_'+post_fix
        else:
            return 'unpaid_'+post_fix
    #2
    df_loan.loc[:,'loan_paid_status_'+post_fix] = df_loan.apply(lambda row: count_status(row['Loan_Payment_Date'], row['Loan_Due_Date']), axis = 1)
    #3
    df_loan = df_loan.groupby(['cust_id', 'loan_paid_status_'+post_fix], squeeze = True).count().reset_index()[['cust_id', 'loan_paid_status_'+post_fix, 'temp_col']]
    df_loan = df_loan.pivot(index = 'cust_id', columns = 'loan_paid_status_'+post_fix, values = 'temp_col').reset_index()
    #Return the dataFrame
    df_loan.fillna(0, inplace = True)
    #Create a column for Ration of Paid status
    df_loan.loc[:,'delay_percent_'+post_fix] = np.nan
    df_loan.loc[:,'on_time_percent_'+post_fix] = np.nan
    df_loan.loc[:,'unpaid_percent_'+post_fix] = np.nan
    df_loan.loc[:,'delay_percent_'+post_fix] = ( df_loan['delay_'+post_fix]/(df_loan['delay_'+post_fix] + df_loan['on_time_'+post_fix] + df_loan['unpaid_'+post_fix]) * 100)
    df_loan.loc[:,'on_time_percent_'+post_fix] = ( df_loan['on_time_'+post_fix]/(df_loan['delay_'+post_fix] + df_loan['on_time_'+post_fix] + df_loan['unpaid_'+post_fix]) * 100)
    df_loan.loc[:,'unpaid_percent_'+post_fix] = ( df_loan['unpaid_'+post_fix]/(df_loan['delay_'+post_fix] + df_loan['on_time_'+post_fix] + df_loan['unpaid_'+post_fix]) * 100)
    return df_loan


#Call the function
    
#df_return = credit_card_status_count(df, duration_days)
# Calling the Suite    
def loan_fun_call():
    df_test_yearly = loan_pay_status_count(df, 365, grace_period, 'yearly')
    df_test_half = loan_pay_status_count(df, 180, grace_period, 'half_yearly')
    df_test_quaterly = loan_pay_status_count(df, 90, grace_period, 'quaterly')
    df_test_monthly = loan_pay_status_count(df, 30, grace_period, 'monthly')
    dfs = [df_test_yearly, df_test_half, df_test_quaterly, df_test_monthly]
    df_final = reduce(lambda left,right: pd.merge(left,right,on='cust_id'), dfs)
    return df_final

#How to use this function
# import the script and call "loan_fun_call()" this script to Master Script
#call_fun_df = loan_fun_call()
#print(call_fun_df)
#call_fun_df.to_csv('loan_final', index = False)

a = loan_fun_call()
print(a)

"""
print('\n\t')
print('\tActuall Data')
print(df)

print('\n\tYearly')
print(df_test_yearly)
print('\n\tHalf - Yearly')
print(df_test_half)
print('\n\tquaterly')
print(df_test_quaterly)

print('\n\tMonthly')
print(df_test_monthly)
"""



