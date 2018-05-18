#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 14 15:48:04 2018

@author: kamran
"""
#import the moduel
import pandas as pd
import numpy as np
import os
import datetime
from datetime import date
from functools import reduce # For merging more than one DataFrame in Py3

#Change the directory
os.chdir('/home/kamran/Link to CVM/AutoML/Test_Part')
# Getting the data 
df_camp = pd.read_csv('camp_response_data.csv')

# Creating the Suit name camp_cha_day_time_count(df)

def camp_cha_day_time_count(df_camp, get_duration, post_fix):
    """
    This suite takes the DF as input and does following things
    1. Aggregate the days at Customer Level and save in DF
    2. Aggregate the channel at Custome Level and save in DF
    3. Aggregate the time_lable at Custome Level and Save in DF
    4. Merge all the above DF on the basis of Customer
    """
    #Change the data type of the response time to datetime stamp
    df_camp['camp_response_time'] = pd.to_datetime(df_camp['camp_response_time'], errors='coerce')
    df_camp['camp_sent_time'] = pd.to_datetime(df_camp['camp_sent_time'], errors = 'coerce')
    #Getting only record based on duration
     # Get today date
    now = pd.to_datetime(str(date.today()), format = '%Y-%m-%d')
    duration = datetime.timedelta(days = get_duration)
    df_camp = df_camp[now - df_camp['camp_sent_time'] <= duration]
    
    
    
    # Create the needed columns
    #Create the day and time(hour, min) column
    df_camp.res_day = np.nan
    #df.resp_time = np.nan
    df_camp.res_hr = np.nan
    df_camp.res_min = np.nan
    df_camp['day_counts'] = 0
    df_camp['cha_count'] = 0
    # 1.Aggregate the days at Customer Level and save in df_day_count
    #Extract day from the camp_response_time
    df_camp['res_day'] = df_camp.camp_response_time.dt.weekday_name
    #df['trans_code'] = post_fix+ str('_') + df['trans_code'].astype(str)
    #Adding the Prefix
    df_camp['res_day'] = str('count_') + df_camp['res_day'].astype(str)
    df_camp['camp_channel_type'] = str('count_') + df_camp['camp_channel_type'].astype(str)
    #
    df_day_count = df_camp.groupby(['cust_id', 'res_day'], squeeze = True).count().reset_index()[['cust_id', 'res_day', 'day_counts']]
    df_day_count = df_day_count.pivot(index = 'cust_id', columns = 'res_day', values = 'day_counts').reset_index()
    df_day_count.fillna(0, inplace = True)
    # 2.Aggregate the channel at Custome Level and save in df_cha_count
    df_cha_count = df_camp.groupby(['cust_id', 'camp_channel_type'], squeeze = True).count().reset_index()[['cust_id', 'camp_channel_type', 'cha_count']]    
    df_cha_count = df_cha_count.pivot(index = 'cust_id', columns = 'camp_channel_type', values = 'cha_count').reset_index()
    df_cha_count.fillna(0, inplace = True)
    
    #3.3. Aggregate the time_lable at Custome Level and Save in df_time_lable_count
    df_camp.res_time_lable = np.nan
    df_camp.loc[:,'lable_count'] = 0
    df_camp.loc[:,'res_hr'] = df_camp.camp_response_time.dt.hour
    df_camp.loc[:,'res_min'] = df_camp.camp_response_time.dt.minute
    #Response Lable
    def res_lable(get_hour, get_min):
        ''' 
        1. 0 <-> 6:30 = Early Morning
        2. 6:31 <-> 9:30 = Work_Coming_Time
        3. 9:31 <-> 12:30 = Work_peak_Time
        4. 12:31 <-> 15:30 = Lunch_Time 
        5. 15:31 <-> 18:30 = Post_lunch_Time
        6. 18:31 <-> 20:30 = Going_Home_Time
        7. 20:30 <-> 23:59 = DND_Family_Time
        '''
        mins = int(get_hour) * 60 + int(get_min)
        if 0 <= mins <= 390:
            return 'early_morning_count'
        elif 391 <= mins <= 570:
            return 'office_comming_time_count'
        elif 571 <= mins <= 750:
            return 'office_peak_time_count'
        elif 751 <= mins <= 930:
            return 'lunch_time_count'
        elif 931 <= mins <= 1110:
            return 'post_lunch_count'
        elif 1111 <= mins <= 1230:
            return 'going_home_time_count'
        elif 1231 <= mins <= 1439:
            return 'office_peak_time_count'
        else:
            return 'nan'
    df_camp.loc[:,'res_time_lable'] = df_camp.apply(lambda row : res_lable(row.res_hr, row.res_min), axis = 1)
    # Doing the groupby
    df_time_lable_count = df_camp.groupby(['cust_id', 'res_time_lable'], squeeze = True).count().reset_index()[['cust_id', 'res_time_lable', 'lable_count']]
    df_time_lable_count = df_time_lable_count.pivot(index = 'cust_id', columns = 'res_time_lable', values = 'lable_count').reset_index()
    df_time_lable_count.fillna(0, inplace = True)
    #Merge all the new DataFrame
    #1.df_day_count 
    #2.df_cha_count
    #3.df_time_lable_count 
    dfs = [df_day_count, df_cha_count, df_time_lable_count]
    df_final = reduce(lambda left,right: pd.merge(left,right,on='cust_id'), dfs)
    return df_final


df_get = camp_cha_day_time_count(df_camp)
print(df_get)
    
    

    
    
    
    
    
    
    