#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 15 17:50:27 2018

@author: kamran
"""
#Note that Script name only contain the underscore(_) not dash(-)

import os 
#import pandas as pd
os.chdir('/media/DATA/CVM/AutoML/Final_Code_Data_Pre_Auto_ML')


# Calling the Loan Module 
from loan_payment_status_count_4_class_May15_v2 import loan_fun_call
loan_final = loan_fun_call()
loan_final.to_csv('loan_final', index = False)
loan_final.head()

# Calling Transaction Module 
# What are done in Transaction Module the Variable Master List - S.No 1 to 15 and 25 to 34
from cust_trans_amount_avg_trans_5_classes_May_16_v_1 import trans_fun_call
trans_final = trans_fun_call()
trans_final.head()
