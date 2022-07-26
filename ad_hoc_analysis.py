import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import calendar
from datetime import date

#Open JSON File
tamis="transaction-data-adhoc-analysis.json"
tamis_df=pd.read_json(tamis)
tamis_df


#Get unique products and their respective prices
products_df = tamis_df.copy()

unique_product=products_df.transaction_items.str.split(";")
products_df = products_df.assign(transaction_items=unique_product)
products_df = products_df[products_df["transaction_items"].apply(len)==1]

products_df["transaction_items"] = products_df["transaction_items"].apply(lambda lst: lst[0])
products_df["Amount"] = products_df["transaction_items"].apply(lambda x: int(x.split(',')[-1].strip('(x)')))
products_df["transaction_items"] = products_df["transaction_items"].apply(lambda lst: lst[:-5])

products_df = products_df.drop_duplicates(subset="transaction_items", keep="first")
label = list(products_df["transaction_items"].values)
products_df = products_df["transaction_value"] / products_df["Amount"]

products_df.index = label

products_df

#Explode unique orders by each customer into different rows
unique_product=tamis_df.transaction_items.str.split(";")
data_df = tamis_df.assign(transaction_items=unique_product).explode('transaction_items')

data_df["Quantity per Item"] = data_df["transaction_items"].apply(lambda x: int(x.split(',')[-1].strip('(x)')))
data_df["transaction_items"] = data_df["transaction_items"].apply(lambda lst: lst[:-5])
data_df["Total Price per Product"] = data_df["Quantity per Item"] * data_df["transaction_items"].apply(lambda x: products_df.loc[x])

#Get month of when order was made
def month_date(x):    
    month = int(x[6:len(x)-3])
    a = calendar.month_name[month]
    return a

data_df["Month of Transaction"] = data_df["transaction_date"].apply(month_date).astype(pd.api.types.CategoricalDtype(categories=['January','February','March','April','May','June']))

#Arrange dataframe
data_df["Transaction Items"]=data_df["transaction_items"]
del data_df["transaction_items"]

data_df.rename(columns={'name':'Customer Name','transaction_value':'Total Transaction Value','transaction_date':'Transaction Date','username':'Username'},inplace=True)
data_df=data_df[['Customer Name','Username','Transaction Items','Quantity per Item','Total Price per Product','Total Transaction Value','Transaction Date','Month of Transaction']]

data_df


#Breakdown of the count of each item sold per month
total_count_per_item=pd.pivot_table(data_df,index="Transaction Items",columns="Month of Transaction",values="Quantity per Item",aggfunc=sum)

total_count_per_item


#Breakdown of the total sale value per item per month
total_sales_per_item=pd.pivot_table(data_df,index="Transaction Items",columns="Month of Transaction",values="Total Price per Product",aggfunc=sum)

total_sales_per_item

#Creating bar graph for Total Sales per item over the course of 6 months
total_sales_per_item.plot.barh()

#Breakdown of total number of items sold and total sales per month
sales_df=data_df.copy()
sales_df['Total Sales']=sales_df['Total Price per Product']

tamis_sales=pd.pivot_table(sales_df,index="Month of Transaction",values=["Quantity per Item","Total Sales"],aggfunc=sum, margins=True, margins_name='Grand Total')
tamis_sales

#Breakdown of number of orders made by each customer
customers_df = data_df[['Customer Name','Quantity per Item','Month of Transaction']]
customers_df = pd.pivot_table(customers_df, values="Quantity per Item", index="Customer Name", columns="Month of Transaction", aggfunc=sum,fill_value = 0)

customers_df

#Finding number of repeater customers
repeaters=customers_df.copy()

repeaters['January'] = 0

repeaters['February'] = np.where((customers_df['January'] >=1) & (customers_df['February'] >=1),1,0)

repeaters['March'] = np.where((customers_df['February'] >=1) & (customers_df['March'] >=1),1,0)

repeaters['April'] = np.where((customers_df['March'] >=1) & (customers_df['April'] >=1),1,0)

repeaters['May'] = np.where((customers_df['April'] >=1) & (customers_df['May'] >=1),1,0)

repeaters['June'] = np.where((customers_df['May'] >=1) & (customers_df['June'] >=1),1,0)

repeaters=pd.DataFrame(repeaters.sum(), columns=["Repeater"], copy=False)

repeaters.transpose()

#Finding number of inactive customers
inactives=customers_df.copy()

inactives['January'] = 0

inactives['February'] = np.where((customers_df['January'] >=1) & (customers_df['February'] ==0),1,0)

inactives['March'] = np.where(((customers_df['January'] >=1) | (customers_df['February'] >=1)) & (customers_df['March'] ==0),1,0)

inactives['April'] = np.where(((customers_df['January'] >=1) | (customers_df['February'] >=1) | (customers_df['March'] >=1)) & (customers_df['April'] ==0),1,0)

inactives['May'] = np.where(((customers_df['January'] >=1) | (customers_df['February'] >=1) | (customers_df['March'] >=1) | (customers_df['April'] >=1)) & (customers_df['May'] ==0),1,0)
                            
inactives['June'] = np.where(((customers_df['January'] >=1) | (customers_df['February'] >=1) | (customers_df['March'] >=1) | (customers_df['April'] >=1) | (customers_df['May'] >=1)) & (customers_df['June'] ==0),1,0)

inactives= pd.DataFrame(inactives.sum(), columns = ["Inactive"], copy=False)

inactives.transpose()

#Finding number of engaged customers
engaged=customers_df.copy()

engaged['January'] = np.where((customers_df['January'] >0),1,0)

engaged['February'] = np.where((customers_df['January'] >0) & (customers_df['February'] >0),1,0)

engaged['March'] = np.where((customers_df['January'] >0) & (customers_df['February'] >0) & (customers_df['March'] >0),1,0)

engaged['April'] = np.where ((customers_df['January'] >0) & (customers_df['February'] >0) & (customers_df['March'] >0) & (customers_df['April'] >0),1,0)

engaged['May'] = np.where ((customers_df['January'] >0) & (customers_df['February'] >0) & (customers_df['March'] >0) & (customers_df['April'] >0) & (customers_df['May'] >0),1,0)

engaged['June'] = np.where ((customers_df['January'] >0) & (customers_df['February'] >0) & (customers_df['March'] >0) & (customers_df['April'] >0) & (customers_df['May'] >0) & (customers_df['June'] >0),1,0)
    
engaged= pd.DataFrame(engaged.sum(), columns = ["Engaged"], copy=False)

engaged.transpose()


from functools import reduce

df=[repeaters,inactives,engaged]

final_df = reduce(lambda left,right: pd.merge(left,right,on=['Month of Transaction'], how='outer'), df)

final_df.transpose()


#Finding age and age group of Lola Tamis' customers
def age(x):
    year=int(x[:len(x)-6])
    a=date.today().year-year
    return int(a)

def age_group(x):
    if 0< x <=14:
        return "Child"
    elif 15<= x <=24:
        return "Youth"
    elif 25<= x <=64:
        return "Adult"
    elif x>=65:
        return "Senior"
    
#Creating new dataframe that includes Age Group
age_df=tamis_df.copy()

age_df['Age']=age_df['birthdate'].apply(age)
age_df['Age Group']=age_df['Age'].apply(age_group)
age_df["Month of Transaction"]=age_df["transaction_date"].apply(month_date).astype(pd.api.types.CategoricalDtype(categories=['January','February','March','April','May','June']))
age_df=pd.DataFrame().assign(Age=age_df['Age'],AgeGroup=age_df['Age Group'],Month=age_df['Month of Transaction'])
 
#Creating bar graph for Age Group
age_category=age_df['AgeGroup'].value_counts()
age_category.plot.bar()
plt.title("Customer's Age Categories for Lola Tamis")
plt.xlabel('Age Category')
plt.ylabel('Number of Transactions')

#Retrieving number of transactions made by each Age Group
age_category=age_df['AgeGroup'].value_counts()
age_category