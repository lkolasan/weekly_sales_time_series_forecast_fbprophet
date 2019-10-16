import warnings, os, re
warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd
# data visualization
import matplotlib.pyplot as plt
# fbprophet by Facebook for timeseries forecast
from fbprophet import Prophet
# To get data from ES
import pyodbc

# Provide the new path here
directory = r"{insert path}"
os.chdir(directory)  
#Parameters to set starting and ending fiscal weeks
trainstart = 201627
trainend = 201826
teststart = 201827
testend = 201848
level = 'SKU'
levelname = 'SKUDescription'
conn = pyodbc.connect(r'DRIVER={insert driver}; SERVER={insert server}; DATABASE={insert database};Trusted_Connection=yes;')
levelvalues = pd.read_csv(r"{insert path to SKUs to forecast}")['SKU']
a = pd.DataFrame(columns=[level, levelname,'Vendor Number', 'Vendor Name', 'Percent Error(MAPE)',1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52])

global a
for levelvalue in levelvalues[:500]:
	levelnamevalue = re.sub(r"[^a-zA-Z0-9]+", ' ', pd.read_sql("select {} from [Merchandising_DataMart].[dbo].[vw_DimItem] where {} = {}".format(levelname,level,levelvalue), conn)[levelname][0])
	query = ("""
	SELECT '{}' as {}, '{}' as {},dt.WeekBeginDate as TransDate, isnull(sum([SalesUnitsPerStore]),0) as SalesUnitsPerStore
	FROM [Merchandising_DataMart].[dbo].[vw_DimDate] dt
	left outer join
	(SELECT {},{}, WeekBeginDate, isnull(sum([SalesUnits])/(7*count(distinct sls.Allstorekey)),0) as SalesUnitsPerStore
	FROM [Merchandising_DataMart].[dbo].[vw_DimDate] dt
	left outer join [Merchandising_DataMart].[dbo].[vw_FactSalesSummary] sls on sls.DateKey=dt.DateKey
	inner join [Merchandising_DataMart].[dbo].[vw_DimItem] item on sls.itemkey=item.ItemKey
	WHERE item.{}={} and (dt.YearNumber*100)+dt.WeekNumber>={} and (dt.YearNumber*100)+dt.WeekNumber<={}  
	GROUP BY {},{}, WeekBeginDate) sls on sls.WeekBeginDate = dt.WeekBeginDate
	WHERE (dt.YearNumber*100)+dt.WeekNumber>={} and (dt.YearNumber*100)+dt.WeekNumber<={}
	GROUP BY {},{}, dt.WeekBeginDate
	""")
	trainquery = query.format(levelvalue,level,levelnamevalue,levelname,level,levelname,level, levelvalue, trainstart, trainend, level,levelname, trainstart, trainend, level,levelname)
	testquery = query.format(levelvalue,level,levelnamevalue,levelname,level,levelname,level, levelvalue, teststart, testend, level,levelname, teststart, testend, level,levelname)
	vendornumber = pd.read_sql("select vendornumber from [Merchandising_DataMart].[dbo].[vw_DimItem] item inner join [Merchandising_DataMart].[dbo].[vw_DimVendor] vendor on item.IVENDOR1=vendor.vendornumber where {} = {}".format(level,levelvalue), conn)['vendornumber'][0].astype(int)
	vendorname = pd.read_sql("select vendorname from [Merchandising_DataMart].[dbo].[vw_DimItem] item inner join [Merchandising_DataMart].[dbo].[vw_DimVendor] vendor on item.IVENDOR1=vendor.vendornumber where {} = {}".format(level,levelvalue), conn)['vendorname'][0]
	trainsales = pd.read_sql(trainquery, conn)
	trainsales = trainsales.rename(columns = {'TransDate': 'ds', 'SalesUnitsPerStore': 'y'})
	trainsales = trainsales.replace(0.0,np.NaN)
	if sum(trainsales['y']>0)< 26:
		continue
	def imputetrain(i):
		if ((trainsales['y'][i]-trainsales['y'][i+52])/(0.5*(trainsales['y'][i]+trainsales['y'][i+52]))) > 0.2 :
			trainsales['y'][i+52] = trainsales['y'][i]
		elif ((trainsales['y'][i+52]-trainsales['y'][i])/(0.5*(trainsales['y'][i+52]+trainsales['y'][i]))) > 0.2 :
			trainsales['y'][i] = trainsales['y'][i+52]
		else:
			pass
    
	for i in range(52):
		imputetrain(i)
    
	trainsales.loc[(trainsales['y'] == 0), 'y'] = None
	trainsales['cap'] = trainsales['y'].max()*1.1
	if trainsales['y'].min() <0:
		trainsales['floor'] = 0
	else: 
		trainsales['floor'] = trainsales['y'].min()*0.8
    
	#dataframe of annual US Public Holidays over training and forecasting periods 
	ny = pd.DataFrame({'holiday': "New Year's Day", 'ds' : pd.to_datetime(['2017-01-01', '2018-01-01','2019-01-01'])})  
	mlk = pd.DataFrame({'holiday': 'Birthday of Martin Luther King, Jr.', 'ds' : pd.to_datetime(['2017-01-16', '2018-01-15'])}) 
	wash = pd.DataFrame({'holiday': "Washington's Birthday", 'ds' : pd.to_datetime(['2016-02-15', '2017-02-20', '2018-02-19'])})
	mem = pd.DataFrame({'holiday': 'Memorial Day', 'ds' : pd.to_datetime(['2016-05-30', '2017-05-29', '2018-05-28'])})
	ind = pd.DataFrame({'holiday': 'Independence Day', 'ds' : pd.to_datetime(['2015-07-04','2016-07-04', '2017-07-04', '2018-07-04'])})
	lab = pd.DataFrame({'holiday': 'Labor Day', 'ds' : pd.to_datetime(['2015-09-07','2016-09-05', '2017-09-04','2017-09-03'])})
	col = pd.DataFrame({'holiday': 'Columbus Day', 'ds' : pd.to_datetime(['2015-10-12','2016-10-10', '2017-10-09', '2015-10-08'])})
	vet = pd.DataFrame({'holiday': "Veteran's Day", 'ds' : pd.to_datetime(['2015-11-11','2016-11-11', '2017-11-11','2018-11-12'])})
	thanks = pd.DataFrame({'holiday': 'Thanksgiving Day', 'ds' : pd.to_datetime(['2015-11-26','2016-11-24','2017-11-23','2018-11-22'])})
	christ = pd.DataFrame({'holiday': 'Christmas', 'ds' : pd.to_datetime(['2015-12-25', '2016-12-25', '2017-12-25', '2018-12-25'])})
	us_public_holidays = pd.concat([ny, mlk, wash, mem, ind, lab, col, vet, thanks, christ])
	#Model
	my_model = Prophet(yearly_seasonality=True, holidays=us_public_holidays,growth='logistic')
	# my_model.add_regressor('SalesUnitsPerStore')
	my_model = my_model.fit(trainsales)
	# dataframe that extends into future 22 weeks 
	future = my_model.make_future_dataframe(periods=22,freq='W')
	future['cap'] = trainsales['y'].max()*1.2
	if trainsales['y'].min()*0.8 <0:
		future['floor'] = 0
	else: 
		future['floor'] = trainsales['y'].min()*0.8
    
	#Testdata
	testsales = pd.read_sql(testquery, conn)
	testsales = testsales.rename(columns = {'TransDate': 'ds', 'SalesUnitsPerStore': 'y'})
	# testsalescopy=testsales
	# future['SalesUnitsPerStore'] =  trainsales['SalesUnitsPerStore'].append(testsales['SalesUnitsPerStore']+2833130, ignore_index=True).reset_index()['SalesUnits']
	forecast = my_model.predict(future)
	fc = forecast[['ds', 'yhat']].rename(columns = {'Date': 'ds', 'Forecast': 'yhat'})
	# # visualizing predicions
	# my_model.plot(forecast)
	# plt.show()
	# # visualizing components of forecast: how daily, weekly and yearly patterns of the time series contribute to the overall forecasted values
	# my_model.plot_components(forecast)
	# plt.show()
	# Checking error rate  
	print("Forecast Accuracy(MAPE) is {}% for {} {}".format(round(np.mean(np.abs((fc.tail(22).reset_index(drop=True)['yhat'] - testsales.tail(22).reset_index(drop=True)['y']) / fc.tail(22).reset_index(drop=True)['yhat'])) * 100),level,levelnamevalue))
	temp = pd.DataFrame({level: [levelvalue], levelname: [levelnamevalue], 'Vendor Number': [vendornumber], 'Vendor Name': [vendorname], 'Percent Error(MAPE)': [round(np.mean(np.abs((fc.tail(22).reset_index(drop=True)['yhat'] - testsales.tail(22).reset_index(drop=True)['y']) / fc.tail(22).reset_index(drop=True)['yhat'])) * 100)]})	
	# temp = pd.DataFrame({level: [levelvalue], levelname: [levelnamevalue], 'Percent Error(MAPE)': [round(np.mean(np.abs((fc.tail(22).reset_index(drop=True)['yhat'] - testsales.tail(22).reset_index(drop=True)['y']) / fc.tail(22).reset_index(drop=True)['yhat'])) * 100)]})	
	#Forecast for next FY
	future = my_model.make_future_dataframe(periods=78,freq='W')
	future['cap'] = trainsales['y'].max()*1.1
	future['floor'] = trainsales['y'].min()*0.8
	forecast = my_model.predict(future)
	fc1 = forecast[['ds', 'yhat']].rename(columns = {'ds': 'Date', 'yhat': '0'})
	my_model.plot(forecast, xlabel='Period', ylabel='Sales Units Per Store').savefig(directory+r"\forecast_graphs\{}_{}_{}_SalesUnitsPerStore_forecast.png".format(level,levelvalue,levelnamevalue), dpi=100)
	fc1 = fc1.tail(52)[['0']].T.reset_index(drop=True)
	fc1.columns = [i for i in range(53)][1:53]
	fc1 = pd.concat([temp, fc1], axis=1, join='inner').reset_index(drop=True)
	a = pd.concat([a, fc1],axis=0)
	print(a)
	a = a.reset_index(drop=True)
	a.to_csv(os.path.join(directory, 'SalesUnitsPerStore_forecast_result_by_{}.csv'.format(level)), sep=',', index=None, encoding = 'utf-8', chunksize=250000)

conn.close()
forecastedlist = list(pd.read_csv(os.path.join(directory, 'SalesUnitsPerStore_forecast_result_by_SKU.csv'),sep=',')['SKU'])
tobeforecastedlist = [value for value in levelvalues if value not in forecastedlist] 
tobeforecasted = pd.DataFrame({'SKU':tobeforecastedlist})
tobeforecasted.to_csv(os.path.join(directory, 'tobeforecastedlist.csv'), sep='|', index=None, encoding = 'utf-8')
