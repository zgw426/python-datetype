import pandas as pd
from datetime import datetime as dt
import os

vPath = "logs/"
allfiles = os.listdir(path="logs/")
csvfiles = list( filter( lambda x: 'log-' in x, allfiles ) )

df_stp1 = pd.read_csv( vPath+csvfiles[0], encoding="utf_8" )

csvfiles.pop(0)
for csvfile in csvfiles:
        try:
                df_tmp = pd.read_csv( vPath+csvfile, encoding="utf_8" )
                df_stp1 = pd.concat([df_stp1,df_tmp])
        except:
                print("csv File {0} load error.".format(csvfile))

df_stp2=[]
for index, row in df_stp1.iterrows():
        tmpChFlg = 0
        for item in row:
                if item != item:
                        tmpChFlg = 1
        if tmpChFlg == 0:
                tmpStr = "###".join(row)
                df_stp2.append( tmpStr )

df_stp2 = set( df_stp2 )

df = []
for item in df_stp2:
        tmplist = item.split("###")
        df.append( tmplist )

df = pd.DataFrame( df, columns=["CreationTime","Operation","ResourceTitle","ResourceUrl","ObjectId","UserId","ResultStatus"])
df["ResourceUrl"] = df["ResourceUrl"].replace('hoge(.*)', r'piyo\1', regex=True)
df["CreationTime"] = pd.to_datetime(df["CreationTime"], utc=True)

ctjst = pd.DatetimeIndex(df.CreationTime , name='CreationTime_JST')
ctjst = ctjst.tz_convert('Asia/Tokyo')
df["CreationTime_JST"] = ctjst
df = df.sort_values('CreationTime_JST')

df["year"] = df["CreationTime_JST"].dt.year
df["month"] = df["CreationTime_JST"].dt.month
df["day"] = df["CreationTime_JST"].dt.day
df["hour"] = df["CreationTime_JST"].dt.hour

df = df.drop("CreationTime",axis=1)
df = df[ ["CreationTime_JST","year","month","day","hour","Operation","ResourceTitle","ResourceUrl","ObjectId","UserId","ResultStatus"] ]

df.to_csv("df.csv", sep=",", encoding='utf_8_sig',index=False)

#dfpivot_table = df.pivot_table(values = "CreationTime_JST", index = ["ObjectId","year","month","day","ResourceTitle","ResourceUrl"], columns = ["Operation"], aggfunc = 'count', fill_value = 0, margins = True).sort_values(['All'])
dfpivot_table = df.pivot_table(values = ["CreationTime_JST"], index = ["ObjectId","ResourceTitle","ResourceUrl"], columns = ["year","month"], aggfunc = 'count', fill_value = 0, margins = True)

dfAdds = []
for index, row in dfpivot_table.iterrows():
        if "All" not in index[0]:
                tmpAdds = []
                tmpObjectId = index[0]
                tmpdf = df[ df["ObjectId"] == tmpObjectId ]
                tmpAdds.append( tmpdf["CreationTime_JST"].head(1) )
                tmpAdds.append( tmpdf["CreationTime_JST"].tail(1) )
                tmpAdds.append( len(tmpdf) )
                dfAdds.append(tmpAdds)

dfAdds = pd.DataFrame( dfAdds, columns=["OldestTime","LatestTime","NumOfLines"])
print(dfAdds)

dfpivot_table.to_csv("PivotTable.csv", sep=",", encoding='utf_8_sig')

