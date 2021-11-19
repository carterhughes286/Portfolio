import arcgis
import arcpy
import pandas as pd
from copy import deepcopy
import datetime
import numpy as np
import yagmail

gis = arcgis.gis.GIS(profile='work')

# Athletic Fields Data feature service
feature_service = gis.content.get('79203c5b0ac34cffb304c912e284bc83')
feature_layers = feature_service.layers
feature_tables = feature_service.tables

# Athletic Fields
field_layer = feature_layers[0]
field_layer_features = field_layer.query().features

# list of current Athletic Fields features
# limited to only fields reviewed by the Athletic Fields Inventory Project (currently only the South)
field_features_list = [f for f in field_layer_features if f.attributes['MGMT_REGION'] == 'South' and f.attributes['OWNER'] == 'M-NCPPC']

# list of current asset IDs for Athletic Fields features
assetID_list = [f.attributes['ASSET_ID'] for f in field_features_list]

# Athletic Fields Labor
labor_table = feature_tables[0]
labor_features = labor_table.query().features

# list of current Athletic Fields Labor features
labor_features_list = [f for f in labor_features]

# template feature for Athletic Fields Labor
template_feature = deepcopy(labor_features_list[0])

# reports exported to shared OneDrive location and script requires the location to be saved/synced locally

# athletic fields routine activities dataframe
df = pd.concat(
    [pd.read_excel(
        r'C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\Cognos_PowrBI_Automation\NP_SP_AthleticFields_Routine_Standings_GENERAL.xlsx',
        sheet_name='Labor Rates_1',
        skiprows=3),
    pd.read_excel(
        r'C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\Cognos_PowrBI_Automation\NP_SP_AthleticFields_Routine_Standings_GENERAL.xlsx',
        sheet_name='Labor Rates_2',
        skiprows=3)])

# change Booked Dates column to datetime
df['Booked Dates'] = pd.to_datetime(df['Booked Dates'])

# filter dataframe by the specified start and end dates
start_date = '01-01-2021'
end_date = '12-31-2021' 
mask = ( df['Booked Dates'] > start_date) & (df['Booked Dates'] <= end_date) & (df['WO Status'].isin(['Completed - Field Completed ', 'Completed']) )
df = df.loc[mask]
df = df[~df["WO Asset ID"].str.contains('MCPS')]
# combine the NORMAL Rate and OVERTIME columns into a single column titled Rate
df = df.rename(columns={'NORMAL Rate': 'Rate'})
df['Rate'] = df['Rate'].fillna('OVERTIME')

# drop unnecessary columns from dataframe
df = df.drop(columns=['GIS Object Reference ID', 'Booked Employee Name', 'OVERTIME Rate', 'WO Status', 'WO Type'])

# filter dataframe to asset IDs present in the list of current asset IDs for Athletic Fields features
df = df[df['WO Asset ID'].isin(assetID_list)]

# list of str pairs of asset IDs and WOs
assetID_WO_list = [f.attributes['WO_Asset_ID'] + ', ' + str(f.attributes['WO_Number']) for f in labor_features_list]
# if a pair exists in the table already, the row has already created a feature since each pair is unique

# 
df['WO Number'] = df['WO Number'].astype(str)
df['unique'] = df['WO Asset ID'] + ', ' + df['WO Number']

df = df[~df['unique'].isin(assetID_WO_list)]
df = df.drop(columns=['unique'])

# remove spaces in columns name
df.columns = df.columns.str.replace(' ','_')

# change Wo Number column to int
df['WO_Number'] = df['WO_Number'].astype(int)

# sort the dataframe by Booked Dates and reset the dataframes indexes
df = df.sort_values('Booked_Dates')
df = df.reset_index(drop=True)

# UPDATING THE ATHLETIC FIELDS LABOR TABLE

with arcpy.da.InsertCursor(
    in_table=labor_table.url,
    field_names=df.columns.tolist()) as cursor:
    
    # iterate over each labor record (row) in the dataframe
    for index, row in df.iterrows():
        
        cursor.insertRow(tuple([row[column] for column in df.columns.tolist()]))

labor_adds = len(df)

# UPDATING THE ATHLETIC FIELD POINTS LAYER

# dict of activity groups (keys) currently tracked in the Athletic Fields layer and their corresponding activity codes (values)
activity_dict = {
    'MOW': ['MOW'],
    'DRAGLINE': ['DRAG', 'CHALK LINE', 'CHALK/LINE'],
    'PAINT': ['PAINT']
    }

# empty list to later hold updates to Athletic Field features
fields_updates_list = []

# iterate through each field feature
for f in field_features_list:
    
    assetID = f.attributes['ASSET_ID']

    # create deepcopy of the feature to update
    f_update = deepcopy(f)

    # create an update counter, if 0 then no update to be appended to list
    update_counter = 0

    # iterate through activity groups (keys) currently tracked in the Athletic Fields layer and their corresponding activity codes (values)
    for key, value in activity_dict.items():

        # create string of corresponding layer activity field
        activity_field = key + '_DATE'

        # filter df to by the asset ID and group of activities
        activity_df = df[(df['WO_Asset_ID'] == assetID) & df['WO_Activity'].isin(value)]

        # check if the filtered df contains any values
        if assetID in activity_df['WO_Asset_ID'].values:

            # sort filtered df by Booked Dates and drop all rows except the one with the most recent Booked Date
            activity_df = activity_df.sort_values('Booked_Dates').drop_duplicates('WO_Asset_ID', keep='last')

            # get the most recent booked date
            for index, row in activity_df.iterrows():
                break
            booked_date = row['Booked_Dates']

            # convert dateime into AGOL friendly datetime
            if isinstance(booked_date, int):
                booked_date = datetime.datetime.fromtimestamp(int(booked_date)/1000)
            else:
                booked_date = pd.Timestamp(booked_date, tz=None).to_pydatetime()
            
            try:
                current_date = datetime.datetime.fromtimestamp(int(f.attributes[activity_field])/1000)

                if booked_date.date() > current_date.date():

                    # update the corresponding layer activity field to the most recent Booked Date
                    f_update.attributes[activity_field] = str(booked_date)

                    # count the update with the counter
                    update_counter += 1

            except:
                # update the corresponding layer activity field to the most recent Booked Date
                f_update.attributes[activity_field] = str(booked_date)

                # count the update with the counter
                update_counter += 1

    # append the update feature to the update list if an actual update occured 
    if update_counter != 0:
        fields_updates_list.append(f_update)

update_list_len = len(fields_updates_list)

# updates features in list segments of 250 to prevent errors
i = 0
new_updates_list = []
for item in fields_updates_list:

    for attribute, value in item.attributes.items():
        if type(value) == str:
            if '<' in value or '>' in value:
                item.attributes[attribute] = None

    new_updates_list.append(item)
    if i == 249 or item == fields_updates_list[-1]:
        field_layer.edit_features(updates=new_updates_list)
        new_adds_list = []
        i = 0
    else:
        i = i + 1

# send email notification detailing update success and content

my_address = 'carterpython@gmail.com'
password = 'citaeupaersxiely'
to_address = 'carter.hughes@montgomeryparks.org'

#initializing the server connection
yag = yagmail.SMTP(user=my_address, password=password)

#sending the email
yag.send(to=to_address,
        subject='Athletic Fields Data - Auto Update',
        contents='Update to Athletic Fields Data was successful!\nNew Athletic Fields Labor Records: ' + str(labor_adds) + '\nNew Athletic Field Points Updates: ' + str(update_list_len))
