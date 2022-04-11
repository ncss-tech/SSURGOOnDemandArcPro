#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Charles.Ferguson
#
# Created:     06/04/2022
# Copyright:   (c) Charles.Ferguson 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import sys, os, arcpy, pandas as pd, requests, caller, zipfile

areas = arcpy.GetParameter(1)
dest = arcpy.GetParameterAsText(2)
unpack = arcpy.GetParameterAsText(3)
#rename = arcpy.GetParameterAsText(4)

areas.sort()

areastring = ",".join(map("'{0}'".format, areas))

q = """SELECT areasymbol, left(areasymbol, 2) as state, areaname, saverest
    FROM sacatalog
    WHERE areasymbol IN (""" + areastring + """)
    ORDER BY areasymbol"""

resp, value = caller.sda(q,False)

if resp:

    columns = value.get('Table')[0]
    data = value.get('Table')[1:]
    # print(columns)
    df = pd.DataFrame(data=data, columns=columns)

    ver = df['saverest'].to_list()
    date = list()
    for i in ver:
        idx = i.find(" ")
        space = i[:idx]
        items = space.split('/')
        month = items[0]
        if len(month) == 1:
            month = '0' + month

        day = items[1]
        if len(day) == 1:
            day = '0'+ day

        year = items[2]

        upload = year + '-' + month + '-' + day
        date.append(upload)

    df['date'] = date

    states = df['state'].to_list()
    templates = ['AK', 'CT', 'FL', 'GA', 'HI', 'IA', 'ID', 'IN', 'ME', 'MI', 'MN', 'MT',\
                 'NC', 'NE', 'NJ', 'OH', 'OR', 'PA', 'SD', 'UT', 'VT', 'WA', 'WI', 'WV',\
                'WY', 'FM']
    dbState = list()

    for s in states:
        if s in templates:
            dbState.append('_soildb_' + s + '_2003_')
        else:
            dbState.append('_soildb_US_2003_')

    df['db'] = dbState

    prefix = 'https://websoilsurvey.sc.egov.usda.gov/DSD/Download/Cache/SSA/wss_SSA_'
    df['URL'] = prefix + df['areasymbol'] + df['db'] + "[" + df['date'] +'].zip'
    df.drop(['date', 'db'], axis = 1, inplace=True)

    # arcpy.AddMessage(df)

    for areasymbol, areaname, url in zip(df['areasymbol'], df['areaname'], df['URL']):


        try:

            arcpy.AddMessage('Downloading ' + areasymbol + ' ' + areaname)
            r = requests.get(url)

        except requests.exceptions.RequestException as e:
            arcpy.AddMessage('Problem downloading ' + areasymbol)
            arcpy.AddMessage(e)

        except Exception as e:
            arcpy.AddMessage(e)

        try:

            open(os.path.join(dest,(os.path.basename(url))), 'wb').write(r.content)

            if unpack == 'true':

                with zipfile.ZipFile(os.path.join(dest,os.path.basename(url)), 'r') as z:
                    z.extractall(dest)

                    #if rename == 'true':
                    base = os.path.basename(url)
                    area = base[8:13]
                    org = os.path.join(dest, area)
                    os.rename(org, os.path.join(dest, 'soil_' + area.lower()))

        except OSError as e:
            arcpy.AddError(e)

        except Exception as e:
            arcpy.AddError(e)



