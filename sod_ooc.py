#-------------------------------------------------------------------------------
# Name:        module2
# Purpose:
#
# Author:      Charles.Ferguson
#
# Created:     08/04/2022
# Copyright:   (c) Charles.Ferguson 2022
# Licence:     <your licence>
#-------------------------------------------------------------------------------

def outofcycle(date):
     """SSURGO data sets are updated annually around October 1.  Throughout the year
     it is possible a survey area is uploaded after this date.  This function identifies
     these survey areas which will help you determine if your local data set(s) need updating.

     :return data frame: pandas data frame"""

     # import caller, pandas as pd

     q = """select left(areasymbol, 2) as state, areasymbol, areaname, saverest
     from sacatalog
     where saverest > """ + date + """
     ORDER BY areasymbol, saverest"""

     resp, data = caller.sda(q=q, meta=False)

     table = data.get('Table')

     df = pd.DataFrame(data = table[2:], columns = table[0])

     return df

import arcpy, os, caller, pandas as pd, numpy, requests

# arcpy.AddMessage('Getting date from Git...')
url = 'https://raw.githubusercontent.com/ncss-tech/SSURGOOnDemandArcPro/master/refresh_date'
r = requests.get(url)
date = r.text

ooc = outofcycle(date)

arcpy.AddMessage(ooc.to_string(index = False))

try:

    dest = arcpy.GetParameterAsText(1)

    if not dest == '':
        wstype = arcpy.Describe(dest).workspaceFactoryProgID

        if wstype == '':

            ooc.to_csv(os.path.join(dest, 'SSURGO_OutOfCycle.csv'), index=False)

        elif wstype.startswith('esriDataSourcesGDB.FileGDBWorkspaceFactory'):

            dname = os.path.dirname(dest)
            ooc.to_csv(os.path.join(dname, 'SSURGO_OutOfCycle.csv'), index=False)
            arcpy.management.CopyRows(os.path.join(dname, 'SSURGO_OutOfCycle.csv'), os.path.join(dest, 'SSURGO_OutOfCycle'))
            os.remove(os.path.join(dname, 'SSURGO_OutOfCycle.csv'))

except arcpy.ExecuteError:
    arcpy.AddError(arcpy.GetMessages(2))


