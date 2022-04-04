# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 10:53:09 2021

@author: Charles.Ferguson
"""

def sdaCall(q):

    import requests, json
    from json.decoder import JSONDecodeError

    try:

        theURL = "https://sdmdataaccess.nrcs.usda.gov"
        theURL = theURL + "/Tabular/SDMTabularService/post.rest"

        rDic = {}


        rDic["format"] = "JSON+COLUMNNAME+METADATA"

        rDic["query"] = q
        rData = json.dumps(rDic)

        results = requests.post(data=rData, url=theURL)

        data = results.json()

        # cols = qData.get('Table')[0]
        # data = qData.get('Table')[1:]

        return True, data

    except JSONDecodeError as e:
        msg = 'JSON Decode error: ' + e.msg
        return False, msg

    except requests.exceptions.RequestException as e:
        msg = e
        return False, msg
    except Exception as e:
        msg = 'Unhadled error in sdaCall ' + e
        return False, msg


def tabRequest(interp, ssa):
    areas = ",".join(map("'{0}'".format, ssa))
    if aggMethod == "Dominant Component":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey  AS mukey,
        (SELECT interphr FROM component INNER JOIN cointerp ON component.cokey = cointerp.cokey AND component.cokey = c.cokey AND ruledepth = 0 AND mrulename LIKE '""" + interp + """') as rating,
        (SELECT interphrc FROM component INNER JOIN cointerp ON component.cokey = cointerp.cokey AND component.cokey = c.cokey AND ruledepth = 0 AND mrulename LIKE '""" + interp + """') as class,
        (SELECT DISTINCT SUBSTRING(  (  SELECT ( '; ' + interphrc)
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey AND compkind != 'miscellaneous area' AND component.cokey=c.cokey
        INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey
        AND ruledepth != 0 AND interphrc NOT LIKE 'Not%' AND mrulename LIKE '""" + interp + """' GROUP BY interphrc, interphr
        ORDER BY interphr DESC, interphrc
        FOR XML PATH('') ), 3, 1000) )as reason
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND l.areasymbol IN (""" + areas + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey  AND c.cokey = (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)"""

    elif aggMethod == "Dominant Condition":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey/1  AS mukey,
       (SELECT TOP 1 ROUND (AVG(interphr) over(partition by interphrc),2)
       FROM mapunit
       INNER JOIN component ON component.mukey=mapunit.mukey
       INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey AND ruledepth = 0 AND mrulename LIKE '""" + interp + """' GROUP BY interphrc, interphr
       ORDER BY SUM (comppct_r) DESC)as rating,
       (SELECT TOP 1 interphrc
       FROM mapunit
       INNER JOIN component ON component.mukey=mapunit.mukey
       INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey AND ruledepth = 0 AND mrulename LIKE '""" + interp + """'
       GROUP BY interphrc, comppct_r ORDER BY SUM(comppct_r) over(partition by interphrc) DESC) as class,

       (SELECT DISTINCT SUBSTRING(  (  SELECT ( '; ' + interphrc)
       FROM mapunit
       INNER JOIN component ON component.mukey=mapunit.mukey AND compkind != 'miscellaneous area' AND component.cokey=c.cokey
       INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey

       AND ruledepth != 0 AND interphrc NOT LIKE 'Not%' AND mrulename LIKE '""" + interp + """' GROUP BY interphrc, interphr
       ORDER BY interphr DESC, interphrc
       FOR XML PATH('') ), 3, 1000) )as reason


       FROM legend  AS l
       INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND l.areasymbol IN (""" + areas + """)
       INNER JOIN  component AS c ON c.mukey = mu.mukey AND c.cokey =
       (SELECT TOP 1 c1.cokey FROM component AS c1
       INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)
       ORDER BY areasymbol, musym, muname, mu.mukey"""

    elif aggMethod == "Weighted Average":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey/1  AS mukey,
        (SELECT TOP 1 CASE WHEN ruledesign = 1 THEN 'limitation'
        WHEN ruledesign = 2 THEN 'suitability' END
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey
        INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey AND ruledepth = 0 AND mrulename LIKE '""" + interp+"""'
        GROUP BY mapunit.mukey, ruledesign) as design,
        ROUND ((SELECT SUM (interphr * comppct_r)
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey
        INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey AND ruledepth = 0 AND mrulename LIKE '""" + interp+"""'
        GROUP BY mapunit.mukey),2) as rating,
        ROUND ((SELECT SUM (comppct_r)
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey
        INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey AND ruledepth = 0 AND mrulename LIKE '""" + interp+"""'
        AND (interphr) IS NOT NULL GROUP BY mapunit.mukey),2) as sum_com,
        (SELECT DISTINCT SUBSTRING(  (  SELECT ( '; ' + interphrc)
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey AND compkind != 'miscellaneous area'
        INNER JOIN cointerp ON component.cokey = cointerp.cokey AND mapunit.mukey = mu.mukey

        AND ruledepth != 0 AND interphrc NOT LIKE 'Not%' AND mrulename LIKE '""" + interp + """' GROUP BY interphrc
        ORDER BY interphrc
        FOR XML PATH('') ), 3, 1000) )as reason

        INTO #main
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND l.areasymbol IN (""" + areas + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey
        GROUP BY  areasymbol, musym, muname, mu.mukey

        SELECT areasymbol, musym, muname, mukey, ISNULL (ROUND ((rating/sum_com),2), 99) AS rating,
        CASE WHEN rating IS NULL THEN 'Not Rated'
        WHEN design = 'suitability' AND  ROUND ((rating/sum_com),2) < = 0 THEN 'Not suited'
        WHEN design = 'suitability' AND  ROUND ((rating/sum_com),2)  > 0.001 and  ROUND ((rating/sum_com),2)  <=0.333 THEN 'Poorly suited'
        WHEN design = 'suitability' AND  ROUND ((rating/sum_com),2)  > 0.334 and  ROUND ((rating/sum_com),2)  <=0.666  THEN 'Moderately suited'
        WHEN design = 'suitability' AND  ROUND ((rating/sum_com),2)  > 0.667 and  ROUND ((rating/sum_com),2)  <=0.999  THEN 'Moderately well suited'
        WHEN design = 'suitability' AND  ROUND ((rating/sum_com),2)   = 1  THEN 'Well suited'

        WHEN design = 'limitation' AND  ROUND ((rating/sum_com),2) < = 0 THEN 'Not limited '
        WHEN design = 'limitation' AND  ROUND ((rating/sum_com),2)  > 0.001 and  ROUND ((rating/sum_com),2)  <=0.333 THEN 'Slightly limited '
        WHEN design = 'limitation' AND  ROUND ((rating/sum_com),2)  > 0.334 and  ROUND ((rating/sum_com),2)  <=0.666  THEN 'Somewhat limited '
        WHEN design = 'limitation' AND  ROUND ((rating/sum_com),2)  > 0.667 and  ROUND ((rating/sum_com),2)  <=0.999  THEN 'Moderately limited '
        WHEN design = 'limitation' AND  ROUND ((rating/sum_com),2)  = 1 THEN 'Very limited' END AS class, reason
        FROM #main
        DROP TABLE #main"""

    return interpQry

def CreateNewTable(newTable, columnNames, columnInfo):
    # Create new table. Start with in-memory and then export to geodatabase table
    #
    # ColumnNames and columnInfo come from the Attribute query JSON string
    # MUKEY would normally be included in the list, but it should already exist in the output featureclass
    #
    try:
        # Dictionary: SQL Server to FGDB
        dType = dict()

        dType["int"] = "long"
        dType["smallint"] = "short"
        dType["bit"] = "short"
        dType["varbinary"] = "blob"
        dType["nvarchar"] = "text"
        dType["varchar"] = "text"
        dType["char"] = "text"
        dType["datetime"] = "date"
        dType["datetime2"] = "date"
        dType["smalldatetime"] = "date"
        dType["decimal"] = "double"
        dType["numeric"] = "double"
        dType["float"] ="double"

        # numeric type conversion depends upon the precision and scale
        dType["numeric"] = "float"  # 4 bytes
        dType["real"] = "double" # 8 bytes

        # Iterate through list of field names and add them to the output table
        i = 0

        # ColumnInfo contains:
        # ColumnOrdinal, ColumnSize, NumericPrecision, NumericScale, ProviderType, IsLong, ProviderSpecificDataType, DataTypeName
        #PrintMsg(" \nFieldName, Length, Precision, Scale, Type", 1)

        outputTbl = os.path.join(r"memory", newTable)
        # arcpy.AddMessage(outputTbl)

        try:
            arcpy.management.CreateTable(os.path.dirname(outputTbl), os.path.basename(outputTbl))
        except:
            arcpy.AddMessage('Unable to create the table')

        for i, fldName in enumerate(columnNames):
            vals = columnInfo[i].split(",")
            length = int(vals[1].split("=")[1])
            precision = int(vals[2].split("=")[1])
            scale = int(vals[3].split("=")[1])
            dataType = dType[vals[4].lower().split("=")[1]]

            if fldName.lower().endswith("key"):
                # Per SSURGO standards, key fields should be string. They come from Soil Data Access as long integer.
                dataType = 'text'
                length = 30
            # arcpy.AddMessage('Adding field ' + fldName)
            arcpy.AddField_management(outputTbl, fldName, dataType, precision, scale, length)

        # desc = arcpy.Describe(outputTbl).fields
        # names = [x.name for x in desc]
        # arcpy.AddMessage(names)

        return True, outputTbl

    except:
        msg = 'Did not create table for ' + interp
        return False, msg


def updateTable(xst, run):

    try:

        insdic = dict()

        # run table does not have the alias
        runfields = ['areasymbol', 'musym', 'muname', 'mukey', 'rating', 'class', 'reason']
        with arcpy.da.SearchCursor(run, runfields) as sCur:
            for row in sCur:
                insdic[row[1]] = row

        # bc we are in the update table func
        # the interp (alias) has run for a previous state
        # so we need to match the results (rating, class, reason)
        # to the appropriate column
        insfields = ['areasymbol', 'musym', 'muname', 'mukey', 'rating_' + alias, 'class_' + alias, 'reason_' + alias]
        cursor = arcpy.da.InsertCursor(xst, insfields)

        for r in insdic.values():
            cursor.insertRow(r)

    except arcpy.ExecuteError:
        arcpy.AddMessage(arcpy.GetMessages(2))

    except:
        # Get the traceback object
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        theMsg = tbinfo + "\n" + str(sys.exc_info()[1])
        arcpy.AddError(theMsg)

# ========================= Prepare parameters =========================

import sys, os, arcpy, traceback

# generte random id to append to temperorary output as search mechanism to
# delete when finished
# rid = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))

arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = True

ssa = arcpy.GetParameter(0)
area_in = arcpy.GetParameter(1)
aggMethod = arcpy.GetParameterAsText(3)
interpParam = arcpy.GetParameter(4)
dest = arcpy.GetParameterAsText(5)
addToSingle = arcpy.GetParameter

arcpy.env.workspace = dest
fail = list()
tables = list()
states = list(set([x[:2] for x in area_in]))
states.sort()

jobs = len(states) * len(interpParam)
n = 0
arcpy.SetProgressor("default", "SSURGO On-Demand: Jobs(" + str(n) + " of " + str(jobs) + ")")

try:

    aggAbbr = dict()
    aggAbbr['Dominant Condition'] = 'dom_cond'
    aggAbbr['Dominant Component'] = 'dom_comp'
    aggAbbr['Weighted Average'] = 'wtavg'

    for interp in interpParam:

        arcpy.AddMessage('Running interpretation: ' + interp)
        # n += 1
        # arcpy.SetProgressorLabel("SSURGO On-Demand: Jobs (" + str(n) + " of " + str(jobs) + ")")

        # this is a leftover, prob don't need to rename interp in validation
        # with need to replace here
        if interp.find("{:}") != -1:
            interp = interp.replace("{:}", ";")

        # column names restricted in length, make small as possible
        aliasA = interp.replace(" ", "")
        aliasB = arcpy.ValidateFieldName(aliasA,dest)
        aliasC = aliasB.replace("_", "")
        alias = aliasC

        tblNameAlias = arcpy.ValidateTableName(interp, dest)

        tblinfo = (aggAbbr.get(aggMethod), tblNameAlias)
        # tblinfo = (aggAbbr.get(aggMethod), 'multi_interps')

        newName = "sod_" + "_".join(map("{0}".format, tblinfo))
        newName = newName.replace("___", "_")
        newName = newName.replace("__", "_")

        if newName.endswith("_"):
            newName = newName[:-1]

        sod_tab = os.path.join(dest, newName)
        tables.append(sod_tab)

        for state in states:

            n+= 1
            arcpy.SetProgressorLabel("SSURGO On-Demand: Jobs (" + str(n) + " of " + str(jobs) + ")")

            req_areas = [x for x in area_in if x[:2] == state]

            arcpy.AddMessage('Running State: ' + state)

            theQ = tabRequest(interp, req_areas)
            # arcpy.AddMessage(theQ)

            tabBool, tabData = sdaCall(theQ)

            # the tabular request returned something
            if tabBool:

                if len(tabData) == 0:
                    fail.append(interp)
                    arcpy.AddMessage('No interpretation information returned for ') + interp

                else:
                    # each run returns: areasymbol,musym,mukey,rating,class,reason
                    # add the alias(interp) to the rating class reason columns
                    tabCols = tabData.get('Table')[0]
                    tabMeta = tabData.get('Table')[1]
                    tabData = tabData.get('Table')[2:]

                    cntbool, cntval = CreateNewTable("temp_table", tabCols, tabMeta)

                    if cntbool:
                        #  inserting the data into the temp table
                        with arcpy.da.InsertCursor(cntval, tabCols) as cursor:
                            for row in tabData:
                                cursor.insertRow(row)

                        # if the interp has not been run for any state then...
                        if not arcpy.Exists(sod_tab):
                            arcpy.conversion.TableToTable(cntval, dest, newName)

                            genflds = ['rating', 'class', 'reason']
                            sodflds = arcpy.Describe(sod_tab).fields

                            # here we are creating fields for 'rating', 'class', 'reason'
                            # with the alias appended to name, calculating them from original
                            # the deleteing them. arcpy AlterField is broken
                            for fld in sodflds:
                                if fld.name in genflds:
                                    # arcpy.management.AlterField(sod_tab, fld.name, fld.name + "_" + alias)

                                    fname = fld.name
                                    falias = fname + "_" + alias
                                    ftype = fld.type
                                    flen = fld.length

                                    # add the field, if not present
                                    arcpy.management.AddField(sod_tab, falias, ftype, None, None, flen)
                                    expr = "!" + fld.name + "!"
                                    arcpy.management.CalculateField(sod_tab, falias, expr)
                                    arcpy.management.DeleteField(sod_tab, fld.name)

                            arcpy.management.Delete(cntval)

                        # the interp has been run for at least 1 state then...
                        else:
                            updateTable(sod_tab, cntval)

                    # if the create new able failed, where
                    else:
                        arcpy.AddMessage(cntval + " " + state)

                    # add line to separate messages
                    # arcpy.AddMessage(u"\u200B")

            # the tabular call failed, SDA down??? Proceed to the next interp
            else:
                # fail.append(state + ":" + interp)
                # arcpy.AddMessage('Error while collecting information returned for ' + interp)
                arcpy.AddMessage(tabData)

    if addToSingle:
        f = list()
        # copy the first table to use as a template
        multi = os.path.join(dest, 'sod_' + aggAbbr.get(aggMethod) + "_multi_interps")

        # create a list, insert tuples of rows
        # this is used bc not all interps have
        # a record for every map unit
        # effectively a unique set of mukeys accross all the interps
        house = list()
        for table in tables:
            with arcpy.da.SearchCursor(table, ['areasymbol', 'musym', 'muname', 'mukey']) as rows:
                for row in rows:
                    house.append(row)

        unq = list(set(house))
        unq.sort()

        arcpy.management.CreateTable(dest, 'sod_' + aggAbbr.get(aggMethod) + "_multi_interps", tables[0])
        for field in arcpy.ListFields(tables[0])[1:4]:
            arcpy.management.AddField(multi, field.name, field.type, field.length)

        with arcpy.da.InsertCursor(multi, ['areasymbol', 'musym', 'muname', 'mukey']) as cursor:

            for u in unq:
                cursor.insertRow(u)

        # get the required fields from remaining
        # tables and add to multi
        for table in tables:
            # arcpy.AddMessage(table)

            # get only the last 3 (rating, class, rason)
            # these have the alias (interp) already
            fields = arcpy.Describe(table).fields[-3:]

            # create a list to grab data filed names
            flist = list()
            for field in fields:
                fname = field.name
                ftype = field.type
                flen = field.length

                arcpy.management.AddField(multi, fname, ftype, None, None, flen)
                flist.append(fname)
                # arcpy.AddMessage(fname)

            # insert mukey into flist for cursors below
            flist.insert(0, 'mukey')

            # get the data from the current table
            # to put into the multi talbe
            data = dict()
            with arcpy.da.SearchCursor(table, flist) as rows:
                for row in rows:
                    data[row[0]] = row[1], row[2], row[3]

            # add the data to the multi table
            with arcpy.da.UpdateCursor(multi, flist) as rows:
                for row in rows:
                    val = data.get(row[0])
                    if val:
                        row[1] = val[0]
                        row[2] = val[1]
                        row[3] = val[2]

                        rows.updateRow(row)

                    else:
                        f.append(row[0])

            # fstr = ','.join(map("'{0}'".format, f))
            # arcpy.AddMessage(fstr)


    if len(fail) > 0:
        fail.sort()
        fstr = ','.join(map("{0}".format, fail))
        arcpy.AddError('The following interpretation(s) failed to execute or returned no results:')
        arcpy.AddError(fstr)
        arcpy.AddMessage(u"\u200B")

except arcpy.ExecuteError:
    arcpy.AddMessage(arcpy.GetMessages())

except:
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    theMsg = tbinfo + "\n" + str(sys.exc_info()[1])
    arcpy.AddError(theMsg)
