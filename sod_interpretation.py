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
            
        return data
        
    except JSONDecodeError as e:
        err = 'This usually happens with incorrect syntax or too large an extent.'
        print('JSON Decode error: ' + e.msg + '\n' + err)
        raise AttributeError('Received NoneType from SDA')
        
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
        
    except Exception as e:
        print('Unhandled error')
        print(e)
        

def tabRequest(interp, keys):

   if aggMethod == "Dominant Component":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey  AS MUKEY,
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
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey  AND c.cokey = (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)"""

   elif aggMethod == "Dominant Condition":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey/1  AS MUKEY,
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
       INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
       INNER JOIN  component AS c ON c.mukey = mu.mukey AND c.cokey =
       (SELECT TOP 1 c1.cokey FROM component AS c1
       INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)
       ORDER BY areasymbol, musym, muname, mu.mukey"""

   elif aggMethod == "Weighted Average":
       interpQry = """SELECT areasymbol, musym, muname, mu.mukey/1  AS MUKEY,
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
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey
        GROUP BY  areasymbol, musym, muname, mu.mukey
    
        SELECT areasymbol, musym, muname, MUKEY, ISNULL (ROUND ((rating/sum_com),2), 99) AS rating,
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
    
    
def updateTable(spatial, tabular):

    # add the standard fields if they aren't in the spatial table 
    housefields = ['areasymbol', 'musym', 'muname']
    havefields = [str(x.name) for x in arcpy.Describe(spatial).fields]
    needfields = [f for f in housefields if not f in havefields]
    
    fobjs = [f for f in arcpy.Describe(tabular).fields if str(f.name) in needfields]
    if len(fobjs) > 0:
        for f in fobjs:
            fname = f.name 
            ftype = f.type
            flen = f.length
        
            arcpy.management.AddField(spatial, fname, ftype, None, None, flen)
        
            # build the dict from tabular to populate the spatial
            hDict = dict()
            with arcpy.da.SearchCursor(tabular, ["mukey", fname]) as rows:
                for row in rows:
                    hDict[str(row[0])] = row[1]
            
            # now iterate over the spatial using the dict 
            with arcpy.da.UpdateCursor(spatial, ["mukey", fname]) as rows:
                for row in rows:
                    val = hDict.get(row[0])
                    row[1] = val
                    rows.updateRow(row)
        
    
    # we will always need the last 3 fields(rating, class, reason) 
    fields = arcpy.Describe(tabular).fields[-3:]
    for f in fields:
        fname = f.name
        falias = fname + "_" + alias
        ftype = f.type
        flen = f.length
    
        arcpy.management.AddField(spatial, falias, ftype, None, None, flen)
    
        pDict = dict()
        with arcpy.da.SearchCursor(tabular, ["mukey", fname]) as rows:
            for row in rows:
                pDict[str(row[0])] = row[1]
    
        with arcpy.da.UpdateCursor(spatial, ["mukey", falias]) as rows:
            for row in rows:
                val = pDict.get(row[0])
                row[1] = val
                rows.updateRow(row)
            
    
   
# ========================= Prepare parameters =========================

import sys, os, arcpy, string, random, traceback

# generte random id to append to temperorary output as search mechanism to
# delete when finished
rid = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))

arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = True

clu_in = arcpy.GetParameter(0)
aggMethod = arcpy.GetParameterAsText(1)
arcpy.AddMessage(aggMethod)
interpParam = arcpy.GetParameter(2)
dest = arcpy.GetParameterAsText(3)
addToGeom = arcpy.GetParameterAsText(4)

arcpy.env.workspace = dest

# ========================= Get the SSURGO Geometry =========================
try:
    
    desc = arcpy.Describe(clu_in)
    sel = desc.FIDSet
    if sel  == '':
        err = "Select at least 1 feature"
        arcpy.AddMessage(err)
        raise TypeError(err)
    
    
    # get coordinate system
    code = desc.spatialReference.GCSCode
    
    
    if code == 4326:
        arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
        hull_target = os.path.join(dest, "clu_sel_" + rid)
    
    # NAD83 and NAD83(2011) 
    elif code == 4269 or code == 6318:
        arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
        trm = "WGS_1984_(ITRF00)_To_NAD_1983"
        arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326), trm)
        hull_target =  os.path.join(dest, "clu_prj_" + rid)
    
    else:
        err = 'This tool only supports spatial reference objects based on WGS84 or NAD83'
        raise TypeError(err)
    
    
    # dissolve feature(s) into 1 part
    # get the smallest feature possible and its wkt
    arcpy.management.Dissolve(hull_target, os.path.join(dest, "sod_sngl_prt_" + rid))
    with arcpy.da.SearchCursor(os.path.join(dest, "sod_sngl_prt_" + rid), "SHAPE@") as rows:
        for row in rows:
            hull = row[0].convexHull()
            wkt = hull.WKT
            # arcpy.AddMessage(wkt)
    
    geoQ = """~DeclareGeometry(@aoi)~
    select @aoi = geometry::STGeomFromText(
      '""" + wkt + """', 4326)
    
    ~DeclareIdGeomTable(@intersectedPolygonGeometries)~
    ~GetClippedMapunits(@aoi,polygon,geo,@intersectedPolygonGeometries)~
    
    select id, geom
    from @intersectedPolygonGeometries"""
    
    # arcpy.AddMessage(geoQ)
    geoResults = sdaCall(geoQ)
    
    geoCols = geoResults.get('Table')[0]
    geoMeta = geoResults.get('Table')[1]
    geoData = geoResults.get('Table')[2:]
    
    # arcpy.AddMessage(geoData)
    
    arcpy.management.CreateFeatureclass(dest, "sod_temp_" + rid, "POLYGON", None, None, None, arcpy.SpatialReference(4326))
    arcpy.management.AddField(os.path.join(dest, "sod_temp_" + rid), "mukey", "TEXT", None, None, "30")
    
    with arcpy.da.InsertCursor(os.path.join(dest, "sod_temp_" + rid), ["SHAPE@WKT", "mukey"]) as cursor:
        for data in geoData:
            mukey = data[0]
            geom = data[1]
                
            row = geom, mukey
            cursor.insertRow(row)
        
    arcpy.analysis.Clip(os.path.join(dest, "sod_temp_" + rid), os.path.join(dest, "sod_sngl_prt_" + rid),  os.path.join(dest, "SSURGO_On_Demand_interpretation"))
    
    # clean up temporary files
    for f in arcpy.ListFeatureClasses("*_" + rid):
        arcpy.management.Delete(f)
        
    
    # ========================= Get the mukeys of the returned geometry =========================
    
    with arcpy.da.SearchCursor(os.path.join(dest, "SSURGO_On_Demand_interpretation"), "mukey") as rows:
        geoKeys = sorted({row[0] for row in rows})
        
    keys = ",".join(map("'{0}'".format, geoKeys))
    
    # ========================= Get the mukeys of the returned geometry =========================
    
    # this dictionary is used for naming tables appropriately
    aggAbbr = dict()
    aggAbbr['Dominant Condition'] = 'dom_cond'
    aggAbbr['Dominant Component'] = 'dom_comp'
    aggAbbr['Weighted Average'] = 'wtavg'
    
    
    
    for interp in interpParam:
        
        # this is a leftover, prob don't need to rename interp in validation
        # with need to replace here
        if interp.find("{:}") != -1:
            interp = interp.replace("{:}", ";")
        
        # column names restricted in length, make small as possible
        aliasA = interp.replace(" ", "")
        aliasB = arcpy.ValidateFieldName(aliasA,dest)
        aliasC = aliasB.replace("_", "") 
        
        alias = aliasC
        
        theQ = tabRequest(interp, keys)
        # arcpy.AddMessage(theQ)
        
        tabData = sdaCall(theQ)
        tabCols = tabData.get('Table')[0]
        tabMeta = tabData.get('Table')[1]
        tabData = tabData.get('Table')[2:]        
            
        # tblinfo = (aggAbbr.get(aggMethod), sdaCol, tDep, bDep)
        tblNameAlias = arcpy.ValidateTableName(interp, dest)
        tblinfo = (aggAbbr.get(aggMethod), tblNameAlias)
    
        newName = "sod_" +  "_".join(map("{0}".format, tblinfo))
        newName = newName.replace("___", "_")
        newName = newName.replace("__", "_")
        
        if newName.endswith("_"):
            newName = newName[:-1]
        
        arcpy.AddMessage(newName)
        
        tbool, tval = CreateNewTable("temp_table", tabCols, tabMeta)
        
        if tbool:
        
            with arcpy.da.InsertCursor(tval, tabCols) as cursor:
                for row in tabData:
                    cursor.insertRow(row)
                    
            arcpy.conversion.TableToTable(tval, dest, newName)
            arcpy.management.Delete(tval)
        
        else:
            arcpy.AddMessage(tval)
            
        
        if addToGeom == 'true':
            
            sod_geom = os.path.join(dest, "SSURGO_On_Demand_interpretation")
            sod_tab = os.path.join(dest, newName)
            
            updateTable(sod_geom, sod_tab)
            
except arcpy.ExecuteError: 
    arcpy.AddMessage(arcpy.GetMessages())

except:
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    theMsg =  tbinfo + "\n" + str(sys.exc_info()[1])

    arcpy.AddError(theMsg)        
        
    # arcpy.AddMessage(tabCols)
    # arcpy.AddMessage(tabMeta)
    
    # if bSingle == 'true':
    #     tblinfo = (aggAbbr.get(aggMethod), 'multi_prop', tDep, bDep)
    #     newName = "sod_" +  "_".join(map("{0}".format, tblinfo))
    #     newName = newName.replace("__", "_")
    #     if newName.endswith("_"):
    #         newName = newName[:-1]
        
    #     # create memory table
    #     tbool, tval = CreateNewTable("temp_table", tabCols, tabMeta)
        
    #     if tbool:
        
    #         with arcpy.da.InsertCursor(tval, tabCols) as cursor:
    #             for row in tabData:
    #                 cursor.insertRow(row)
                    
    #         #  test if the multi_prop table exists
    #         if not arcpy.Exists(os.path.join(dest, newName)):
    #             arcpy.conversion.TableToTable(tval, dest, newName)
            
    #         # if it exists create dictionar
    #         else:
    #             desc = arcpy.Describe(os.path.join(dest, newName))
    #             xstFlds = [f.name for f in desc.fields]
                
    #             # only 1 field returned here, the property that is currently executed
    #             for fld in arcpy.Describe(tval).fields:
    #                 if fld.name not in xstFlds:
    #                     arcpy.management.AddField(os.path.join(dest, newName), fld.name, fld.type, fld.precision, fld.scale, fld.length)
                
    #             tblDict = dict()

    #             with arcpy.da.SearchCursor(tval, ["mukey", fld.name]) as rows:
    #                 for row in rows:
    #                     tblDict[row[0]] = row[1]

    #             with arcpy.da.UpdateCursor(os.path.join(dest, newName), ["mukey", fld.name]) as rows:
    #                 for row in rows:
    #                     val = tblDict.get(row[0])
    #                     row[1] = val
    #                     rows.updateRow(row)
    #     else:
    #         arcpy.AddMessage(tval)
    # =========================================================================
    # if bLyrs == 'true':
    #     aprx = arcpy.mp.ArcGISProject("CURRENT")
    #     m = aprx.listMaps()[0]
        
    #     # add the table just built
    #     m.addDataFromPath(os.path.join(dest, newName))
        
    #     lyrs = [l.name for l in  m.listLayers()]
    #     if  not "SSURGO_On_Demand" in lyrs:
    #         m.addDataFromPath(os.path.join(dest, "SSURGO_On_Demand"))
                    
    #     ssurgoLyr = m.listLayers("SSURGO_On_Demand")[0]
    #     tblLyr = m.listTables(newName)[0]
        
    #     # arcpy.AddMessage(tblLyr.name)
            
    #     try:
    #         # lyrxName =  "SSURGO_On_Demand_" + newName[4:]
            
    #         arcpy.management.ValidateJoin(ssurgoLyr, 'mukey',tblLyr, 'mukey')
    #         arcpy.AddMessage(arcpy.GetMessages())
    #         arcpy.management.JoinField(ssurgoLyr, 'mukey',tblLyr, 'mukey')
            
    #         # arcpy.management.AddJoin(ssurgoLyr, 'mukey',tblLyr, 'mukey')
            
    #     except:
    #         arcpy.AddMessage("Unable to add join")
        
        # create a output lyrx name
        # lyrxName =  "SSURGO_On_Demand_" + newName[4:]
        
        # rename the SSURGO On Demand layer in TOC
        # ssurgoLyr.name
        
        # ssurgoLyr.saveACopy(os.path.join(os.path.dirname(dest), lyrxName + ".lyrx"))
        # arcpy.management.RemoveJoin(ssurgoLyr)
        # m.addDataFromPath(os.path.join(os.path.dirname(dest), theLyr.name + ".lyrx"))
        
        
        
        
        
        
        
    
    
    
    

    
    
