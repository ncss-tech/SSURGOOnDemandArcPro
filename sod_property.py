# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 10:53:09 2021

@author: Charles.Ferguson
"""

def sdaCall(q, var=str):
    
    import requests, json
    from json.decoder import JSONDecodeError
    from requests import exceptions
    
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
            
        return True, data, var
        
    except requests.exceptions.RequestException as e:
        return False, e, var
        
    except JSONDecodeError as e:
        
        msg = ('JSON Decode error: ' + e.msg)
        return False, msg, var
    
    except Exception as e:
        arcpy.AddMessage('Unhandled error in sdaCall')
        return False, e, var
        
def tabRequest(aProp):


    if aggMethod == "Dominant Component (Category)":
        pQry = """SELECT areasymbol, musym, muname, mu.mukey  AS mukey, """ + aProp + """ AS """ + aProp + """
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey
        AND mu.mukey IN (""" + keys + """)
        INNER JOIN component AS c ON c.mukey = mu.mukey
        AND c.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)"""
    elif aggMethod == "Weighted Average":
        pQry = """SELECT areasymbol, musym, muname, mukey
        INTO #kitchensink
        FROM legend  AS lks
        INNER JOIN  mapunit AS muks ON muks.lkey = lks.lkey AND muks.mukey IN (""" + keys + """)
        SELECT mu1.mukey, cokey, comppct_r,
        SUM (comppct_r) over(partition by mu1.mukey ) AS SUM_COMP_PCT
        INTO #comp_temp
        FROM legend  AS l1
        INNER JOIN  mapunit AS mu1 ON mu1.lkey = l1.lkey AND mu1.mukey IN (""" + keys + """)
        INNER JOIN  component AS c1 ON c1.mukey = mu1.mukey AND majcompflag = 'Yes'
        SELECT cokey, SUM_COMP_PCT, CASE WHEN comppct_r = SUM_COMP_PCT THEN 1
        ELSE CAST (CAST (comppct_r AS  decimal (5,2)) / CAST (SUM_COMP_PCT AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
        INTO #comp_temp3
        FROM #comp_temp
        SELECT
        areasymbol, musym, muname, mu.mukey/1  AS MUKEY, c.cokey AS COKEY, ch.chkey/1 AS CHKEY, compname, hzname, hzdept_r, hzdepb_r, CASE WHEN hzdept_r <""" + tDep + """  THEN """ + tDep + """ ELSE hzdept_r END AS hzdept_r_ADJ,
        CASE WHEN hzdepb_r > """ + bDep + """  THEN """ + bDep + """ ELSE hzdepb_r END AS hzdepb_r_ADJ,
        CAST (CASE WHEN hzdepb_r > """ +bDep + """  THEN """ +bDep + """ ELSE hzdepb_r END - CASE WHEN hzdept_r <""" + tDep + """ THEN """ + tDep + """ ELSE hzdept_r END AS decimal (5,2)) AS thickness,
        comppct_r,
        CAST (SUM (CASE WHEN hzdepb_r > """ + bDep + """  THEN """ + bDep + """ ELSE hzdepb_r END - CASE WHEN hzdept_r <""" + tDep + """ THEN """ + tDep + """ ELSE hzdept_r END) over(partition by c.cokey) AS decimal (5,2)) AS sum_thickness,
        CAST (ISNULL (""" + aProp + """, 0) AS decimal (5,2))AS """ + aProp + """
        INTO #main
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey
        INNER JOIN chorizon AS ch ON ch.cokey=c.cokey AND hzname NOT LIKE '%O%'AND hzname NOT LIKE '%r%'
        AND hzdepb_r >""" + tDep + """ AND hzdept_r <""" + bDep + """
        INNER JOIN chtexturegrp AS cht ON ch.chkey=cht.chkey  WHERE cht.rvindicator = 'yes' AND  ch.hzdept_r IS NOT NULL
        AND texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM' and texture NOT LIKE '%MPT%' and texture NOT LIKE '%MUCK' and texture NOT LIKE '%PEAT%' and texture NOT LIKE '%br%' and texture NOT LIKE '%wb%'
        ORDER BY areasymbol, musym, muname, mu.mukey, comppct_r DESC, cokey,  hzdept_r, hzdepb_r
        SELECT #main.areasymbol, #main.musym, #main.muname, #main.MUKEY,
        #main.COKEY, #main.CHKEY, #main.compname, hzname, hzdept_r, hzdepb_r, hzdept_r_ADJ, hzdepb_r_ADJ, thickness, sum_thickness, """ + aProp + """, comppct_r, SUM_COMP_PCT, WEIGHTED_COMP_PCT ,
        SUM((thickness/sum_thickness ) * """ + aProp + """ )over(partition by #main.COKEY)AS COMP_WEIGHTED_AVERAGE
        INTO #comp_temp2
        FROM #main
        INNER JOIN #comp_temp3 ON #comp_temp3.cokey=#main.cokey
        ORDER BY #main.areasymbol, #main.musym, #main.muname, #main.MUKEY, comppct_r DESC,  #main.COKEY,  hzdept_r, hzdepb_r
        SELECT #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT * COMP_WEIGHTED_AVERAGE AS COMP_WEIGHTED_AVERAGE1
        INTO #last_step
        FROM #comp_temp2
        GROUP BY  #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT, COMP_WEIGHTED_AVERAGE
        SELECT areasymbol, musym, muname,
        #kitchensink.mukey, #last_step.COKEY,
        CAST (SUM (COMP_WEIGHTED_AVERAGE1) over(partition by #kitchensink.mukey) as decimal(5,2))AS """ + aProp + """
        INTO #last_step2
        FROM #last_step
        RIGHT OUTER JOIN #kitchensink ON #kitchensink.mukey=#last_step.mukey
        GROUP BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey, COMP_WEIGHTED_AVERAGE1, #last_step.COKEY
        ORDER BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey
        SELECT #last_step2.areasymbol, #last_step2.musym, #last_step2.muname,
        #last_step2.mukey, #last_step2.""" + aProp + """
        FROM #last_step2
        LEFT OUTER JOIN #last_step ON #last_step.mukey=#last_step2.mukey
        GROUP BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2.""" + aProp + """
        ORDER BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2."""+ aProp
    elif aggMethod == "Min\Max":
        pQry = """SELECT areasymbol, musym, muname, mu.mukey  AS mukey,
        (SELECT TOP 1 """ + mmC + """ (chm1.""" + aProp + """) FROM  component AS cm1
        INNER JOIN chorizon AS chm1 ON cm1.cokey = chm1.cokey AND cm1.cokey = c.cokey
        AND CASE WHEN chm1.hzname LIKE  '%O%' AND hzdept_r <10 THEN 2
        WHEN chm1.hzname LIKE  '%r%' THEN 2
        WHEN chm1.hzname LIKE  '%'  THEN  1 ELSE 1 END = 1
        ) AS """ + aProp + """
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey  AND c.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)"""
    elif aggMethod == "Dominant Component (Numeric)":
        pQry = """SELECT areasymbol, musym, muname, mukey
        INTO #kitchensink
        FROM legend  AS lks
        INNER JOIN  mapunit AS muks ON muks.lkey = lks.lkey AND muks.mukey  IN (""" + keys + """)
        SELECT mu1.mukey, cokey, comppct_r,
        SUM (comppct_r) over(partition by mu1.mukey ) AS SUM_COMP_PCT
        INTO #comp_temp
        FROM legend  AS l1
        INNER JOIN  mapunit AS mu1 ON mu1.lkey = l1.lkey AND mu1.mukey IN (""" + keys + """)
        INNER JOIN  component AS c1 ON c1.mukey = mu1.mukey AND majcompflag = 'Yes'
        AND c1.cokey =
        (SELECT TOP 1 c2.cokey FROM component AS c2
        INNER JOIN mapunit AS mm1 ON c2.mukey=mm1.mukey AND c2.mukey=mu1.mukey ORDER BY c2.comppct_r DESC, c2.cokey)
        SELECT cokey, SUM_COMP_PCT, CASE WHEN comppct_r = SUM_COMP_PCT THEN 1
        ELSE CAST (CAST (comppct_r AS  decimal (5,2)) / CAST (SUM_COMP_PCT AS decimal (5,2)) AS decimal (5,2)) END AS WEIGHTED_COMP_PCT
        INTO #comp_temp3
        FROM #comp_temp
        SELECT areasymbol, musym, muname, mu.mukey/1  AS MUKEY, c.cokey AS COKEY, ch.chkey/1 AS CHKEY, compname, hzname, hzdept_r, hzdepb_r, CASE WHEN hzdept_r < """ + tDep + """ THEN """ + tDep + """ ELSE hzdept_r END AS hzdept_r_ADJ,
        CASE WHEN hzdepb_r > """ + bDep + """  THEN """ + bDep + """ ELSE hzdepb_r END AS hzdepb_r_ADJ,
        CAST (CASE WHEN hzdepb_r > """ + bDep + """  THEN """ + bDep + """ ELSE hzdepb_r END - CASE WHEN hzdept_r <""" + tDep + """ THEN """ + tDep + """ ELSE hzdept_r END AS decimal (5,2)) AS thickness,
        comppct_r,
        CAST (SUM (CASE WHEN hzdepb_r > """ + bDep + """  THEN """ + bDep + """ ELSE hzdepb_r END - CASE WHEN hzdept_r <""" + tDep + """ THEN """ + tDep + """ ELSE hzdept_r END) over(partition by c.cokey) AS decimal (5,2)) AS sum_thickness,
        CAST (ISNULL (""" + aProp + """ , 0) AS decimal (5,2))AS """ + aProp + """
        INTO #main
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey
        INNER JOIN chorizon AS ch ON ch.cokey=c.cokey AND hzname NOT LIKE '%O%'AND hzname NOT LIKE '%r%'
        AND hzdepb_r >""" + tDep + """ AND hzdept_r <""" + bDep + """
        INNER JOIN chtexturegrp AS cht ON ch.chkey=cht.chkey  WHERE cht.rvindicator = 'yes' AND  ch.hzdept_r IS NOT NULL
        AND
        texture NOT LIKE '%PM%' and texture NOT LIKE '%DOM' and texture NOT LIKE '%MPT%' and texture NOT LIKE '%MUCK' and texture NOT LIKE '%PEAT%' and texture NOT LIKE '%br%' and texture NOT LIKE '%wb%'
        ORDER BY areasymbol, musym, muname, mu.mukey, comppct_r DESC, cokey,  hzdept_r, hzdepb_r
        SELECT #main.areasymbol, #main.musym, #main.muname, #main.MUKEY,
        #main.COKEY, #main.CHKEY, #main.compname, hzname, hzdept_r, hzdepb_r, hzdept_r_ADJ, hzdepb_r_ADJ, thickness, sum_thickness, """ + aProp + """ , comppct_r, SUM_COMP_PCT, WEIGHTED_COMP_PCT ,
        SUM((thickness/sum_thickness ) * """ + aProp + """)over(partition by #main.COKEY)AS COMP_WEIGHTED_AVERAGE
        INTO #comp_temp2
        FROM #main
        INNER JOIN #comp_temp3 ON #comp_temp3.cokey=#main.cokey
        ORDER BY #main.areasymbol, #main.musym, #main.muname, #main.MUKEY, comppct_r DESC,  #main.COKEY,  hzdept_r, hzdepb_r
        SELECT #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT * COMP_WEIGHTED_AVERAGE AS COMP_WEIGHTED_AVERAGE1
        INTO #last_step
        FROM #comp_temp2
        GROUP BY  #comp_temp2.MUKEY,#comp_temp2.COKEY, WEIGHTED_COMP_PCT, COMP_WEIGHTED_AVERAGE
        SELECT areasymbol, musym, muname,
        #kitchensink.mukey, #last_step.COKEY,
        CAST (SUM (COMP_WEIGHTED_AVERAGE1) over(partition by #kitchensink.mukey) as decimal(5,2))AS """ + aProp + """
        INTO #last_step2
        FROM #last_step
        RIGHT OUTER JOIN #kitchensink ON #kitchensink.mukey=#last_step.mukey
        GROUP BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey, COMP_WEIGHTED_AVERAGE1, #last_step.COKEY
        ORDER BY #kitchensink.areasymbol, #kitchensink.musym, #kitchensink.muname, #kitchensink.mukey
        SELECT #last_step2.areasymbol, #last_step2.musym, #last_step2.muname,
        #last_step2.mukey, #last_step2.""" + aProp + """
        FROM #last_step2
        LEFT OUTER JOIN #last_step ON #last_step.mukey=#last_step2.mukey
        GROUP BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2.""" + aProp + """
        ORDER BY #last_step2.areasymbol, #last_step2.musym, #last_step2.muname, #last_step2.mukey, #last_step2.""" + aProp
    elif aggMethod == "Dominant Condition":
        pQry = """SELECT areasymbol, musym, muname, mu.mukey/1  AS mukey,
        (SELECT TOP 1 """ + aProp + """
        FROM mapunit
        INNER JOIN component ON component.mukey=mapunit.mukey
        AND mapunit.mukey = mu.mukey
        GROUP BY """ + aProp + """, comppct_r ORDER BY SUM(comppct_r) over(partition by """ + aProp + """) DESC) AS """ + aProp + """
        FROM legend  AS l
        INNER JOIN  mapunit AS mu ON mu.lkey = l.lkey AND  mu.mukey IN (""" + keys + """)
        INNER JOIN  component AS c ON c.mukey = mu.mukey
        AND c.cokey =
        (SELECT TOP 1 c1.cokey FROM component AS c1
        INNER JOIN mapunit ON c.mukey=mapunit.mukey AND c1.mukey=mu.mukey ORDER BY c1.comppct_r DESC, c1.cokey)
        GROUP BY areasymbol, musym, muname, mu.mukey, c.cokey,  compname, comppct_r
        ORDER BY areasymbol, musym, muname, mu.mukey, comppct_r DESC, c.cokey"""
        
    # arcpy.AddMessage(pQry)
    return pQry
            
            
def rslvProps(aProp):
   
    #propDict = {'Wind Erodibility Index': 'wei', 'Wind Erodibility Group' : 'weg', 'Drainage Class' : 'drainagecl', 'Hydrologic Group' : 'hydgrp', 'Corrosion of Steel' : 'corsteel', 'Corrosion of Steel' : 'corsteel', 'Taxonomic Class Name' : 'taxclname', 'Taxonomic Suborder' : 'taxsuborder', 'Taxonomic Order' : 'taxorder', 'Taxonomic Temperature Regime' : 'taxtempregime', 't Factor' : 'tfact', 'Cation Exchange Capcity - Rep Value': 'cec7_r', 'CaCO3 Clay - High': 'claysizedcarb_h', 'Coarse Silt - Low': 'siltco_l', 'Total Rock Fragment Volume - Rep Value': 'fragvoltot_r', 'Water Soluble Phosphate - Rep Value': 'ph2osoluble_r', 'Sum of Bases - High': 'sumbases_h', 'Available Water Capacity - Low': 'awc_l', 'Fine Sand - Low': 'sandfine_l', 'Extractable Acidity - High': 'extracid_h', 'CaCO3 Clay - Rep Value': 'claysizedcarb_r', 'pH Oxidized - Rep Value': 'phoxidized_r', 'Oxalate Aluminum - Low': 'aloxalate_l', 'Coarse Sand - Rep Value': 'sandco_r', 'no. 4 sieve - Low': 'sieveno4_l', 'Bulk Density 15 bar H2O - High': 'dbfifteenbar_h', 'Electrical Conductivity - Rep Value': 'ec_r', 'Calcium Carbonate - Low': 'caco3_l', 'Bulk Density 0.33 bar H2O - Low': 'dbthirdbar_l', 'Rock Fragments 3 - 10 cm- Rep Value': 'frag3to10_r', 'Bray 1 Phosphate - High': 'pbray1_h', 'pH Oxidized - Low': 'phoxidized_l', 'Exchangeable Sodium Percentage - Rep Value': 'esp_r', 'Sodium Adsorption Ratio - Rep Value': 'sar_r', 'no. 4 sieve - High': 'sieveno4_h', 'Medium Sand - Low': 'sandmed_l', 'Bulk Density 15 bar H2O - Rep Value': 'dbfifteenbar_r', 'Effective Cation Exchange Capcity - Rep Value': 'ecec_r', 'pH Oxidized - High': 'phoxidized_h', 'Rock Fragments 3 - 10 cm- High': 'frag3to10_h', 'Oxalate Iron - Low': 'feoxalate_l', 'Free Iron - High': 'freeiron_h', 'Total Rock Fragment Volume - High': 'fragvoltot_h', 'Fine Sand - High': 'sandfine_h', 'Total Sand - High': 'sandtotal_h', 'Liquid Limit - Low': 'll_l', 'Organic Matter - Rep Value': 'om_r', 'Coarse Sand - High': 'sandco_h', 'Very Fine Sand - Low': 'sandvf_l', 'Oxalate Iron - Rep Value': 'feoxalate_r', 'Very Coarse Sand - Low': 'sandvc_l', 'Total Silt - Rep Value': 'silttotal_r', 'Liquid Limit - High': 'll_h', 'Saturated Hydraulic Conductivity - High': 'ksat_h', 'no. 40 sieve - Rep Value': 'sieveno40_r', 'Extract Aluminum - High': 'extral_h', 'no. 40 sieve - High': 'sieveno40_h', 'Kr ': 'krfact', 'Coarse Sand - Low': 'sandco_l', 'Sum of Bases - Rep Value': 'sumbases_r', 'Organic Matter - High': 'om_h', 'no. 10 sieve - Rep Value': 'sieveno10_r', 'Total Silt - High': 'silttotal_h', 'Saturated Hydraulic Conductivity - Low': 'ksat_l', 'Calcium Carbonate - Rep Value': 'caco3_r', 'pH .01M CaCl2 - Rep Value': 'ph01mcacl2_r', 'Bulk Density 15 bar H2O - Low': 'dbfifteenbar_l', 'Sodium Adsorption Ratio - Low': 'sar_l', 'Gypsum - High': 'gypsum_h', 'Rubbed Fiber % - Rep Value': 'fiberrubbedpct_r', 'CaCO3 Clay - Low': 'claysizedcarb_l', 'Electrial Conductivity 1:5 by volume - Rep Value': 'ec15_r', 'Satiated H2O - Rep Value': 'wsatiated_r', 'Medium Sand - High': 'sandmed_h', 'Bulk Density oven dry - Rep Value': 'dbovendry_r', 'Plasticity Index - Low': 'pi_l', 'Extractable Acidity - Rep Value': 'extracid_r', 'Oxalate Aluminum - Rep Value': 'aloxalate_r', 'Medium Sand - Rep Value': 'sandmed_r', 'Total Rock Fragment Volume - Low': 'fragvoltot_l', 'pH 1:1 water - Low': 'ph1to1h2o_l', 'no. 10 sieve - Low': 'sieveno10_l', 'Very Coarse Sand - Rep Value': 'sandvc_r', 'Gypsum - Low': 'gypsum_l', 'Plasticity Index - High': 'pi_h', 'Total Phosphate - High': 'ptotal_h', 'Unrubbed Fiber % - Rep Value': 'fiberunrubbedpct_r', 'Bulk Density 0.1 bar H2O - High': 'dbtenthbar_h', 'Cation Exchange Capcity - Low': 'cec7_l', '0.33 bar H2O - Rep Value': 'wthirdbar_r', '0.1 bar H2O - Low': 'wtenthbar_l', 'Bulk Density 0.1 bar H2O - Rep Value': 'dbtenthbar_r', 'no. 40 sieve - Low': 'sieveno40_l', 'Extract Aluminum - Low': 'extral_l', 'Calcium Carbonate - High': 'caco3_h', 'Water Soluble Phosphate - Low': 'ph2osoluble_l', 'Gypsum - Rep Value': 'gypsum_r', '0.33 bar H2O - High': 'wthirdbar_h', 'Bray 1 Phosphate - Low': 'pbray1_l', 'Available Water Capacity - Rep Value': 'awc_r', 'Rubbed Fiber % - High': 'fiberrubbedpct_h', 'Coarse Silt - Rep Value': 'siltco_r', '0.1 bar H2O - High': 'wtenthbar_h', 'Plasticity Index - Rep Value': 'pi_r', 'Extract Aluminum - Rep Value': 'extral_r', 'Fine Sand - Rep Value': 'sandfine_r', 'Fine Silt - Low': 'siltfine_l', 'Bulk Density oven dry - High': 'dbovendry_h', 'Total Clay - High': 'claytotal_h', 'Fine Silt - High': 'siltfine_h', 'Exchangeable Sodium Percentage - High': 'esp_h', 'Total Clay - Low': 'claytotal_l', 'Bulk Density 0.33 bar H2O - High': 'dbthirdbar_h', 'Total Phosphate - Low': 'ptotal_l', 'Cation Exchange Capcity - High': 'cec7_h', '15 bar H2O - Low': 'wfifteenbar_l', 'no. 10 sieve - High': 'sieveno10_h', 'Extractable Acidity - Low': 'extracid_l', 'Electrical Conductivity - High': 'ec_h', 'Oxalate Phosphate - Low': 'poxalate_l', 'Electrial Conductivity 1:5 by volume - High': 'ec15_h', 'Sodium Adsorption Ratio - High': 'sar_h', 'Liquid Limit - Rep Value': 'll_r', '0.33 bar H2O - Low': 'wthirdbar_l', 'Satiated H2O - High': 'wsatiated_h', 'Bulk Density 0.33 bar H2O - Rep Value': 'dbthirdbar_r', '15 bar H2O - Rep Value': 'wfifteenbar_r', '15 bar H2O - High': 'wfifteenbar_h', 'no. 200 sieve - Low': 'sieveno200_l', 'LEP - Low': 'lep_l', 'Satiated H2O - Low': 'wsatiated_l', 'Total Clay - Rep Value': 'claytotal_r', 'Very Fine Sand - High': 'sandvf_h', 'Available Water Capacity - High': 'awc_h', 'Total Phosphate - Rep Value': 'ptotal_r', 'Electrical Conductivity - Low': 'ec_l', 'Oxalate Aluminum - High': 'aloxalate_h', 'Effective Cation Exchange Capcity - High': 'ecec_h', 'Rubbed Fiber % - Low': 'fiberrubbedpct_l', 'Coarse Silt - High': 'siltco_h', 'Bulk Density oven dry - Low': 'dbovendry_l', 'no. 4 sieve - Rep Value': 'sieveno4_r', 'Bray 1 Phosphate - Rep Value': 'pbray1_r', 'no. 200 sieve - High': 'sieveno200_h', '0.1 bar H2O - Rep Value': 'wtenthbar_r', 'Unrubbed Fiber % - Low': 'fiberunrubbedpct_l', 'pH .01M CaCl2 - Low': 'ph01mcacl2_l', 'Saturated Hydraulic Conductivity - Rep Value': 'ksat_r', 'Kw ': 'kwfact', 'Unrubbed Fiber % - High': 'fiberunrubbedpct_h', 'Rock Fragments > 10 cm - High': 'fraggt10_h', 'Kf': 'kffact', 'no. 200 sieve - Rep Value': 'sieveno200_r', 'pH .01M CaCl2 - High': 'ph01mcacl2_h', 'Oxalate Phosphate - Rep Value': 'poxalate_r', 'Rock Fragments 3 - 10 cm- Low': 'frag3to10_l', 'Water Soluble Phosphate - High': 'ph2osoluble_h', 'Very Fine Sand - Rep Value': 'sandvf_r', 'Electrial Conductivity 1:5 by volume - Low': 'ec15_l', 'Total Silt - Low': 'silttotal_l', 'Total Sand - Low': 'sandtotal_l', 'Organic Matter - Low': 'om_l', 'Fine Silt - Rep Value': 'siltfine_r', 'Very Coarse Sand - High': 'sandvc_h', 'Free Iron - Low': 'freeiron_l', 'Rock Fragments > 10 cm - Rep Value': 'fraggt10_r', 'LEP - High': 'lep_h', 'pH 1:1 water - High': 'ph1to1h2o_h', 'Oxalate Phosphate - High': 'poxalate_h', 'Total Sand - Rep Value': 'sandtotal_r', 'Oxalate Iron - High': 'feoxalate_h', 'Rock Fragments > 10 cm - Low': 'fraggt10_l', 'Sum of Bases - Low': 'sumbases_l', 'Free Iron - Rep Value': 'freeiron_r', 'LEP - Rep Value': 'lep_r', 'Effective Cation Exchange Capcity - Low': 'ecec_l', 'pH 1:1 water - Rep Value': 'ph1to1h2o_r', 'Exchangeable Sodium Percentage - Low': 'esp_l', 'Ki ': 'kifact', 'Bulk Density 0.1 bar H2O - Low': 'dbtenthbar_l'}
    propDict = {'0.1 bar H2O - Rep Value' : 'wtenthbar_r', '0.33 bar H2O - Rep Value' : 'wthirdbar_r', '15 bar H2O - Rep Value' : 'wfifteenbar_r', 'Available Water Capacity - Rep Value' : 'awc_r', 'Bray 1 Phosphate - Rep Value' : 'pbray1_r', 'Bulk Density 0.1 bar H2O - Rep Value' : 'dbtenthbar_r', 'Bulk Density 0.33 bar H2O - Rep Value' : 'dbthirdbar_r', 'Bulk Density 15 bar H2O - Rep Value' : 'dbfifteenbar_r', 'Bulk Density oven dry - Rep Value' : 'dbovendry_r', 'CaCO3 Clay - Rep Value' : 'claysizedcarb_r', 'Calcium Carbonate - Rep Value' : 'caco3_r', 'Cation Exchange Capcity - Rep Value' : 'cec7_r', 'Coarse Sand - Rep Value' : 'sandco_r', 'Coarse Silt - Rep Value' : 'siltco_r', 'Corrosion of Steel' : 'corsteel', 'Corrosion of Concrete' : 'corcon', 'Drainage Class' : 'drainagecl', 'Effective Cation Exchange Capcity - Rep Value' : 'ecec_r', 'Electrial Conductivity 1:5 by volume - Rep Value' : 'ec15_r', 'Electrical Conductivity - Rep Value' : 'ec_r', 'Exchangeable Sodium Percentage - Rep Value' : 'esp_r', 'Extract Aluminum - Rep Value' : 'extral_r', 'Extractable Acidity - Rep Value' : 'extracid_r', 'Fine Sand - Rep Value' : 'sandfine_r', 'Fine Silt - Rep Value' : 'siltfine_r', 'Free Iron - Rep Value' : 'freeiron_r', 'Gypsum - Rep Value' : 'gypsum_r', 'Hydrologic Group' : 'hydgrp', 'Kf' : 'kffact', 'Ki ' : 'kifact', 'Kr ' : 'krfact', 'Kw ' : 'kwfact', 'LEP - Rep Value' : 'lep_r', 'Liquid Limit - Rep Value' : 'll_r', 'Medium Sand - Rep Value' : 'sandmed_r', 'Organic Matter - Rep Value' : 'om_r', 'Oxalate Aluminum - Rep Value' : 'aloxalate_r', 'Oxalate Iron - Rep Value' : 'feoxalate_r', 'Oxalate Phosphate - Rep Value' : 'poxalate_r', 'Plasticity Index - Rep Value' : 'pi_r', 'Rock Fragments 3 - 10 cm - Rep Value' : 'frag3to10_r', 'Rock Fragments > 10 cm - Rep Value' : 'fraggt10_r', 'Rubbed Fiber % - Rep Value' : 'fiberrubbedpct_r', 'Satiated H2O - Rep Value' : 'wsatiated_r', 'Saturated Hydraulic Conductivity - Rep Value' : 'ksat_r', 'Sodium Adsorption Ratio - Rep Value' : 'sar_r', 'Sum of Bases - Rep Value' : 'sumbases_r', 'Taxonomic Class Name' : 'taxclname', 'Taxonomic Order' : 'taxorder', 'Taxonomic Suborder' : 'taxsuborder', 'Taxonomic Temperature Regime' : 'taxtempregime', 'Total Clay - Rep Value' : 'claytotal_r', 'Total Phosphate - Rep Value' : 'ptotal_r', 'Total Rock Fragment Volume - Rep Value' : 'fragvoltot_r', 'Total Sand - Rep Value' : 'sandtotal_r', 'Total Silt - Rep Value' : 'silttotal_r', 'Unrubbed Fiber % - Rep Value' : 'fiberunrubbedpct_r', 'Very Coarse Sand - Rep Value' : 'sandvc_r', 'Very Fine Sand - Rep Value' : 'sandvf_r', 'Water Soluble Phosphate - Rep Value' : 'ph2osoluble_r', 'Wind Erodibility Group' : 'weg', 'Wind Erodibility Index' : 'wei', 'no. 10 sieve - Rep Value' : 'sieveno10_r', 'no. 200 sieve - Rep Value' : 'sieveno200_r', 'no. 4 sieve - Rep Value' : 'sieveno4_r', 'no. 40 sieve - Rep Value' : 'sieveno40_r', 'pH .01M CaCl2 - Rep Value' : 'ph01mcacl2_r', 'pH 1:1 water - Rep Value' : 'ph1to1h2o_r', 'pH Oxidized - Rep Value' : 'phoxidized_r', 't Factor' : 'tfact'}
    propVal = propDict.get(aProp)

    # arcpy.AddMessage(propVal)
    return propVal


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
        msg = 'Did not create table for ' + prop
        return False, msg
    
    
def updateTable(spatial, tabular):

    # add the standard fields if they aren't in the spatial table 
    housefields = ['areasymbol', 'musym', 'muname']
    havefields = [str(x.name) for x in arcpy.Describe(spatial).fields]
    needflds = [f for f in housefields if not f in havefields]
    
    fobjs = [f for f in arcpy.Describe(tabular).fields if str(f.name) in needflds]
    if len(fobjs) > 0:
        for f in fobjs:
            fname = f.name 
            ftype = f.type
            flen = f.length
        
            arcpy.management.AddField(spatial, fname, ftype, None, None, flen)
        
            hDict = dict()
            with arcpy.da.SearchCursor(tabular, ["mukey", fname]) as rows:
                for row in rows:
                    hDict[str(row[0])] = row[1]
            
            
            with arcpy.da.UpdateCursor(spatial, ["mukey", fname]) as rows:
                for row in rows:
                    val = hDict.get(row[0])
                    row[1] = val
                    rows.updateRow(row)
        
    
    # we will always need the last field(property) 
    field = arcpy.Describe(tabular).fields[-1:]
    fname = field[0].name 
    ftype = field[0].type
    flen = field[0].length
    
    arcpy.management.AddField(spatial, fname, ftype, None, None, flen)
    
    pDict = dict()
    with arcpy.da.SearchCursor(tabular, ["mukey", fname]) as rows:
        for row in rows:
            pDict[str(row[0])] = row[1]
    
    with arcpy.da.UpdateCursor(spatial, ["mukey", fname]) as rows:
        for row in rows:
            val = pDict.get(row[0])
            row[1] = val
            rows.updateRow(row)
        
    
   
# ========================= Prepare parameters =========================

import sys, os, arcpy, string, random, traceback

rid = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))

arcpy.env.overwriteOutput = True
arcpy.env.addOutputsToMap = True

clu_in = arcpy.GetParameter(0)
aggMethod = arcpy.GetParameterAsText(1)
arcpy.AddMessage(aggMethod)
props = arcpy.GetParameter(2)
tDep = arcpy.GetParameterAsText(3)
bDep = arcpy.GetParameterAsText(4)
mmC = arcpy.GetParameterAsText(5)
dest = arcpy.GetParameterAsText(6)
addToGeom = arcpy.GetParameterAsText(7)
# bLyrs = arcpy.GetParameterAsText(7)
# bSingle = arcpy.GetParameterAsText(8)

arcpy.env.workspace = dest

fail = list()

jobs = 1 + (len(props))
n = 1
arcpy.SetProgressor("default", "SSURGO On-Demand: Jobs(" + str(n) + " of " + str(jobs) + ")")

try:

    # ========================= Get the SSURGO Geometry =========================
    arcpy.AddMessage(u"\u200B")
    arcpy.AddMessage('Collecting SSURGO geometry...')
    arcpy.AddMessage(u"\u200B")
    desc = arcpy.Describe(clu_in)
    sel = desc.FIDSet
    if sel  == '':
        err = "Select at least 1 feature"
        # arcpy.AddMessage(err)
        raise RuntimeError(err)
    
    # get coordinate system
    sr = desc.spatialReference
    
    pcsCode = sr.PCSCode
    # projected coordinate systems need to be reprojected bc we need lat/long 
    if pcsCode != 0:
        code = sr.GCS.GCSCode
    
        if code == 4326:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326))
            hull_target = os.path.join(dest, "clu_prj_" + rid)
    
        # NAD83 and NAD83(2011), needs transformation 
        elif code == 4269:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            trm = "WGS_1984_(ITRF00)_To_NAD_1983"
            arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326), trm)
            hull_target =  os.path.join(dest, "clu_prj_" + rid)
            
        elif code == 6318:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            trm = "WGS_1984_(ITRF08)_To_NAD_1983_2011"
            arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326), trm)
            hull_target =  os.path.join(dest, "clu_prj_" + rid)
        
        else:
            err = 'This tool only supports spatial reference objects based on WGS84, NAD83, NAD83(2011)'
            raise TypeError(err)
    
    else:
        code = sr.GCS.GCSCode
        
        # no transformation needed
        if code == 4326:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            # arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326))
            hull_target = os.path.join(dest, "clu_sel_" + rid)
    
        # NAD83 and NAD83(2011), needs transformation 
        elif code == 4269:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            trm = "WGS_1984_(ITRF00)_To_NAD_1983"
            arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326), trm)
            hull_target =  os.path.join(dest, "clu_prj_" + rid)
            
        elif code == 6318:
            arcpy.management.CopyFeatures(clu_in, os.path.join(dest, "clu_sel_" + rid))
            trm = "WGS_1984_(ITRF08)_To_NAD_1983_2011"
            arcpy.management.Project(os.path.join(dest, "clu_sel_" + rid), os.path.join(dest, "clu_prj_" + rid), arcpy.SpatialReference(4326), trm)
            hull_target =  os.path.join(dest, "clu_prj_" + rid)
        
        else:
            err = 'This tool only supports spatial reference objects based on WGS84, NAD83, NAD83(2011)'
            raise TypeError(err)
    
    # dissolve feature(s) into 1 part
    # get the smallest feature possible and its wkt
    arcpy.management.Dissolve(hull_target, os.path.join(dest, "sod_sngl_prt_" + rid))
    with arcpy.da.SearchCursor(os.path.join(dest, "sod_sngl_prt_" + rid), "SHAPE@") as rows:
        for row in rows:
            hull = row[0].convexHull()
            wkt = hull.WKT
            # arcpy.AddMessage(wkt)
            
        if not wkt.startswith("MULTIPOLYGON ("):
            for f in arcpy.ListFeatureClasses("*_" + rid):
                arcpy.management.Delete(f)
            msg = 'The input AOI appears to have extended attributes (Z,M).  This is not supported.'
            raise TypeError(msg)
        # arcpy.AddMessage(wkt)
    
    geoQ = """~DeclareGeometry(@aoi)~
    select @aoi = geometry::STGeomFromText(
      '""" + wkt + """', 4326)
    
    ~DeclareIdGeomTable(@intersectedPolygonGeometries)~
    ~GetClippedMapunits(@aoi,polygon,geo,@intersectedPolygonGeometries)~
    
    select id, geom
    from @intersectedPolygonGeometries"""
    
    # arcpy.AddMessage(geoQ)
    gBool, geoResults, gType = sdaCall(geoQ, var='geo')
    
    if gBool:
        
        if len(geoResults) == 0:
        
            for f in arcpy.ListFeatureClasses("*_" + rid):
                arcpy.management.Delete(f)
            
            err = 'Received no SDA geometry to process. Exiting'
            raise ValueError(err)
    
        else:
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
                
            arcpy.analysis.Clip(os.path.join(dest, "sod_temp_" + rid), os.path.join(dest, "sod_sngl_prt_" + rid),  os.path.join(dest, "SSURGO_On_Demand_property"))
            
            # clean up temporary files
            for f in arcpy.ListFeatureClasses("*_" + rid):
                arcpy.management.Delete(f)
                
            
            # ========================= Get the mukeys of the returned geometry =========================
            
            with arcpy.da.SearchCursor(os.path.join(dest, "SSURGO_On_Demand_property"), "mukey") as rows:
                geoKeys = sorted({row[0] for row in rows})
                
            keys = ",".join(map("'{0}'".format, geoKeys))
            
            # ========================= Get the mukeys of the returned geometry =========================
            
            # this dictionary is used for naming tables appropriately
            aggAbbr = dict()
            aggAbbr['Dominant Condition'] = 'dom_cond'
            aggAbbr['Dominant Component (Category)'] = 'dom_comp_cat'
            aggAbbr['Dominant Component (Numeric)'] = 'dom_comp_num'
            aggAbbr['Weighted Average'] = 'wtavg'
            aggAbbr['Min\Max'] = mmC
            
            
            for prop in props:
                
                arcpy.AddMessage('Running property: ' + prop )
                
                n += 1
                arcpy.SetProgressorLabel("SSURGO On-Demand: Jobs (" + str(n) + " of " + str(jobs) + ")")
                
                sdaCol = rslvProps(prop)
                theQ = tabRequest(sdaCol)
                # arcpy.AddMessage(theQ)
                
                tabBool, tabData, tabType = sdaCall(theQ, var='tab')
                
                if tabBool:
                    
                    if len(tabData) == 0:
                        fail.append(prop)
                        arcpy.AddMessage('No property information returned for ') + prop
                    
                    else:
                        tabCols = tabData.get('Table')[0]
                        tabMeta = tabData.get('Table')[1]
                        tabData = tabData.get('Table')[2:]        
                            
                        tblinfo = (aggAbbr.get(aggMethod), sdaCol, tDep, bDep)
                    
                        newName = "sod_" +  "_".join(map("{0}".format, tblinfo))
                        newName = newName.replace("__", "_")
                        if newName.endswith("_"):
                            newName = newName[:-1]
                        
                        # arcpy.AddMessage(newName)
                        
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
                            
                            sod_geom = os.path.join(dest, "SSURGO_On_Demand_property")
                            sod_tab = os.path.join(dest, newName)
                            
                            updateTable(sod_geom, sod_tab)
                            
                        # add empty line to separate any messages
                        arcpy.AddMessage(u"\u200B")
                        
                else:
                    fail.append(prop)
                    arcpy.AddMessage('Error while collecting information returned for ' + prop)
                    arcpy.AddMessage(tabData)
    else:
    
        if gType == 'geo':
            
            arcpy.AddMessage(geoResults)
            
            # clean up temporary files
            for f in arcpy.ListFeatureClasses("*_" + rid):
                arcpy.management.Delete(f)
            
            err = 'Unable to collect SDA geometry to process. Exiting'
            raise ValueError(err)
        
                
    
    if len(fail) > 0:
        
        fstr = ','.join(map("{0}".format, fail))
        arcpy.AddError('The following property(s) failed to execute or returned no results:')
        arcpy.AddError(fstr)
        arcpy.AddMessage(u"\u200B")
        
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
        
        
        
        
        
        
        
    
    
    
    

    
    
