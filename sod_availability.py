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

def availability(form=None, dest=None):

    """Download SSURGO Soil Survey Availability from Web Soil Survey.
    :param form str: select format, vaild types are 'pdf', 'jpg', 'shp'(shapefile). If None 'shp' will be downloaded
    :param dest str: destination directory.  If None specified download location is your current working directory
    :return: file object on disk"""

    import os, requests

    if form == 'pdf':
        pdf = 'https://websoilsurvey.sc.egov.usda.gov/DataAvailability/SoilDataAvailabilityMap.pdf'
        name = os.path.basename(pdf)
        r = requests.get(pdf)
    elif form == 'jpg':
        jpg = 'https://websoilsurvey.sc.egov.usda.gov/DataAvailability/SoilDataAvailabilityMap.jpg'
        name = os.path.basename(jpg)
        r = requests.get(jpg)
    elif form == 'shp' or form is None:
        shp = 'https://websoilsurvey.sc.egov.usda.gov/DataAvailability/SoilDataAvailabilityShapefile.zip'
        name = os.path.basename(shp)
        r = requests.get(shp)
    else:
        err = 'Unsupported format requested'
        raise RuntimeError(err)

    if dest is None:
        dest = os.getcwd()
    else:
        dest=dest

    try:
        open(os.path.join(dest,name), 'wb').write(r.content)

        return True, 'The requested SSURGO availability file ' + name + ' has been saved to ' + dest

    except Exception as e:
        return False, e

import arcpy

form = arcpy.GetParameterAsText(0)
dest = arcpy.GetParameterAsText(1)

avbool, avmsg = availability(form, dest)

if avbool:
    arcpy.AddMessage(avmsg)
else:
    arcpy.AddError(avmsg)
