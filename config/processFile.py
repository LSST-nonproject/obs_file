import os

root.calibrate.doAstrometry = os.path.split(os.environ.get("ASTROMETRY_NET_DATA_DIR", "/None"))[1] != "None"
if not root.calibrate.doAstrometry:
    root.calibrate.doPhotoCal = False

