#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import numpy
import os
from lsst.pipe.tasks.processImage import ProcessImageTask
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.afw.table as afwTable
from lsst.pex.config import Field
import lsst.pex.exceptions as pexExcept
import lsst.pipe.base as pipeBase

from .argumentParser import FileArgumentParser

class ProcessFileConfig(ProcessImageTask.ConfigClass):
    """Config for ProcessFile"""
    doCalibrate=Field(dtype=bool, default=True, doc="Perform calibration?")
    doVariance=Field(dtype=bool, default=False, doc="Calculate variance?")
    doMask=Field(dtype=bool, default=False, doc="Calculate mask?")
    gain=Field(dtype=float, default=0.0, doc="Gain (e/ADU) for image")
    noise=Field(dtype=float, default=1.0, doc="Noise (ADU) in image")
    saturation=Field(dtype=float, default=65535, doc="Saturation limit")
    low=Field(dtype=float, default=0.0, doc="Low limit")
    isBackgroundSubtracted=Field(dtype=bool, default=False, doc="Input image is already background subtracted")
    
class ProcessFileTask(ProcessImageTask):
    """Process a CCD
    
    Available steps include:
    - calibrate
    - detect sources
    - measure sources
    """
    ConfigClass = ProcessFileConfig
    _DefaultName = "processFile"

    def __init__(self, **kwargs):
        ProcessImageTask.__init__(self, **kwargs)

    def makeIdFactory(self, sensorRef):
        expBits = sensorRef.get("ccdExposureId_bits")
        expId = long(sensorRef.get("ccdExposureId"))
        return afwTable.IdFactory.makeSource(expId, 64 - expBits)        

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser
        """
        parser = FileArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id", datasetType="calexp",
                               help="data ID, e.g. --id calexp=XXX")

        return parser

    @pipeBase.timeMethod
    def run(self, sensorRef):
        """Process one CCD
        
        @param sensorRef: sensor-level butler data reference
        @return pipe_base Struct containing these fields:
        - exposure: calibrated exposure (calexp): as computed if config.doCalibrate,
            else as upersisted and updated if config.doDetection, else None
        - calib: object returned by calibration process if config.doCalibrate, else None
        - sources: detected source if config.doPhotometry, else None
        """
        self.log.info("Processing %s" % (sensorRef.dataId))

        # initialize outputs
        #
        # Be careful.  If we've already run processFile.py, then there's a copy of the
        # input file written into the output directory, and the butler will find it before
        # it finds the original.  We also need to be read straight Fits images with no
        # mask planes or Wcs
        #
        inputFile = sensorRef.get("calexp_filename")[0]
        fileDir, fileName = os.path.split(inputFile)
        originalFile = os.path.join(fileDir, "_parent", fileName)
        if os.path.exists(originalFile):
            inputFile = originalFile

        try:
            postIsrExposure = afwImage.ExposureF(inputFile)
        except pexExcept.LsstCppException, e:
            etype = e.args[0].getType()
            if etype == "lsst::afw::fits::FitsError *":
                import lsst.daf.base as dafBase
                md = dafBase.PropertyList()
                mi = afwImage.MaskedImageF(inputFile, md)

                postIsrExposure = afwImage.makeExposure(mi)

                wcs = afwImage.makeWcs(md)
                if wcs:
                    postIsrExposure.setWcs(wcs)
                else:
                    self.log.warn("No WCS found in %s; caveat emptor" % (sensorRef.dataId))
            else:
                raise

        postIsrExposure.getMaskedImage().getMask()[:] &= \
            afwImage.MaskU.getPlaneBitMask(["SAT", "INTRP", "BAD", "EDGE"])
        if self.config.doVariance:
            self.setVariance(postIsrExposure)
        if self.config.doMask:
            self.setMask(postIsrExposure)
        
        # delegate the work to ProcessImageTask
        result = self.process(sensorRef, postIsrExposure)
        return result

    def setVariance(self, exposure):
        mi = exposure.getMaskedImage()
        image = mi.getImage().getArray()
        variance = mi.getVariance().getArray()
        if self.config.isBackgroundSubtracted:
            bkgdVariance = afwMath.makeStatistics(mi.getImage(), afwMath.VARIANCECLIP).getValue()
            self.log.info("Setting variance: background variance = %g ADU" % (bkgdVariance))
        else:
            self.log.info("Setting variance: noise=%g ADU" % (self.config.noise))
            bkgdVariance = self.config.noise**2

        variance[:] = bkgdVariance

        if self.config.gain > 0.0:
            self.log.info("Setting variance: gain=%g e/ADU" % (self.config.gain))
            variance[:] += image/self.config.gain

    def setMask(self, exposure):
        mi = exposure.getMaskedImage()
        image = mi.getImage().getArray()
        mask = mi.getMask().getArray()
        isLow = image < self.config.low
        isSat = image > self.config.saturation
        self.log.info("Masking %d low and %d saturated pixels" % (isLow.sum(), isSat.sum()))
        mask += numpy.where(isLow, afwImage.MaskU.getPlaneBitMask("BAD"), 0)
        mask += numpy.where(isSat, afwImage.MaskU.getPlaneBitMask("SAT"), 0)

