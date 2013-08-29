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


"""This module provides a re-write of the lsst.pipe.base.ArgumentParser
suitable for use with the FileMapper, so that a single file can be
processed.  It duplicates a lot of code from the ArgumentParser, which
is not good, and suggests that the ArgumentParser needs to be refactored.
However, in the mean time, we have to live with the problem that changes
in the ArgumentParser may break this FileArgumentParser.
"""


import argparse
import getpass
import os
import shutil
import sys

import lsst.daf.persistence as dafPersist
import lsst.pex.logging as pexLog
from lsst.pipe.base.argumentParser import (ArgumentParser, _fixPath, DEFAULT_INPUT_NAME, DEFAULT_CALIB_NAME,
                                           DEFAULT_OUTPUT_NAME)
from .fileMapper import FileMapper


class FileArgumentParser(ArgumentParser):
    def parse_args(self, config, args=None, log=None, override=None):
        """Parse arguments for a pipeline task

        @param config: config for the task being run
        @param args: argument list; if None use sys.argv[1:]
        @param log: log (instance pex_logging Log); if None use the default log
        @param override: a config override callable, to be applied after camera-specific overrides
            files but before any command-line config overrides.  It should take the root config
            object as its only argument.

        @return namespace: a struct containing many useful fields including:
        - camera: camera name
        - config: the supplied config with all overrides applied, validated and frozen
        - butler: a butler for the data
        - datasetType: dataset type
        - dataIdList: a list of data ID dicts
        - dataRefList: a list of butler data references; each data reference is guaranteed to contain
            data for the specified datasetType (though perhaps at a lower level than the specified level,
            and if so, valid data may not exist for all valid sub-dataIDs)
        - log: a pex_logging log
        - an entry for each command-line argument, with the following exceptions:
          - config is Config, not an override
          - configfile, id, logdest, loglevel are all missing
        - obsPkg: name of obs_ package for this camera
        """
        if args == None:
            args = sys.argv[1:]

        if len(args) < 1 or args[0].startswith("-") or args[0].startswith("@"):
            self.print_help()
            self.exit("%s: error: Must specify input as first argument" % self.prog)

        # note: don't set namespace.input until after running parse_args, else it will get overwritten
        inputRoot = _fixPath(DEFAULT_INPUT_NAME, args[0])
        if not os.path.exists(inputRoot):
            self.error("Error: input=%r not found" % (inputRoot,))
        if not os.path.isdir(inputRoot):
            inputRoot, fileName = os.path.split(inputRoot)
            args[0:1] = [inputRoot, "--id", "calexp=%s" % fileName]
        if not os.path.isdir(inputRoot):
            self.error("Error: input=%r is not a directory" % (inputRoot,))
        
        namespace = argparse.Namespace()
        namespace.input = _fixPath(DEFAULT_INPUT_NAME, args[0])
        if not os.path.isdir(namespace.input):
            self.error("Error: input=%r not found" % (namespace.input,))
        namespace.config = config
        namespace.log = log if log is not None else pexLog.Log.getDefaultLog()
        namespace.dataIdList = []
        namespace.datasetType = "calexp"

        self.handleCamera(namespace)

        namespace.obsPkg = "obs_file"   # used to find initial overrides
        namespace.camera = ""
        self._applyInitialOverrides(namespace)
        if override is not None:
            override(namespace.config)

        # Add data ID containers to namespace
        for dataIdArgument in self._dataIdArgDict.itervalues():
            setattr(namespace, dataIdArgument.name, dataIdArgument.ContainerClass(level=dataIdArgument.level))

        namespace = argparse.ArgumentParser.parse_args(self, args=args, namespace=namespace)
        del namespace.configfile

        namespace.calib = _fixPath(DEFAULT_CALIB_NAME,  namespace.calib)
        namespace.output = _fixPath(DEFAULT_OUTPUT_NAME, namespace.output)

        if namespace.clobberOutput:
            if namespace.output is None:
                self.error("--clobber-output is only valid with --output")
            elif namespace.output == namespace.input:
                self.error("--clobber-output is not valid when the output and input repos are the same")
            if os.path.exists(namespace.output):
                namespace.log.info("Removing output repo %s for --clobber-output" % namespace.output)
                shutil.rmtree(namespace.output)

        namespace.log.info("input=%s"  % (namespace.input,))
        if namespace.calib:
            namespace.log.info("calib=%s"  % (namespace.calib,))
        namespace.log.info("output=%s" % (namespace.output,))

        if "config" in namespace.show:
            namespace.config.saveToStream(sys.stdout, "config")

        mapper = FileMapper(root=namespace.input,
                            calibRoot=namespace.calib, outputRoot=namespace.output)
        namespace.butler = dafPersist.Butler(root=namespace.input, mapper=mapper)

        # convert data in each of the identifier lists to proper types
        # this is done after constructing the butler, hence after parsing the command line,
        # because it takes a long time to construct a butler
        self._processDataIds(namespace)
        if "data" in namespace.show:
            for dataRef in namespace.dataRefList:
                print "dataRef.dataId =", dataRef.dataId

        if "exit" in namespace.show:
            sys.exit(0)

        if namespace.debug:
            try:
                import debug
            except ImportError:
                sys.stderr.write("Warning: no 'debug' module found\n")
                namespace.debug = False

        if namespace.logdest:
            namespace.log.addDestination(namespace.logdest)
        del namespace.logdest

        if namespace.loglevel:
            permitted = ('DEBUG', 'INFO', 'WARN', 'FATAL')
            if namespace.loglevel.upper() in permitted:
                value = getattr(pexLog.Log, namespace.loglevel.upper())
            else:
                try:
                    value = int(namespace.loglevel)
                except ValueError:
                    self.error("log-level=%s not int or one of %s" % (namespace.loglevel, permitted))
            namespace.log.setThreshold(value)
        del namespace.loglevel

        namespace.config.validate()
        namespace.config.freeze()

        return namespace

    def _processDataIds(self, namespace):
        """See lsst.pipe.tasks.ArgumentParser._processDataIds"""

        # Strip the .fits (or .fit or .fts) extension from the name of the calexp
        # as we'll be using the basename to specify the output directory
        for d in namespace.id.idList:
            calexp = d.get("calexp", None)
            if calexp:
                b, e = os.path.splitext(calexp)
                if e in (".fits", ".fit", ".fts"):
                    d["calexp"] = b

        return super(FileArgumentParser, self)._processDataIds(namespace)
