#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
 pySMSCMon: T4-compliant CSV processor and visualizer for Acision SMSC Monitor
 ------------------------------------------------------------------------------
 2014-2015 (c) J.M. Fernández - fernandez.cuesta@gmail.com

 Syntax:

 t4 input_file

 CSV file header may come in 2 different formats:

  ** Format 1: **
  The first four lines are header data:

  line0: Header information containing T4 revision info and system information.

  line1: Collection date  (optional line)

  line2: Start time       (optional line)

  line3: Parameter Headings (comma separated).

 or

  ** Format 2: **

  line0: Header information containing T4 revision info and system information.

  line1: <delim> START COLUMN HEADERS  <delim>  where <delim> is a triple $

  line2: parameter headings (comma separated)
   ...

  line 'n': <delim> END COLUMN HEADERS  <delim>  where <delim> is a triple $

  The remaining lines are the comma separated values. The first column is the
  sample time. Each line represents a sample, typically 60 seconds apart.
  However T4 incorrectly places an extra raw line with the column averages
  almost at the end of the file. That line will be considered as a closing hash
  and contents followed by it (sometimes even more samples...) is ignored.

"""
__all__ = ['pysmscmon', 'calculations']
