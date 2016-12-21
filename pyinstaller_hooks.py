#!/usr/bin/env python2
# -*- coding: utf-8 -*-
# Hooks required by pyinstaller

from __future__ import print_function

import encodings.idna
import multiprocessing
import tkinter

import t4mon

if __name__ == "__main__":
    multiprocessing.freeze_support()
    t4mon.main()
