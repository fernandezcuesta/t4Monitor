#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import multiprocessing

import t4mon

if __name__ == "__main__":
    multiprocessing.freeze_support() 
    t4mon.main()