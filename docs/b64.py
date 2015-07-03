#!/bin/python2
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 11:23:51 2014

@author: fernandezjm
"""
import re
import argparse
from urllib2 import urlopen





def to_base64(input_img):
   """
   Converts a plot into base64-encoded graph
   """
   extension = input_img.split('.')[-1]
   if 1 > len(extension) > 4:
      print 'Could not determine file extension for %s' % input_img

   if '://' in input_img:
      content = urlopen(input_img)
   else:
      content = open(input_img, 'rb')

   text = 'data:image/{0};base64, {1}'.format(extension, \
                                              content.read().encode("base64"))
   content.close()
   return text


def reencode_html(input_html, **kwargs):
   """
   Converts all images from an html file to base64
   """
   output_file = kwargs.pop('output_file', 'HC_%s' % input_html)
   with open(input_html, 'r') as content, open(output_file, 'w') as output:
      my_regex = re.compile('.+(<img.+src=\")(.+?)"', re.IGNORECASE)
      for line in content:
         try:
            item = re.finditer(my_regex, line).next().group(2)
            output.write(line.replace(item, to_base64(item)))
         except StopIteration:
            output.write(line)



if __name__ == "__main__":
   PARSER = \
   argparse.ArgumentParser(description='b64 encoder for images in html files',
                           formatter_class=argparse.RawTextHelpFormatter)
   PARSER.add_argument('input_file', metavar='input-html', type=str,
                       help='html input file (local)')
   PARSER.add_argument('--output', metavar='output-html', type=str,
                       help='html output file (local)')
   ARGS = PARSER.parse_args()

   if ARGS.output:
      reencode_html(ARGS.input_file, output_file=ARGS.output)
   else:
      reencode_html(ARGS.input_file)
