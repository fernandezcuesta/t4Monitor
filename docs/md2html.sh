#!/bin/zsh
BASEDIR=$(dirname $0)
echo -ne 'Converting markdown to html\r'
~/.cabal/bin/pandoc -Ss --toc --toc-depth 4 --template $BASEDIR/template.html $BASEDIR/SMSC_Monitor.md -H pandoc.css -o $BASEDIR/_SMSC_Monitor.html
~/.cabal/bin/pandoc -Ss --toc --toc-depth 3 --template $BASEDIR/template.html $BASEDIR/log_howto.md -H pandoc.css -o $BASEDIR/log_howto.html
echo -ne '\rConverting images to base64'
python2 $BASEDIR/b64.py $BASEDIR/_SMSC_Monitor.html --output $BASEDIR/Monitoring_procedure_TLF_2014.html
rm $BASEDIR/_SMSC_Monitor.html
echo -ne '\rDone!                       \n'
