#Log output guidelines

By default, the log output for each report contains the output of the following commands:

```

search smsc$root:[log]smsc.log "-e-","-w-","-f-","license" /win=(3,2)
search DSA0:[SYS%.SYSMGR]OPERATOR.LOG "-e-","-w-","-f-" /win=(3,3)
search DSA0:[SYS0.UCX_SYSLOGD]SYSLOGD.LOG "DEC 23" /win=(1)

@CMG$TOOLS:smsc_check_entities
show queue /batch/all
show cluster

mon cluster/end="+00:00:30"/summ=sys$scratch:mon.sum/nodisplay
type sys$scratch:mon.sum
purge sys$scratch:mon.sum

dir billing_backup /since="-00:15:00"


```
## smsc_check_entities should not report anything

The output of `smsc_check_entities` should always be empty, meaning that all entities are up and running in their corresponding nodes.

##Common errors under normal operation

- `%SMH-W-NO_SIW_SESS, No SIW or no session found to deliver a message, SIW type UN
KNOWN PID: 24`

    This is a known error and may be ignored.

- `CHK_SYS, %DEVICE-E-ERR, Event 1539: Machine Check 620 - System CE`

    This error is shown in SMTFC due to a broken fan in one of the MSA1000 disk enclosures.

##- Check that batch jobs are running

The batch queues should have the following jobs running:

```

  Entry  Jobname         Username             Status
  -----  -------         --------             ------
    745  FTP_2           SMSC                 Holding until 23-DEC-2014 10:04:43

    759  ESTADISTICAS_TRAFICO
                         CONMUT               Holding until 23-DEC-2014 10:04:46

    760  CDR_FTP         SMSC                 Holding until 23-DEC-2014 10:05:18

    761  BILLING_PP      SMSC                 Holding until 23-DEC-2014 10:05:27

    754  MON_PML         SMSC                 Holding until 23-DEC-2014 10:07:24

    691  RESUMEN_TRAFICO CONMUT               Holding until 24-DEC-2014 00:00:00

    710  CMG_CLEANUP     SYSTEM               Holding until 24-DEC-2014 00:05:00

    898  ESTADISTICAS_PMS
                         SMSC                 Holding until 24-DEC-2014 01:00:00
``` 

Pay special attention to `billing_pp` and `mon_pml` (the one collecting stats for the monitoring).


##Keep an eye on billing files

Basically what is needed is check that billing files are produced.

You can see if this is true by having a look at the output from the last command being monitored: `dir billing_backup /since="-00:15:00"`, which simply lists the billing files for the last 15 minutes.

For a normal period of time (normal day not in peak or low traffic) this yields around 60-70 files.

##Differences between the 3 systems

All 3 clusters (SMTFB, SMTFC and SMTFD) are clone of each other except for GHLR entity (FSG functionality), which is not running on SMTFC.

Graphics for GHLR are not produced for SMTFC.