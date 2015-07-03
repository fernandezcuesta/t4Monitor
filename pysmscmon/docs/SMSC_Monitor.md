#SMSC 5.4 Monitoring Procedure

## **Introduction**

This procedure simplifies the process of collecting statistics from the SMSC by
using two main modules:

- **Collector module**

    - DCL Scripts running in OpenVMS batch jobs (requires python 2.50 or
higher installed on the SMSCs)
 
    - Python2 script for converting PML output into T4-Format2 compliant
comma-separated values (CSV) file, that can be visualized directly with 
[TLViz](http://h71000.www7.hp.com/openvms/products/t4/download.html).

- **Report generator**

    - Python2 scripts that download the CSV and log output for each system and
produce a HTML file with the results.


## Short how-to

1. **Prepare environment and download the tool** (see below *Environment*)
2. **Configure the tool** (see below *Tool settings*) with the user and password
3. **Run the tool** (see below *Run the tool*). Ignore `ssh/config not found`
 warnings.
4. If everything went OK, in the same folder where you have the tool there are
two subdirectories: `report` and `store`. The first contains the HTML reports
to be sent to the customer and the latter contains a copy of the data collected
(CSV statistics and log output).

<br/>
<hr/>


## **Automatic statistic collector**

A python-based tool running locally on the PC will automatically:

- Connect via SSH and SFTP to the 3 SMSCs
- Download the CSV files containing statistics for all monitored counters
- Run a DCL script on each SMSC and get the output back
- Put all this information together in a HTML file under `/reports` subfolder
- Make a local copy of the retrieved data under the `/store` subfolder:
   - Statistics saved both as plain CSV and pickle (pandas DF) files
   - Log output are saved in plain text files

The overall process takes aproximately **5 minutes**.

####Tool settings

Before executing, verify that valid credentials and other settings are set in
the general configuration file: **`settings.cfg`**, stored in the same folder
as the tool .EXE file.

```xml
    [DEFAULT]
    ssh_timeout = 10
    ssh_port = 22
    ip_or_hostname = 127.0.0.1
    folder = smsc$root:[monitor.out] <-- MON_PML output folder
    username = smsc                  <-- SMSC credentials
    password = smsc

    [GATEWAY]                        <-- VPN credentials
    username = <gateway_user>
    password = <gateway_password>
    ip_or_hostname = <gateway_ip_or_hostname>

    [MISC]
    calculations_file = calc.cnf

    [SMSC-1]                          <-- Section for each system to be monitored
                                          Name must match cluster's ID on T4 header
    ip_or_hostname = <SMSC-1_ip_or_hostname>
    tunnel_port = 20970              <-- Can be any unused port
    cluster_id = smsc-1              <-- CSV files are filtered based on this value

    [SMSC-2]
    ip_or_hostname = <SMSC-2_ip_or_hostname>
    tunnel_port = 0                  <-- Random port (within the range [20000, 60000))
    cluster_id = smsc-2

    [...]
```

#### Run the tool

>**Open an Anaconda Command Prompt and go to the folder where the tool was installed**

```dos
    C:\> cd <folder_where_tool_was_extracted>
    C:\FOLDER> SMSC_Monitor.exe
```

**IMPORTANT**: Allow firewall rule for `SMSC_Monitor.exe`.

> By default, the tool will download the data corresponding to the current day.
If you need long term data (load time may grow up though), run it with the
`--all` flag.

The outcome under normal operation (VPN connectivity up and running) should
look like this (repeated as many times as systems being monitored, 3 in this
case):
```
    ---------------------------------------------------------
    Collecting statistics for SMSC-1
    WARNING: Could not read SSH configuration file:  ~/.ssh/config
    Opening connection to server
    Server is started.
    WARNING: Could not read SSH configuration file
    Establishing SFTP session: smsc@127.0.0.1:20970...
    Reusing established sftp session...
    Loading file DISKS_SMSC1_22DEC2014.CSV;1...
    ...         ...          output ommitted      ...            ...
    Loading file ssd_smsc1_22dec2014.csv;1...
    Data size obtained from smsc1: (65, 378)
    Applying calculations on obtained data...
    Resulting dataframe size: (65, 460)
    Closing sftp connection on port 20970
    Closing ssh connection to 127.0.0.1
    WARNING: Could not read SSH configuration file
    Establishing SFTP session: smsc@127.0.0.1:20970...
    Getting log output from the system, may take a minute...
    Closing sftp connection on port 20970
    Closing ssh connection to 127.0.0.1
    Closing all open connections...
    ---------------------------------------------------------
```

Followed by:

```
Generating reports
Generating report for SMSC1
Generating graphics and rendering HTML output file...
Generating report for SMSC2
Generating graphics and rendering HTML output file...
Generating report for SMSC3
Generating graphics and rendering HTML output file...
Done!
```

Now there should be 3 new reports under the `reports` subdirectory.
Refer to `report_sample.html` for an example of how the outcome should look
like.


## Troubleshoot

### How to check that SMSC collectors are running



    PML> show entry mon_pml

or

    PML> pipe show queue /batch/all | sea sys$input mon_pml


If collectors are not running, **start** SMSC data collection with:
```
set def smsc$root:[MONITOR]
@mon_pml.com
```

If you want to **stop** data collection:
```
pipe show queue /batch/all | sea sys$input mon_pml    !Note down the job ID
delete /entry=<job_entry_number>
```


### Manually visualize the CSV

If the tool is not working or you want to manually visualize the CSV, the
faster way to do it is by downloading all CSV files in `smsc$root:[monitor.out]`
and read them with TLViz:

![ ](tlviz.png "")

Another option is opening the statistics locally stored under the `store`
subfolder with MS Excel or any other spreadsheet tool.



### Manually collect the log output

If the tool does not work or you want to run get the output of the logs, you
may run the following command in the OpenVMS prompt:

```
SMSC> set def smsc$root:[monitor]
SMSC> @log_mon_onscreen
```

Should you want to add extra commands to the script, edit monitor_config.cnf
under `[LOGMONITOR]` section. Refer to **Appendix b** for more information.

# Appendix
##Appendix a: PML parameters being monitored


###GIW class


| PML Parameter | Description |
|---------------|-------------|
| ***MOSM_OK*** | Number of accepted Mobile Originated SM |
| ***MOSM_FAIL*** | Number of rejected Mobile Originated SM |
| ***MTSM_OK*** | Number of successful MT-FwdSM responses received |
| ***MTSM_FAIL*** | Number of responses for MT-FwdSM with an error |
| ALERT_OK | Number of accepted Alert SC indications |
| ALERT_FAIL | Number of rejected Alert SC indications |
| FSG_MTSM_OK | Number of FSG accepted Mobile Terminated SM |
| FSG_MTSM_FAIL | Number of FSG rejected Mobile Terminated SM |
| ROUT_INFO_OK | SendRoutingInfoForSM successful operation responses received from the HLR |
| ROUT_INFO_FAIL | SendRoutingInfoForSM operation requests failed (either sending or received with error) |
| DEL_STAT_OK | Number of reportSM-DeliveryStatus successful operation responses received from HLR |
| DEL_STAT_FAIL | Number of reportSM-DeliveryStatus responses received from the
| HLR with an error |
| ABSENT_SUBSCRIBER | Number of MT-FwdSM and SRI responses with Absent-Subscriber error |
| ABSENT_SUBS_HLR | Number of SRI responses with Absent-Subscriber error |
| UNKWNOWN_SUBSCRIBER | Number of SRI responses with Unknown Subscriber error |
| MAP_SC_CONGESTION | destination user buffer full |
| MAP_INVALID_SME_ADDRES | blacklisted destination or prepaid no credit |
| MAP_SUBSCR_NOT_SC_SUBS | attempts to find non-secured SMSC |
| MAP_UNKNOWN_SC | wrong SMSC |
| SYSTEM_FAILURE | Number of system failure errors |
| TCAP_UABT_RCVD | number of TCAP User-Abort indications received |
| TCAP_UABT_SNT | number of TCAP User-Abort indications sent |
| TCAP_BEGIN_RCVD | Number of Begin Dialogue transactions received |
| TCAP_UNKN_MSG_RCVD | Number of incorrect or unknown TCAP msgs received |
| TCAP_MSG_DISC | Number of discarded TCAP messages |
| MAP_PROTO_ERR | Number of MAP protocol errors |
| MAP_PDU_DEC_ERR | Number of MAP PDU decode errors |
| MAP_PRV_DEC_ERR | Number of MAP provider decode errors |
| MAP_INV_APP_CTXT | Invalid MAP application contexts received |
| MAP_OPCODE_ERR | Number of invalid MAP operation codes received |
| MAP_DIALOGUES | Total number of dialogues per minute (STATISTICAL) |
| MAP_DIALOGUES_PENDING | Number of dialogues currently pending |
| MAP_DIA_IN_USE | Number of MAP dialogues in use |
| MAP_DIA_FREE | Number of free MAP dialogues |

<br/>

***Highlighted parameters*** are being collected by Telefónica with PMS and
reset on every run.
Since the data collection sample time is not synchronized with PMS, these
counters may not give accurate information.

Important metrics based in the table above:

*	Accepted vs rejected traffic: `MOSM_OK vs MOSM_FAIL`

*	`MTSM_OK vs MTSM_FAIL`

*	HLR Response: `ROUT_INFO_OK vs ROUT_INFO_FAIL`

*	Absent subscribers `ABSENT_SUBSCRIBER vs ABSENT_SUBS_HLR` (ABSENT_SUBSCRIBER
counts both a.s. failures for MTFWD-MT and SRI)

*	Invalid and unknown signaling =
`TCAP_UNKN_MSG_RCVD + TCAP_MSG_DISC + MAP_PROTO_ERR + MAP_PDU_DEC_ERR + MAP_PRV_DEC_ERR + MAP_INV_APP_CTXT + MAP_OPCODE_ERR`

*	Wrong SMSC = `MAP_UNKNOWN_SC`

*	Attempts to find non-secured SMSC = `MAP_SUBSCR_NOT_SC_SUBSCR`

*	Blacklisted destination = `MAP_INVALID_SME_ADDR`

*	Destination user buffer full = `MAP_SC_CONGESTION`

*	System failure errors = `SYSTEM_FAILURE`
                                                                                                                                                                                                                                            


###SMH
   

| PML Parameter | Description |
|---------------|-------------|
| SM_RECEIVED | Number of received Short Messages [STATISTICAL] |
| SM_TRANSMITTED | Short Messages delivery attempts [STATISTICAL] |
| NOTIF_TRANSMITTED | Notification delivery attempts [STATISTICAL] |
| SM_FAILED | Total number of delivery attempts of SM that failed [STATISTICAL] |
| NOTIF_FAILED | Total number of delivery attempts of Notifications that failed [CUMULATIVE] |
| FIRST_DELIV_ATTEMPT | [DMR] SM and Notification delivery attempts in FDA [STATISTICAL] |
| FDA_ON_SMSC_LIC | [DMR] FDA SM and Notifications using MDA licence [STATISTICAL] |
| RETRY_ATTEMPT | [DMR] Retries of SM and Notifications using MDA licence. When DMR is disabled, RETRY_ATTEMPT=DELIVERY_ATTEMPT (with slight delta)    [STATISTICAL] |
| DELIVERY_ATTEMPT | Number of delivery attempts of messages and notifications that have been sent by the SMH [STATISTICAL] |
| LICENSE_LIMIT_REACHED | Number of times that the delivery license was reached [CUMULATIVE] |
| LICENSE_LIMIT_DELAY | Max. delay due to license reached [STATISTICAL, use with /Interval] |
| SM_BUFFERED | Total number of SM buffered (First,Next or Deferred) [INDICATIVE] |
| NOTIF_BUFFERED | Total number of Notifications buffered (First and Next)  [INDICATIVE] |
| MSG_DEFERRED | Total number of SM in the Deferred Delivery Queue [INDICATIVE] |
| SM_VALID | Total number of received valid Short Messages [CUMULATIVE] |
| SM_INVALID | Total number of reveived invalid Short Messages [CUMULATIVE] |
| SM_DISMISSED | Total number of received valid Short Messages (SMs) that are rejected due to internal reason [CUMULATIVE] |
| STAT_MAX_SMDA_PER_SEC | Maximum number of delivery attempts performed per second [STATISTICAL] |
| SM_FST_TX_OK | Total (SM+notif) successful messages during 1st attempt [STATISTICAL] |
| SM_NXT_TX_OK | Total (SM+notif) successfully delivered during 2nd or subsequent attempts [STATISTICAL] |
| SM_FST_TX_FAIL_SMSC | Internal errors during 1st attempt [STATISTICAL] |
| SM_NXT_TX_FAIL_SMSC | Internal errors during 2nd or subsequent attempts [STATISTICAL] |
| SM_FST_TX_FAIL_EXTERN | External errors during 1st attempt [STATISTICAL] |
| SM_NXT_TX_FAIL_EXTERN | External errors during 2nd or subsequent attempts [STATISTICAL] |
| SM_DELETED_FST_TX_FAIL | Messages and notifications deleted after 1st failed attempt [STATISTICAL] |
| SM_DELETED_NXT_TX_FAIL | Messages and notifications deleted after 2nd or subsequent attempts [STATISTICAL] |
| LENGTH_SM_CUMULATIVE | Accumulated length of contents of all Short Messages accepted by the SMH [STATISTICAL] |
| BUF_TIME_CUMULATIVE | Accumulated time in seconds that the messages were buffered in SMH [STATISTICAL] |
| ALLOCATED_ADDRBLOCKS | Total number of allocated/active address blocks [INDICATIVE] |

<br/>

Important metrics based in the table above (only valid if DMR is disabled):

- Messages and notifications failed = `SM_FAILED, NOTIF_FAILED`

- Messages received and transmitted = `SM_RECEIVED, SM_TRANSMITTED`

- Messages invalid and dismissed= `SM_INVALID, SM_DISMISSED`

- Delivery_attempt_ratio_per_smh = `retry_attempt / interval_in_sec`

- license_usage_ratio_per_smh = `delivery_attempt_ratio_per_smh/delivery_limit`

**Per node:**

- license_usage_per_smh_on_node =
`sum(delivery_attempt_ratio_per_smh,on_node) / [smh_common/delv_per_node_max]`

- Percentage of buffers used

     `msg_buffered = sm_buffered + notif_buffered + msg_deferred` (for non DMR
systems)

     `buffer_usage = msg_buffered / retq_max`


- Successful FDA

     `total_messages = SM_FST_TX_OK + SM_FST_TX_FAIL_SMSC + SM_FST_TX_FAIL_EXTERN`

     `Successful_FDA = SM_FST_TX_OK / total_messages `


- Dropped after 1st delivery attempt: `SM_DELETED_FST_TX_FAIL/total_messages`

- Average attempts per message:

     `nxt_attemps = SM_NXT_TX_OK + SM_NXT_TX_FAIL_SMSC + SM_NXT_TX_FAIL_EXTERN`

- Average_attempts = `1 + (nxt_attemps) / total_messages`

- Average attempts per retried message

     `nxt_attempts/(SM_FST_TX_FAIL_SMSC + SM_FST_TX_FAIL_EXTERN - SM_DELETED_FST_TX_FAIL)`

- First attempts per second:  `total_messages / interval_in_sec`

- Average message length: `avg_length = LENGTH_SM_CUMULATIVE / SM_RECEIVED`

- Average time waiting in buffers: 

     `avg_buff_time = BUF_TIME_CUMULATIVE / (SM_FST_TX_OK + SM_NXT_TX_OK + SM_DELETED_NXT_TX_FAIL + SM_DELETED_FST_TX_FAIL)`



###SSD

| PML Parameter | Description |
|---------------|-------------|
| SM_MO_BARRED_ORIG_LST | Number of MO SMs rejected due to originator address being on the Black list or not on the White list [CUMULATIVE] |
| SM_MO_BARRED_RECIP_LST | Number of MO SMs rejected due to recipient address being on the Black list or not on the White list [CUMULATIVE] |


###GHLR

| PML Parameter | Description |
|---------------|-------------| 
| SRI_RECEIVED | Number of MO SMs rejected due to originator address being on the Black list or not on the White list [CUMULATIVE] |
| SRI_REJECTED | Number of MO SMs rejected due to recipient address being on the Black list or not on the White list [CUMULATIVE] |
| SRI_DISCARDED |  [CUMULATIVE] |
| MAP_OPCODE_ERR |  [CUMULATIVE] |
| MAP_INVOKE_ERR |  [CUMULATIVE] |
| MAP_INV_APP_CTXT |  [CUMULATIVE] |
| TCAP_UABT_RCVD |  [CUMULATIVE] |
| TCAP_UABT_SNT |  [CUMULATIVE] |



###IP7

| PML Parameter | Description |
|---------------|-------------|
| XUA_SND_BUF_FREE | Number of Free Send Buffers [INDICATIVE] |
| XUA_SIG_NODES_FREE | Number of Free Signal Buffers [INDICATIVE] |
| SCTP_DT_CHKS_RETRANS | Number of SCTP data chunks retransmitted [CUMULATIVE] |
| SSRV_RAS_BUF_FREE | Number of Free Reassembly Buffers [INDICATIVE] |
| SSRV_LOCAL_NTWK_CONG | Number of outbound messages dropped locally due to Network Congestion [CUMULATIVE] |
| SSRV_LOCAL_SS_CONG | Number of outbound messages dropped locally due to Subsystem Congestion [CUMULATIVE] |
| SSRV_LOCAL_MTP_FAIL | Number of outbound messages dropped locally due to MTP Failure [CUMULATIVE] |
| SSRV_LOCAL_SS_FAIL | Number of outbound messages dropped locally due to Subsystem Failure [CUMULATIVE] |
| SCTP_ASSOC_CURR_ESTAB | Total number of established SCTP associations [CUMULATIVE] |
| SCCP_SSC_RCVD | Number of received SCCP Subsystem Congested (SSC) messages [CUMULATIVE] |
| SSRV_OVERLOAD_PROT | Number of dropped messages due to overload protection [CUMULATIVE] |
| SSRV_NW_RCV_HOPC_VIOL | SCCP hop counter violation returned from network [CUMULATIVE] |
| SSRV_NW_RCV_MSG_TRANS | SCCP error in message transport from network [CUMULATIVE] |
| SSRV_NW_RCV_LOCAL_PROC | SCCP error in local processing returned from network [CUMULATIVE] |
| SSRV_NW_RCV_MTP_FAIL | SCCP error with return cause "MTP failure" from network [CUMULATIVE] |
| SSRV_NW_RCV_NTRAN_ADDR | SCCP error with return cause "No translation for this address" from network [CUMULATIVE] |
| SSRV_NW_RCV_NTRAN_NOA | SCCP error with return cause "No tra" [CUMULATIVE] |
| SSRV_NW_RCV_NTWK_CONG | SCCP error with return cause "Network congestion" from network [CUMULATIVE] |
| SSRV_NW_RCV_SS_CONG | SCCP error with return case "Subsystem congestion" from network [CUMULATIVE] |
| SSRV_NW_RCV_SS_FAIL | SCCP error with return cause "Subsystem  failure" from network [CUMULATIVE] |
| SSRV_TRANS_FAILURE | Number of dropped messages due to SCCP transportation errors [CUMULATIVE] |

<br/>
Important metrics based in the table above for IP7 class:
   
    
- Transportation errors
    -  to the network:     `SSRV_TRANS_FAILURE + SSRV_LOCAL_MTP_FAIL`
    -  from the network:   `SSRV_NW_RCV_MSG_TRANS + SSRV_NW_RCV_LOCAL_PROC + SSRV_NW_RCV_MTP_FAIL + SSRV_NW_RCV_HOPC_VIOL`    

- Congestion and subsystem errors:
    -  to the network:     `SSRV_LOCAL_NTWK_CONG + SSRV_LOCAL_SS_CONG + SSRV_LOCAL_SS_FAIL`
    -  from the network:   `SSRV_NW_RCV_NTWK_CONG + SSRV_NW_RCV_SS_CONG + SSRV_NW_RCV_SS_FAIL`

- Other errors from the network:
    - translation errors: `SSRV_NW_RCV_NTRAN_ADDR + SSRV_NW_RCV_NTRAN_NOA`

    

###SIWPC and SIWSMPP class Parameters

| PML Parameter       | Description                                   |
|---------------------|-----------------------------------------------| 
|SES_SETUP_REJ_MAXSES | Number of rejected session setup due to maximum number of sessions exceeded [CUMULATIVE] |
|SES_SETUP_TOT_ATT	 | Total number of setup attempts [CUMULATIVE]   |
|SES_SETUP_TOT_REJ    | Number of rejected setup attempts [CUMULATIVE]|

##Appendix b: Log files monitoring

-	A series of searches will be done automatically by a script for specific log 
files in the system.
-	Output will be an updated text file containing all the errors/warnings for
that day.

**[Part of `MONITOR_CONFIG.CNF`]**

```

!!!!!!!!!!!!!!! LOG MONITOR SECTION !!!!!!!!!!!!!!!!!!!!!!
!
! FILES:   element list containing the log files to be checked
! GREPS:   strings to be searched for each file, comma separated if more than one
! WINDOWS: corresponding to the /win attribute of OpenVMS search command
!
!
! FILES                       = "Log_file_1\Log_file_2\..."
! GREPS                       = "string1,string2\string1,string2,string3\..."
! WINDOWS                     = "N,M\N\..."
! OTHERCMDS                   = "VMS command1\VMS Command2\..." ` denotes double-quotes in a command
```


**Example:**


    [LOGMONITOR]
    FILES                   = "smsc$root:[log]smsc.log\DSA0:[SYS%.SYSMGR]OPERATOR.LOG"
    GREPS                   = "-e-,-w-,-f-,license\-e-,-w-,-f-\-e-,-f-"
    WINDOWS                 = "3,2\3,3"
    CMDS_LINES              = 3
    OTHERCMDS_1             = "@CMG$TOOLS:smsc_check_entities\show queue /batch/all\show cluster"
    OTHERCMDS_2             = "mon cluster/end=`+00:00:30` /summ=sys$scratch:mon.sum /nodisplay\type sys$scratch:mon.sum\purge sys$scratch:mon.sum"
    OTHERCMDS_3             = "dir billing_backup /since=`-00:15:00`"


> Since one of the commands monitors cluster for a 30sec period, time taken to
get the output can be **up to 1 minute**.


##Appendix c: Anyconnect for Linux users

There are several alternatives for Linux users:

-	Use the official Anyconnect client
-	Use Openconnect with/without `networkmanager-openconnect` (recommended)
-	Use hacked Anyconnect client (recommended when openconnect is not an option)

Last 2 options allow the user to ignore the routing policies as well as DNS
negotiated with the VPN server during connection establishment.

###Cisco Anyconnect client

- Log in to the VPN URL with your credentials
- Choose ***AnyConnect*** from the options at the left side
- Click on Start AnyConnect and let the client be installed automatically

###Openconnect

-	Download cstub from the VPN gateway:
```bash
$ wget https://<VPN_GW_IP>/CACHE/sdesktop/hostscan/linux_x64/cstub –no-check-certificate
```

-	Create a custom CSD wrapper script:   
```bash
#!/bin/sh
exec 2>&1 > /dev/null
shift
URL=
TICKET=
STUB=
GROUP=
CERTHASH=
LANGSELEN=
while [ "$1" ]; do
    if [ "$1" == "-ticket" ];   then shift; TICKET=$1; fi
    if [ "$1" == "-stub" ];     then shift; STUB=$1; fi
    if [ "$1" == "-group" ];    then shift; GROUP=$1; fi
    if [ "$1" == "-certhash" ]; then shift; CERTHASH=$1; fi
    if [ "$1" == "-url" ];      then shift; URL=$1; fi
    if [ "$1" == "-langselen" ];then shift; LANGSELEN=$1; fi
    shift
done
#ARGS="-log debug -ticket $TICKET -stub $STUB -group $GROUP -host $URL -certhash $CERTHASH"
ARGS="-log error -ticket $TICKET -stub $STUB -group $GROUP -host $URL -certhash $CERTHASH"
echo $ARGS
$HOME/cstub $ARGS
```

-	Connect Manually (set the appropriate values for `--user`, `--os` and
`--authgroup`):
```bash
$ sudo openconnect --user=<VPN_USER> --csd-user=$USER --no-xmlpost --no-cert-check \
 --os=linux-64 --authgroup=<VPN_AUTH_GRP> --csd-wrapper=$HOME/.wrapper.sh https://<VPN_GW_IP>
```
You can now manually set the default route and restore `/etc/resolv.conf`
entries.

-	Automatically with networkmanager-openconnect (preferred):

     *It is assumed that package* `networkmanager-openconnect`
*is already installed.* 

    Create a connection from the following profile at
`/etc/NetworkManager/system-connections/MY-VPN` (or wherever NetworkManager
profiles reside for your distribution) with the following contents (customize
your home folder and user!):
```bash
[connection]
id=MY-VPN
uuid=b851d0de-1926-411f-ae3e-92fef816752e
type=vpn
autoconnect=false
timestamp=1405349505
[vpn]
service-type=org.freedesktop.NetworkManager.openconnect
enable_csd_trojan=yes
xmlconfig-flags=0
pem_passphrase_fsid=yes
gwcert-flags=2
gateway-flags=2
autoconnect-flags=0
lasthost-flags=0
stoken_source=disabled
certsigs-flags=0
cookie-flags=2
csd_wrapper=YOUR_HOME_FOLDER/.wrapper.sh
gateway=<VPN_GW_IP>
authtype=password
[vpn-secrets]
certsigs=6F249B250D86F23D424BCAE965B4FCB810F0   <-- TODO: Check how to get this!!!
form:main:group_list=<VPN_ACCESS_GROUP>
form:main:username=<VPN_USERNAME>
lasthost=<VPN_GATEWAY_IP_OR_HOSTNAME>
[ipv6]
method=auto
[ipv4]
method=auto
route1=10.135.0.0/16,0.0.0.0,0    <-- Routes that will be redirected through the VPN
route2=10.36.17.224/28,0.0.0.0,0
ignore-auto-dns=true
never-default=true
```

### Hacked Anyconnect client

It is assumed that the official Cisco AnyConnect client (see ***Cisco Anyconnect
client***) has been already installed on the system.


- First we create a file `hack.c`:
```c
int _ZN27CInterfaceRouteMonitorLinux20routeCallbackHandlerEv()
{
  return 0;
}
```

- Then compile it:
```bash
$ gcc -o libhack.so -shared -fPIC hack.c
```

- Install libhack.so into the Cisco library path (check for your distribution):
```bash
$ sudo cp libhack.so  /opt/cisco/anyconnect/lib/
```

- Bring the agent down:
```bash
$ /etc/init.d/vpnagentd stop
(or with systemd)
$ systemctl stop vpnagentd
```

- Then fix up `/etc/init.d/vpnagentd` by adding
`LD_PRELOAD=/opt/cisco/anyconnect/lib/libhack.so` where the vpnagentd is being
invoked so it looks like this:
```bash
LD_PRELOAD=/opt/cisco/anyconnect/lib/libhack.so /opt/cisco/anyconnect/bin/vpnagentd
```
(see below for systemd)

- Now start the agent:
```bash
$/etc/init.d/vpnagentd start
(or with systemd)
$ systemctl start vpnagentd
```
<br/>

**SYSTEMD files:**

**[/etc/systemd/system/ciscovpn.service]**
```bash
[Unit]
Description=Cisco AnyConnect Secure Mobility Client Agent
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=forking
PIDFile=/var/run/vpnagentd.pid
ExecStart=/opt/cisco/anyconnect/bin/initscript
ExecStop=/usr/bin/killall /opt/cisco/anyconnect/bin/vpnagentd
Restart=on-abort

[Install]
# one may want to use multi-user.target instead
WantedBy=graphical.target
```

**[/opt/cisco/anyconnect/bin/initscript]**
```bash
#!/bin/bash
LD_PRELOAD=/opt/cisco/anyconnect/lib/libhack.so /opt/cisco/anyconnect/bin/vpnagentd
```

##Environment

###Linux

```console
$ mkvirtualenv --python=$(which python2) pySMSCMon
$ git clone https://github.com/fernandezcuesta/pySMSCMon.git .
$ pip install -r requirements.txt
```

###Windows

- Download and install [Microsoft Visual C++ compiler for Python 2.7](http://aka.ms/vcpython27)
- Download and install [git](http://git-scm.com/download/win)
- Download and install [Anaconda](https://store.continuum.io/cshop/anaconda/) (Python 2 version)
- Open an Anaconda Command Prompt and run the following commands:

    ```dos
    Anaconda> git clone https://github.com/fernandezcuesta/pySMSCMon.git SMSC
    Anaconda> cd SMSC
    Anaconda> conda create -n SMSC --file requirements-conda.txt
    Anaconda> activate SMSC
    [SMSC] > 
    ```

- [jesusmanuel.fernandez@acision.com](mailto:jesusmanuel.fernandez@acision.com)


##Version History

|Version|Status|Date       |Details of Changes                                           |Author(s)   |Approver(s)|
|-------|------|-----------|-------------------------------------------------------------|------------|-----------|
|0.1    |DRAFT |13/Nov/2012|Initial version	                                             |fernandezjm |           |
|0.2    |DRAFT |14/Nov/2012|Typos, Acision template                                      |fernandezjm |           |
|0.3    |DRAFT |07/Dec/2012|Added calculation files                                      |fernandezjm |           |
|0.4    |DRAFT |17/Dec/2012|Fixed typos, included overall procedure, added output sample |fernandezjm |	          |
|1.0    |FINAL |19/Dec/2012|Added how-to and troubleshooting                             |fernandezjm |           |
|1.1.1  |FINAL |23/Dec/2013|Cleaned and updated	                                         |fernandezjm |           |
|1.2    |DRAFT |22/Dec/2014|Added Linux client appendix <br/>Updated for 2014            |fernandezjm |           |
|1.2.1  |DRAFT |23/Dec/2014|Added `--all` option                                         |fernandezjm |           |
|1.2.2  |FINAL |23/Dec/2014|Added short how-to                                           |fernandezjm |           |
|1.2.3  |FINAL |23/Dec/2014|Minor corrections                                            |fernandezjm |           |
|1.2.4  |FINAL |24/Dec/2014|Typos fixed                                                  |fernandezjm |           |
|1.2.5  |DRAFT |22/Jan/2015|Minor changes in VPN procedure <br/>Added new flags `--settings` and `--fast` for refactored code| fernandezjm |         |
