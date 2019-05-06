# sacromonte
Agent to get info from inactive mysql/mariadb instances. Currently info is only latest GTID applied.
It starts a local httpd server on port 2934 and replies with the GTID on the first line of the index page.
Two following lines are the binlog it searched back (latest binlog might not contain any GTID), and the latest binlog.

Minimal config:


`[main]`
`port=2934`
`ip=192.168.1.100`
`binlog_location=/data/mariadb`
`binlog_basename=myserver-bin`
`mysqlbinlog_exec=mysqlbinlog`

`

Execution:   `sudo python3 sacromonte.py`


Use: Point your web browser to 192.168.1.100:2934
