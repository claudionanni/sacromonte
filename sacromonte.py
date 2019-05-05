#!/usr/bin/env python
import subprocess
import configparser
global listOfBinlogs
global mb_executable

from http.server import BaseHTTPRequestHandler, HTTPServer
# Http Server Class
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    # Start from last binlog, -1 index in python lists
    binlog_index=-1
    global listOfBinlogs
    self.send_response(200)
    self.send_header('Content-type','text/html')
    self.end_headers()
    binlog_list=read_conf()
    last_gtid=os_readbinlog(binlog_list[binlog_index])
    print(last_gtid,binlog_index,binlog_list[binlog_index])

    # If the latest binlog doesn't contain any GTID go to the previous one, stop anyway to first binlog found in index file
    while (last_gtid=="" and len(binlog_list)+binlog_index>0):
      print("Last binlog does not contain a GTID, checking the previous one...")
      binlog_index=binlog_index-1
      last_gtid=os_readbinlog(binlog_list[binlog_index])
      #last_gtid="" to test backward search, simulating missing gtid
      print(last_gtid,binlog_index,binlog_list[binlog_index])
    
    # At this point we should have found at least one GTID or none of the binary logs contain a single GTID
    if(last_gtid==""):
      last_gtid="NOT_FOUND"
    message = last_gtid + '<br>' + 'Searched up to: ' + binlog_list[binlog_index] + '<br>' + 'Latest binlog: ' + binlog_list[-1]
    self.wfile.write(bytes(message, "utf8"))
    return

# reads the last GTID recorded in the binary_log passed as parameter
def os_readbinlog(binary_log):
  global mb_executable
  cmd = mb_executable + ''' ''' + binary_log + ''' | grep end_log_pos | egrep GTID | tail -1 | awk -F"GTID"  '{print $2}' | awk '{print $1}' '''
  res = subprocess.run(cmd,shell=True,universal_newlines=True, check=True)
  output = subprocess.check_output(['bash','-c', cmd]).decode('ascii')
  print(output)
  return output

# Read the config file in /etc/binlog_infod.conf
def read_conf():
  global listOfBinlogs
  global mb_executable
  # Default config location
  conf="/etc/sacromonte.cnf"
  config = configparser.ConfigParser()
  config.read(conf)
  conf_bl=config['main']['binlog_location']
  conf_bb=config['main']['binlog_basename']
  conf_if=conf_bl + '/' + conf_bb + '.index'
  mb_executable=config['main']['mysqlbinlog_exec']
  # Get the list of binlogs from the index file
  listOfBinlogs = [(conf_bl + '/' + line.rstrip('\n')) for line in open(conf_if)]
  return listOfBinlogs


# Main loop
def run():
  print('Http server starting...')
  server_address = ('127.0.0.1', 2934)
  httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
  print('Listening on port 2934')
  httpd.serve_forever()
run()
