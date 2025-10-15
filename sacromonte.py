#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sacromonte: An agent to get the latest GTID from an inactive MariaDB/MySQL instance.
Returns real-time GTID data by searching backwards from the latest binlog.
"""

import configparser
import logging
import subprocess
import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_config(config_path: Path) -> dict:
    """Reads and validates the configuration file."""
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    conf_main = config['main']
    binlog_location = Path(conf_main['binlog_location'])
    binlog_basename = conf_main['binlog_basename']
    index_file = binlog_location / f"{binlog_basename}.index"

    if not index_file.is_file():
        raise FileNotFoundError(f"Binlog index file not found: {index_file}")

    with open(index_file, 'r') as f:
        binlogs = [str(binlog_location / line.strip()) for line in f if line.strip()]

    return {
        "ip": conf_main.get('ip', '0.0.0.0'),
        "port": conf_main.getint('port', 2934),
        "mysqlbinlog_exec": conf_main.get('mysqlbinlog_exec', 'mysqlbinlog'),
        "binlogs": binlogs
    }

def get_all_gtids_from_log(mysqlbinlog_exec: str, binlog_file: str) -> list:
    """Executes mysqlbinlog and extracts all GTID strings from a single file."""
    command = f"{mysqlbinlog_exec} {binlog_file} | grep GTID"
    gtids = []
    try:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, check=True
        )
        for line in result.stdout.strip().split('\n'):
            if "GTID" in line:
                try:
                    gtid = line.split("GTID")[1].strip().split()[0]
                    gtids.append(gtid)
                except IndexError:
                    continue
        return gtids
    except subprocess.CalledProcessError:
        logging.error(f"Failed to execute mysqlbinlog on {binlog_file}.")
        return []

def process_gtids(gtid_list: list) -> dict:
    """Processes a list of GTID strings and returns a dictionary of the latest for each pair."""
    latest_gtids = {}
    for gtid_str in gtid_list:
        try:
            parts = gtid_str.split('-')
            domain_id = int(parts[0])
            server_id = int(parts[1])
            key = f"{domain_id}-{server_id}"
            latest_gtids[key] = gtid_str
        except (ValueError, IndexError):
            logging.warning(f"Could not parse GTID string: {gtid_str}")
            continue
    return latest_gtids

def find_latest_gtids_by_pair(config: dict) -> dict:
    """
    Finds the latest GTID for each pair by searching backwards from the newest binlog.
    """
    binlog_files = config.get("binlogs", [])
    if not binlog_files:
        return {"metadata": {"latest_binlog_in_index": "N/A", "latest_binlog_scanned": "N/A"}, "gtids": {}}

    latest_binlog_in_index = binlog_files[-1]
    
    # --- KEY CHANGE: Search backwards from the newest file ---
    for binlog_file in reversed(binlog_files):
        logging.info(f"Searching for GTIDs in: {binlog_file}")
        gtids_in_file = get_all_gtids_from_log(config["mysqlbinlog_exec"], binlog_file)
        
        # If we find GTIDs in this file, it's the most recent one with activity.
        if gtids_in_file:
            logging.info(f"Found GTIDs. This is the most recent active binlog.")
            gtids = process_gtids(gtids_in_file)
            return {
                "metadata": {
                    "latest_binlog_in_index": latest_binlog_in_index,
                    "latest_binlog_scanned": binlog_file
                },
                "gtids": gtids
            }
    
    # If the loop finishes, no GTIDs were found in any file
    logging.warning("No GTIDs found in any binlog files.")
    return {
        "metadata": {
            "latest_binlog_in_index": latest_binlog_in_index,
            "latest_binlog_scanned": "None"
        },
        "gtids": {}
    }

def create_request_handler(config: dict):
    """Factory to create a request handler that has access to the config."""
    class SacromonteRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            self.config = config
            super().__init__(*args, **kwargs)

        def do_GET(self):
            results = find_latest_gtids_by_pair(self.config)
            json_response = json.dumps(results, indent=4).encode('utf-8')

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json_response)

    return SacromonteRequestHandler

def main():
    """Main function to parse arguments and start the HTTP server."""
    parser = argparse.ArgumentParser(description="Sacromonte: A tool to get the latest GTID from offline binlogs.")
    parser.add_argument(
        "-c", "--config",
        default="/etc/sacromonte.cnf",
        type=Path,
        help="Path to the configuration file (default: /etc/sacromonte.cnf)"
    )
    args = parser.parse_args()

    try:
        config = parse_config(args.config)
        Handler = create_request_handler(config)
        server_address = (config['ip'], config['port'])
        httpd = HTTPServer(server_address, Handler)
        
        logging.info(f"Starting Sacromonte HTTP server on http://{config['ip']}:{config['port']}")
        httpd.serve_forever()
        
    except FileNotFoundError as e:
        logging.error(f"Configuration error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    main()
