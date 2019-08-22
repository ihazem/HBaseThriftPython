# HBase Thrift Script
#
# HBase Python Thrift code to create a table
# 
# Configuration:
# 	Define your configurations in an ini file (ex: hbasethriftpy.ini) and provide as arg to script
#
# Usage:
#	$ python3 hbasecreate.py hbasethriftpy.ini
#

# Import thrift and hbase modules
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import sys
import configparser

import os
import os.path

# Read in configs from config file
conffile = sys.argv[1]
config = configparser.ConfigParser()
config.sections()
config.read(conffile)
host = config['DEFAULT']['Host']
port = config['DEFAULT']['Port']

# Connect to HBase Thrift server: This creates the socket transport and line protocol and 
# allows the Thrift client to connect and talk to the Thrift server.
transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

# Create and open the client connection: create the Client object you will be using to 
# interact with HBase. From this client object, you will issue all your Gets and Puts.
client = Hbase.Client(protocol)
transport.open()

#########################################
# Define Table Name and Column Family Name Here
#########################################
tablename = 'haztable5'
cfname = 'cf1'

tableexists = 0

tables = client.getTableNames()
for table in tables:
  if table == tablename.encode('ascii'):
    print("TABLE ALREADY EXISTS")
    tableexists = 1

if tableexists == 0:
  # Create a new table if it does not exist
  client.createTable(tablename.encode('ascii'), [Hbase.ColumnDescriptor(name=cfname.encode('ascii'))])
  print("Created table", tablename, "with column familiy", cfname)
    
# Get list of tables from HBase and print
tables = client.getTableNames()
print("List of current user tables in HBase:")
for table in tables:
  print("Table: ",  table.decode('ascii'))


# This closes up the socket and frees up the resources on the Thrift server.
transport.close()
