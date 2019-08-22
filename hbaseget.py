# HBase Thrift Script
#
# The 
from common import *
# Import thrift and hbase modules
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

import os
import os.path

# Connect to HBase Thrift server: This creates the socket transport and line protocol and allows the Thrift client to connect and talk to the Thrift server.
#transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
#protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
#sectransport = TTransport.TBufferedTransport(TSocket.TSocket("nightly62x-1.nightly62x.root.hwx.site", "9090"))
#transport = TTransport.TBufferedTransport(TSocket.TSocket("nightly6x-unsecure-1.nightly6x-unsecure.root.hwx.site", "9090"))
transport = TTransport.TBufferedTransport(TSocket.TSocket("nightly5x-unsecure-1.vpc.cloudera.com", "9090"))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

# Create and open the client connection: create the Client object you will be using to interact with HBase. From this client object, you will issue all your Gets and Puts.
client = Hbase.Client(protocol)
transport.open()

# 
tables = client.getTableNames()

#found = False

for table in tables:
  print("Table: ",  table.decode('ascii'))
  #if table == tablename:
    #found = True

# Create a new table
#tablename = 'haztable'
#cfname = 'cf1'
#client.createTable(tablename.encode('ascii'), [Hbase.ColumnDescriptor(name=cfname.encode('ascii'))])

# This closes up the socket and frees up the resources on the Thrift server.
transport.close()
