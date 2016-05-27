# Easy Setup Cassandra
==========
by Datos IO
<http://www.datos.io>

Introduction
------------
Easy Setup Cassandra sets up Cassandra clusters with ease.
Allows setup of both DSE Cassandra and Datastax Community Cassandra versions.

Requirements
------------
##### System Libraries:
* Python 2.7

##### Python Libraries: 
* Paramiko
* IPy
* Scp

Usage
-----
```
python -m -u <user> -p <pass> -v 2.1.4 -i 10.0.1.10,10.0.1.11
```

optional arguments:
  -h, --help            show this help message and exit
  -u USER, --user USER  OS user of remote cassandra nodes.
  -p PASSWORD, --password PASSWORD
                        OS user password of remote cassandra nodes.
  -k KEY, --key KEY     Os user key to access remote nodes, not required.
  -v CASS_VERSION, --cass_version CASS_VERSION
                        Cassandra version.
  -i CASS_IPS, --cass_ips CASS_IPS
                        File that lists ips of cluster nodes.
  -s CASS_SEEDS, --cass_seeds CASS_SEEDS
                        File that lists ips of seed nodes or comma separated
                        string.
  -d CASS_DATA_LOCATION, --cass_data_location CASS_DATA_LOCATION
                        Location for cassandra data on nodes
  -c CASS_COMMITLOG_LOCATION, --cass_commitlog_location CASS_COMMITLOG_LOCATION
                        Location for cassandra data on nodes
  -n CASS_NAME, --cass_name CASS_NAME
                        Name of your cluster.
  --num_tokens NUM_TOKENS
                        Set num vnode tokens per cassandra node.
  --clean               Purge DB and Commit Log.
  --verbose             Verbose reporting.
  --dse                 Install Datastax Enterprise version.
  --dse_user DSE_USER   Datastax Enterprise username.
  --dse_pass DSE_PASS   Datastax Enterprise password.
  --cass_auth           Add authentication to Cassandra cluster.
  --jmx_auth            Not implemented yet. Set JMX port.
  --jmx_port JMX_PORT   Set JMX port.

Downloads
---------

Source code is available at <https://github.com/datosio/easy_setup_cassandra>.

License
-------

The Easy Setup Cassandra code is distributed under the MIT License. <https://opensource.org/licenses/MIT>


Version History
---------------

Version 1.0 - June 7, 2016

* Initial release.