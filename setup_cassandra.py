"""
The MIT License (MIT)
Copyright (c) 2016 Datos IO, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""


import os
import sys
import time
import getpass
import textwrap
import argparse
import paramiko
from IPy import IP
import threading
import multiprocessing
from scp import SCPClient


lock = threading.Lock()


def parse_input():
    parser = argparse.ArgumentParser(description='Cassandra Installer.')
    parser.add_argument('-u', '--user', default='centos', help='OS user of remote cassandra nodes.')
    parser.add_argument('-p', '--password', help='OS user password of remote cassandra nodes.')
    parser.add_argument('-k', '--key', help='Os user key to access remote nodes, not required.')
    parser.add_argument('-v', '--cass_version', default='2.1.13', help='Cassandra version.')
    parser.add_argument('-i', '--cass_ips', default='127.0.0.1', help='File that lists ips of cluster nodes.')
    parser.add_argument('-s', '--cass_seeds', help='File that lists ips of seed nodes or comma separated string.')
    parser.add_argument('-d', '--cass_data_location', default='/mnt/cassandra/data', help='Location for cassandra data on nodes')
    parser.add_argument('-c', '--cass_commitlog_location', default='/mnt/cassandra/commitlog', help='Location for cassandra data on nodes')
    parser.add_argument('-n', '--cass_name', default='My_Cluster', help='Name of your cluster.')
    parser.add_argument('--num_tokens', default=256, help='Set num vnode tokens per cassandra node.')
    parser.add_argument('--clean', action='store_true', help='Purge DB and Commit Log.')
    parser.add_argument('--verbose', action='store_true', help='Verbose reporting.')

    parser.add_argument('--dse', action='store_true', help='Install Datastax Enterprise version.')
    parser.add_argument('--dse_user', default='', help='Datastax Enterprise username.')
    parser.add_argument('--dse_pass', default='', help='Datastax Enterprise password.')

    parser.add_argument('--cass_auth', action='store_true', help='Add authentication to Cassandra cluster.')

    parser.add_argument('--jmx_auth', action='store_true', help='Not implemented yet. Set JMX port.')
    parser.add_argument('--jmx_port', default=7199, help='Set JMX port.')

    args = parser.parse_args()

    if args.dse:
        available_versions = ['4.0', '4.0.1', '4.0.2', '4.0.3', '4.0.4', '4.0.5', '4.0.6', '4.0.7',
                              '4.5', '4.5.1', '4.5.2', '4.5.3', '4.5.4', '4.5.5', '4.5.6', '4.5.7', '4.5.8', '4.5.9',
                              '4.6', '4.6.1', '4.6.2', '4.6.3', '4.6.4', '4.6.5', '4.6.6', '4.6.7', '4.6.8', '4.6.9', '4.6.10', '4.6.11', '4.6.12',
                              '4.7', '4.7.1', '4.7.2', '4.7.3', '4.7.4', '4.7.5', '4.7.6', '4.7.7', '4.7.8',
                              '4.8', '4.8.1', '4.8.2', '4.8.3', '4.8.4', '4.8.5', '4.8.6',
                              ]
    else:
        available_versions = ['2.0.0', '2.0.9', '2.0.10', '2.0.11', '2.0.12', '2.0.13', '2.0.14', '2.0.15', '2.0.16', '2.0.17',
                              '2.1.0', '2.1.4', '2.1.5', '2.1.6', '2.1.7', '2.1.8', '2.1.9', '2.1.10', '2.1.11', '2.1.12', '2.1.13',
                              '2.2.0', '2.2.1', '2.2.2', '2.2.3', '2.2.4', '2.2.5',
                              # '3.0.0',
                              # '3.3.0',
                              # '3.4.0',
                              ]

        # So users don't have to put in latest revision, we can handle that and bump to latest.
        if args.cass_version == '2.0':
            args.cass_version = '2.0.17'
        elif args.cass_version == '2.1':
            args.cass_version = '2.1.13'
        elif args.cass_version == '2.2':
            args.cass_version = '2.2.5'

    if args.cass_version not in available_versions:
        print('%s version not available./nAvailable versions: %s' % (args.cass_version, available_versions))
        sys.exit(1)

    if args.dse:
        if not args.dse_user or not args.dse_pass:
            print('Cassandra DSE version requires username and password.')

    # Fix @ issue in username for DSE repo. (Platform specific)
    if args.dse_user and '@' in args.dse_user:
        args.dse_user = args.dse_user.replace('@', '%40')

    return args


def parse_ips(cluster_ips):
    """
    Consumes ips as a comma separated string or a file with single ips or ip pairs per line and returns list of (pub, priv) ips.
    :param cluster_ips comma separated IP string or IP file to be parsed.
    :return: [(pub_1, priv_1), (pub_2, priv_2), ..., (pub_n, priv_n)].

    example string:

        <pub_1>,<pub_2>,...,<pub_n>
        <pub_1>,<priv_1>,<pub_2>,<priv_2>,...,<pub_n>,<priv_n>

        '10.1.2.3,10.1.2.4,10.1.2.5'
        '10.1.2.3,107.1.2.3,10.1.2.4,107.1.2.4'

    example IP file:

        #########################################################################
        # Public  Private
        10.1.2.3  107.1.2.3  # Most restricted IP (private) always second on line.
        10.1.2.4  107.1.2.4
        10.1.2.5  # If no public or private IP, only need one IP on line.
        #########################################################################
    """
    try:
        ip_strings = cluster_ips.split(',')

        if len(ip_strings) > 1:
            ips = []
            if ip_strings[0].split('.')[0] != ip_strings[1].split('.')[0]:  # Assume if they're different then pub, priv pairs.
                curr_ips = []
                for ip in ip_strings:
                    curr_ips.append(ip)
                    if len(curr_ips) == 2:
                        ips.append(tuple(curr_ips))
                        curr_ips = []
                if len(curr_ips) > 0:
                    curr_ips.append(curr_ips[0])
                    ips.append(tuple(curr_ips))
            else:
                ips = [(x, x) for x in cluster_ips.split(',')]
                IP(ips[0][0])  # Assume if first one is good ip, then argument not passed in as file, but string.
        else:
            ips = [(x, x) for x in cluster_ips.split(',')]
            IP(ips[0][0])  # Assume if first one is good ip, then argument passed in as string.

    except ValueError:
        with open(cluster_ips, 'r') as f:
            lines = f.readlines()

        ips = []
        for line in lines:
            line = line.split('#')[0]  # Allow for commenting in file.
            line = ' '.join(line.split()).replace('\n', '').strip()

            if line != '':
                ip_data = line.split()
                if len(ip_data) > 1:
                    pub, priv = ip_data
                else:
                    pub = priv = ip_data[0]  # If only 1 ip on line, use for both pub and priv ip.
                ips.append((pub, priv))

    return ips


def progress_bar(message, event):
    """
    Creates a spinner and message on screen. Example: [x] Performing pre-installation check.
    :param message: Message to show next to progress bar.
    :param event: threading.Event so we can know when to exit progress bar function.
    :return: None
    """
    i = 0
    spinner = ['\\', '/', '-']

    lock.acquire()  # Serialize progress reporting.

    while not event.is_set():
        if i % 5 == 0:
            j = (i // 5) % 3
            bar = '[%s] %s' % (spinner[j], message)

            print('\r%s' % bar), ; sys.stdout.flush()
        i += 1
        time.sleep(0.2)

    # Clear the line if status bar was shown.
    print('\r[x] %s' % (message))

    lock.release()


def rpc(ip, command, user=getpass.getuser(), password=None, key=None, timeout=60*20, retries=1, no_tty=False, suppress_output=False, print_real_time=False):
    """
    Remote procedure call.
    :param ip:
    :param command:
    :param user:
    :param password:
    :param key:
    :param timeout:
    :param retries:
    :param no_tty:
    :param suppress_output:
    :param print_real_time:
    :return: (<str>, <str>)
    """
    assert(retries >= 0)
    assert(timeout >= 0)

    retry_times = [3, 15, 30, 60, 60*5]
    for i in xrange(retries + 1):
        try:
            if not suppress_output:
                print('[Try #%s] RPC: {%s} %s' % (i + 1, ip, command))

            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=ip, username=user, password=password, key_filename=key, look_for_keys=True, port=22, timeout=timeout)
            std_in, out, err = ssh.exec_command(command, get_pty= not no_tty)
            out, err = out.read().strip('\n').strip('\r'), err.read().strip('\n').strip('\r')

            if 'Connection to %s closed' % ip in err:
                err = ''

            # Print command and try # if there is an error and we weren't printing information already.
            if suppress_output and err:
                print('[Try #%s] RPC: {%s} %s' % (i + 1, ip, command))

            # Now print output if we are not suppressing it.
            if not suppress_output and out:
                print(out)
            if err:
                print(err)

            time.sleep(0.5)
            ssh.close()
            return out, err

        except Exception as pe:
            print(pe)
            if not i > retries:
                time.sleep(retry_times[i])

    print('Error Connecting.')
    return '', 'Error Connecting.'


def scp(mode='put', local_path='.', remote_path='.', ip='127.0.0.1', user=getpass.getuser(), password=None, key=None, timeout=60*60):
    """

    :param mode:
    :param local_path:
    :param remote_path:
    :param ip:
    :param user:
    :param password:
    :param key:
    :param timeout:
    :return: None
    """
    assert(timeout >= 0)
    assert(mode in ['get', 'put'])

    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, username=user, password=password, key_filename=key, look_for_keys=True, port=22, timeout=timeout)
    with SCPClient(ssh.get_transport()) as scp:
        if mode == 'put':
            print('[SCP PUT] %s to <%s>:%s' % (local_path, ip, remote_path))
            scp.put(files=local_path, remote_path=remote_path, recursive=True)
        else:
            print("[SCP GET] <%s>:%s to %s" % (ip, remote_path, local_path))
            scp.get(local_path=local_path, remote_path=remote_path, recursive=True)

    ssh.close()


def install(ips, args):
    """
    Performs the installation of Cassandra to the whole cluster. Blocking.
    :param ips:
    :param args:
    :return: None
    """
    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Performing installation.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    REPO = "[datastax]\n" \
           "name = DataStax Repo for Apache Cassandra\n" \
           "baseurl = https://rpm.datastax.com/community\n" \
           "enabled = 1\n" \
           "gpgcheck = 0"

    if args.dse:
        REPO = "[datastax]\n" \
               "name = DataStax Repo for DataStax Enterprise\n" \
               "baseurl=https://%s:%s@rpm.datastax.com/enterprise\n" \
               "enabled = 1\n" \
               "gpgcheck = 0" % (args.dse_user, args.dse_pass)

    def _install_single_node(ip):
        def _rpc(cmd):
            return rpc(ip, cmd, args.user, args.password, args.key, suppress_output=not args.verbose)

        major_version = args.cass_version.replace('.', '')[:2]

        _rpc('''sudo sh -c "echo '%s' > /etc/yum.repos.d/datastax.repo"''' % REPO)

        if args.dse:
            _rpc('sudo yum -y install dse-full-%s-1' % (args.cass_version))
        else:
            _rpc('sudo yum install -y dsc%s-%s-1 cassandra%s-1' % (major_version, args.cass_version, args.cass_version))

        if args.clean:
            _rpc('sudo rm -rf %s/*' % args.cass_data_location)
            _rpc('sudo rm -rf %s/*' % args.cass_commitlog_location)

    # Spawn threads to run instructions on all nodes at once
    threads = []
    for pub, priv in ips:
        t = threading.Thread(target=_install_single_node, args=(pub,))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()


def do_pre_check(ips, args):
    """
    Checks requirements, java, firewalls for all nodes in cluster. Blocking.
    :param ips:
    :param args:
    :return: None
    """
    def _port_is_open(host, port, mode='tcp'):
        """
        # Utilizing timing attack to determine if port is blocked by firewall, or simply no listener.
        :param host:
        :param port:
        :return:
        """
        # cmd = 'echo "knock knock" | nc -w 10 %s %s %s' % ('-u' if mode == 'udp' else '', host, port)
        # if args.verbose:
        #     print(cmd)
        #
        # start_time = time.time()
        # rv = os.system(cmd)
        # end_time = time.time()
        #
        # if rv == 0:
        #     return True
        # if end_time - start_time < 5:
        #     return True
        # return False
        return True

    def _set_hostname(ip, args):
        out, _ = rpc(ip, 'hostname -i', args.user, args.password, args.key, suppress_output=not args.verbose)
        if 'Unknown host' in out:
            rpc(ip, '''sudo sh -c "echo '`hostname -I` `hostname`' >> /etc/hosts"''', args.user, args.password, args.key, suppress_output=not args.verbose)

    def _check_ports(ip):
        PORT_LIST = [7000, 7001, 7199, 9160, 9042, 8080]
        for port in PORT_LIST:
            if not (_port_is_open(ip, port, mode='tcp') and _port_is_open(ip, port, mode='udp')):
                print("Check for port %s failed. Make sure that ports %s are open." % (port, PORT_LIST))
                sys.exit(1)

    def _check_java(ip):
        out, _ = rpc(ip, 'which java', args.user, args.password, args.key, suppress_output=not args.verbose)
        if 'no java' in out:
            rpc(ip, 'sudo yum install -y java-1.7.0-openjdk-devel', args.user, args.password, args.key, suppress_output=not args.verbose)

    def _create_directories(ip):
        def _rpc(cmd):
            return rpc(ip, cmd, args.user, args.password, args.key, suppress_output=not args.verbose)

        out1, err1 = _rpc('sudo mkdir -p %s' % args.cass_data_location)
        out2, err2 = _rpc('sudo mkdir -p %s' % args.cass_commitlog_location)
        out3, err3 = _rpc('sudo chmod -R 777 %s' % args.cass_data_location)
        out4, err4 = _rpc('sudo chmod -R 777 %s' % args.cass_commitlog_location)
        out5, err5 = _rpc('sudo chown -R %s:%s %s' % (args.user, args.user, args.cass_data_location))
        out6, err6 = _rpc('sudo chown -R %s:%s %s' % (args.user, args.user, args.cass_commitlog_location))

        # Check for errors.
        for out in [out1+err1, out2+err2, out3+err3, out4+err4, out5+err5, out6+err6]:
            if 'denied' in out or 'cannot' in out:
                print("Error creating cassandra directories.\n%s" % out)
                sys.exit(1)

    def _single_node_pre_check(ip):
        _check_ports(ip)
        _create_directories(ip)
        _set_hostname(ip, args)
        _check_java(ip)

    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Performing pre-installation check.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    # Spawn threads to run instructions on all nodes at once
    threads = []
    for pub, priv in ips:
        t = threading.Thread(target=_single_node_pre_check, args=(pub,))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()


def do_cleanup(ips, args):
    """

    :param ips:
    :param args:
    :return: None
    """
    def _cleanup_single_node(ip):
        def _rpc(cmd):
            return rpc(ip, cmd, args.user, args.password, args.key, suppress_output=not args.verbose)

        # TODO: (Make this more targeted)
        # Stop services.
        _rpc('sudo service cassandra stop')
        _rpc('sudo service dsc stop')
        _rpc('sudo service dse stop')
        _rpc('sudo service datastax-agent stop')
        _rpc('sudo /etc/init.d/cassandra stop')
        _rpc('sudo /etc/init.d/dsc stop')
        _rpc('sudo /etc/init.d/dse stop')
        _rpc('sudo /etc/init.d/datastax-agent stop')

        # Uninstall packages.
        _rpc('sudo yum remove -y \'*cassandra*\' \'*dsc*\' \'*dse*\' \'*datastax*\'')

        # Cleanup install folders.
        _rpc('sudo rm -rf /var/lib/cassandra/*')
        _rpc('sudo rm -rf /var/log/{cassandra,hadoop,hive,pig}/*')
        _rpc('sudo rm -rf /etc/{cassandra,dsc,dse}/*')
        _rpc('sudo rm -rf /usr/share/{dse,dse-demos}')
        _rpc('sudo rm -rf /etc/default/{cassandra,dsc,dse}')


    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Performing pre-install cleanup.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    # Spawn threads to run instructions on all nodes at once
    threads = []
    for pub, priv in ips:
        t = threading.Thread(target=_cleanup_single_node, args=(pub,))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()


def set_yaml_configs(ips, seeds, args):
    """
    Sets the cassandra.yaml configuration settings on each nodes.
    :param ips: List of (<pub>, <priv>) ip tuples of all Cassandra nodes.
    :param seeds: List of (<pub>, <priv>) ip tuples of Cassandra seed nodes.
    :param args: Argparse arguments.
    :return: None
    """

    if args.dse:
        configs_dir = '/etc/dse/cassandra'
    else:
        configs_dir = '/etc/cassandra/default.conf'

    def _single_node_set_yaml(pub, priv):
        def _rpc(cmd):
            return rpc(pub, cmd, args.user, args.password, args.key, suppress_output=not args.verbose)

        # Update seeds.
        seeds_orig = 'seeds: "127.0.0.1"'
        seeds_new = 'seeds: "%s"' % ','.join([x for x, y in seeds])
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (seeds_orig, seeds_new, configs_dir))

        # Update listener.
        listener_orig = 'listen_address: localhost'
        listener_new = 'listen_address: %s' % priv
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (listener_orig, listener_new, configs_dir))

        # Update rpc address.
        rpc_orig = 'rpc_address: localhost'
        rpc_new = 'rpc_address: %s' % priv
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (rpc_orig, rpc_new, configs_dir))

        # Update data location.
        data_orig = '/var/lib/cassandra/data'.replace('/', '\\/')
        data_new = '%s' % args.cass_data_location.replace('/', '\\/')
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (data_orig, data_new, configs_dir))

        # Update commitlog location.
        commit_orig = '/var/lib/cassandra/commitlog'.replace('/', '\\/')
        commit_new = '%s' % args.cass_commitlog_location.replace('/', '\\/')
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (commit_orig, commit_new, configs_dir))

        if args.cass_auth:
            auth_orig = 'authenticator: AllowAllAuthenticator'
            auth_new = 'authenticator: PasswordAuthenticator'
            _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (auth_orig, auth_new, configs_dir))

            auth_orig = 'authorizer: AllowAllAuthorizer'
            auth_new = 'authorizer: CassandraAuthorizer'
            _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra.yaml" % (auth_orig, auth_new, configs_dir))

        # Update JMX port.
        jmx_orig = 'JMX_PORT="7199"'
        jmx_new = 'JMX_PORT="%s"' % args.jmx_port
        _rpc("sudo sed -i 's/%s/%s/g' %s/cassandra-env.sh" % (jmx_orig, jmx_new, configs_dir))

    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Updating Cassandra settings.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    # Spawn threads to run instructions on all nodes at once
    threads = []
    for pub, priv in ips:
        t = threading.Thread(target=_single_node_set_yaml, args=(pub, priv))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()


def turn_on(ips, seeds, args):
    """
    Turns on the cassandra nodes. Seeds first and then all nodes.
    :param ips: All nodes in cluster.
    :param seeds: Seed nodes in cluster.
    :param args: Argparse arguments.
    :return: None
    """

    # Spawn threads to run instructions on all nodes at once
    def _turn_on_single_node(ip):
        if args.dse:
            rpc(ip, 'sudo service dse start', args.user, args.password, args.key, suppress_output=not args.verbose)
        else:
            rpc(ip, 'sudo service cassandra start', args.user, args.password, args.key, suppress_output=not args.verbose)

    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Starting Cassandra cluster.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    threads = []
    for pub, priv in seeds:
        t = threading.Thread(target=_turn_on_single_node, args=(pub,))
        t.setDaemon(True)
        t.start()
        threads.append(t)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    if len(ips) > len(seeds):
        time.sleep(40)  # Give time for the seed nodes to come up.

        # Spawn threads to run instructions on all nodes at once
        threads = []
        for pub, priv in ips:
            if (pub, priv) not in seeds:
                t = threading.Thread(target=_turn_on_single_node, args=(pub,))
                t.setDaemon(True)
                t.start()
                threads.append(t)
                time.sleep(15)  # Slow down startup a bit ... sometimes they get overwhelmed.

        # Wait for all threads to complete
        for t in threads:
            t.join()

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()


def final_report(seeds, args):
    """

    :param seeds:
    :param args:
    :return: None
    """

    # Start a progress bar if not in verbose mode.
    if not args.verbose:
        e = threading.Event()
        bar_thread = threading.Thread(target=progress_bar, args=('Checking status of cluster.', e))
        bar_thread.setDaemon(True)
        bar_thread.start()

    time.sleep(40)  # Give time for nodes to come up.
    ip = seeds[0][0]
    out, err = rpc(ip, 'nodetool status', args.user, args.password, args.key, suppress_output=not args.verbose)

    # Terminate the progress bar if not in verbose mode.
    if not args.verbose:
        e.set()
        bar_thread.join()
        time.sleep(1)  # Allow other progress bar to complete.
        print('\n%s' % out)


def print_ip_pairs(ip_list):
    """
    Function to standardize printing our (<pub>, <priv>) ip pair tuples.
    :param ip_list: List of (<pub>, <priv>) ip pairs.
    :return: None
    """
    print('%s  %s' % ('<Public>'.ljust(15), '<Private>'.ljust(15)))
    for pub, priv in ip_list:
        print('%s  %s' % (pub.ljust(15), priv.ljust(15)))


def do_welcome():
    """

    :return: None
    """
    title="""
           _____                       _   _
          / ____|                     | | | |
         | |  __  ___ _ __  _ __   ___| |_| |_ ____
         | | |_ |/ _ \ '_ \| '_ \ / _ \ __| __/ _  |
         | |__| |  __/ |_) | |_) |  __/ |_| || (_) |
          \_____|\___| .__/| .__/ \___|\__|\__\___/
                     | |   | |
                     |_|   |_|  The Cloud Maestro

          """
    print(title)


def print_license():
    license = """THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE."""
    print('*'*70)
    print('%s' % textwrap.fill(license, 70))
    print('*'*70)
    print('')


def main():
    args = parse_input()
    ips = parse_ips(args.cass_ips)

    do_welcome()
    print_license()

    # If more than one node use first two as seed nodes.
    seeds = parse_ips(args.cass_seeds) if args.cass_seeds else None
    if not seeds:
        if len(ips) > 2:
            seeds = [(ips[0][1], ips[0][1]), (ips[1][1], ips[1][1])]
        else:
            seeds = [(ips[0][1], ips[0][1]),]

    # Print seed and node information.
    print('+ Cassandra node IP addresses:')
    print_ip_pairs(ips)
    print('+ Seed Nodes IP addresses:')
    print_ip_pairs(seeds)
    print('')

    # Do precheck.
    do_pre_check(ips, args)

    # Do cleanup.
    do_cleanup(ips, args)

    # Do install.
    install(ips, args)

    # Set yaml configs.
    set_yaml_configs(ips, seeds, args)

    # Do turn on.
    turn_on(ips, seeds, args)

    # Final report
    final_report(ips, args)


if __name__ == "__main__":
    main()
