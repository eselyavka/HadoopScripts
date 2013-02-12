#!/usr/bin/env python

'''
Module name: getHDFSMetadata.py
Author: Vijay Thakorlal
GitHub: https://github.com/vijayjt
Blog: vijayjt.blogspot.co.uk 
Desc: Retrieves the fsimage and edits file from the NameNode daemon in a Apache Hadoop cluster. 
License: THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED. IN OTHER WORDS YOU SHOULD TEST THIS IN 
YOUR OWN DEV/TEST ENVIRONMENT BEFORE USING THIS IN PRODUCTION. 

'''

import optparse
import socket
import urllib2
import shutil
import sys
import re
import os
import datetime
import hashlib


''' TODO
1. Allow command line specified backup directory path 
2. Discover NameNode IP and port from config file, but this assumes the script is running on a cluster node or that a local copy of the configuration files are present
3. Modify script to handle both Apache Hadoop 1.0/CDH3 and Apache Hadoop 2.0/CDH4 method of retrieving the metadata files  
    a) Modify the script to allow user specified txid for fsimage rather than always retrieving the latest version
    b) Improve error handling of command line options, i.e. an --endTxId option specified with --getimage makes no sense
5. Add support for incremental backups of edits file:  
    a) Write backup info to a log file then use it to determine the last transaction range that was last backed up for the edits file
    b) Alternatively we could just parse the file name of the last edits file backup
'''

def isValidIPAddress(address):
    ''' Function to check if an IP address is valid (yes we could just use a regex) '''
    try: socket.inet_aton(address)
    except socket.error: return False
    else: return True

def isValidHostname(hostname):
    if len(hostname) > 255:
        return False
    if hostname[-1:] == ".":
        hostname = hostname[:-1] # strip exactly one dot from the right, if present
    allowed = re.compile("(?!-)[A-Z\d-]{1,63}(?<!-)$", re.IGNORECASE)
    return all(allowed.match(x) for x in hostname.split("."))

def isValidPort(port):
    if (port > 1023 and port < 65535):
        return True
    else:
        return False

def timestamp(format='%Y-%m-%d-%H-%M-%S'):
    ''' Returns a timestamp in the format YYY-MM-DD-HH-MM-SS to be used to name our backup files '''
    return datetime.datetime.now().strftime(format)

def dlProgress(block_count, block_size, total_size):
    total_kb = total_size/1024
    print "%d kb of %d kb downloaded" %(block_count * (block_size/1024),total_kb )

def downloadFile(URL,fileDest,fileMode='700'):
    ''' Function for downloading / fetching the fsimage and edits files '''
    try:
        remote_file = urllib2.urlopen(URL)
         
        headers = remote_file.info()
        totalSize = int(headers["Content-Length"])
        blockSize = 8192
        
        with open(fileDest, 'wb' + fileMode) as local_file:
            shutil.copyfileobj(remote_file,local_file,blockSize) 
	
    except urllib2.HTTPError as e:
        print "The server couldn\'t fulfil the request."
        print "Error code: ", e.code
        # Althought this produces ugly output but we print the error page/message to aid troubleshooting
        print "Error page follows: ", e.read()
        return False
    except urllib2.URLError as e:
        print "We failed to reach a server"
        print "Reason: ", e.reason
        return False
    else:
        return True

scriptUsage = """
%prog -s HOSTNAME | -i IP -r RELEASE [-p PORT] --ACTION --getimage | getedits [SUBOPTIONS]

Examples:

# Backup the fsimage file
getHDFSMetaData.py -s namenode.net -p 50070 -r 1 --getimage

# Backup the edits file
getHDFSMetaData.py -s namenode.net -p 50070 -r 2 --getedits --startTxId 1 --endTxId 50

"""

if __name__ == "__main__":
    parser = optparse.OptionParser(usage=scriptUsage, version="%prog 0.5")

    parser.add_option('-s', '--server', type='string', dest='namenode_host', help='The hostname of the NameNode')
    parser.add_option('-i', '--ip', type='string', dest='namenode_ip', help='The IP address of the NameNode')
    parser.add_option('-p', '--port', type='int', dest='namenode_port', help='The NameNode Web UI Port (default 50070)', default=50070)

    parser.add_option('--getimage', action='store_true', help='Retrieve the latest fsimage from the NameNode')
    parser.add_option('--getedits', action='store_true', help='Retrieve the latest edits file from the NameNode')

    parser.add_option('--startTxId', action='store', help='The starting transaction ID for the edits file. Only required for Apache Hadoop 2.0/CDH4.')
    parser.add_option('--endTxId', action='store', help='The end transaction ID for the edits file. Only required for Apache Hadoop 2.0/CDH4.')

    parser.add_option('-r', '--release', type='float', help='The release of Apache Hadoop, 1.0 or 2.0. This affects how the fsimage and edits files are retrieved and is a required parameter.')

    (options, args) = parser.parse_args()

    #print options
    #print args


    if (options.namenode_host is None) and (options.namenode_ip is None) or (options.release is None):
        parser.error("Wrong number of arguements. You must at least specify the NameNode IP address or hostname AND the release AND the getimage or getedits parameters.\n")
        print scriptUsage
    elif (options.getimage is None) and (options.getedits is None):
        parser.error("Wrong number of arguements. You must at least specify the NameNode IP address or hostname AND the release AND getimage or getedits parameters.\n")
        print scriptUsage


    # Location where backups will be stored
    backupDir = '/backup/hdfs'

    if not os.path.exists(backupDir):
        print "Error: the backup directory %s does not exist. Please create this directory or modify the backupDir variable.\n" % backupDir
        sys.exit(1)
    if not os.access(backupDir, os.W_OK):
        print "Error: the backup directory %s is not writable by the user (%s) executing this script.\n" % (backupDir,os.getlogin())
        sys.exit(1)


    nnURL = 'http://' 
    hostOrIP = ''

    if(options.namenode_port is not None):
        if not isValidPort(options.namenode_port):
            print "Invalid port number provided exiting script.\n"
            print scriptUsage
            sys.exit(1)

    if (options.namenode_ip is not None):
        if isValidIPAddress(options.namenode_ip):
            #print "Valid IP provided"
            hostOrIP = options.namenode_ip
        else:
            print "Invalid IP address provided, exiting script.\n"
        sys.exit(1)

    if (options.namenode_host is not None):
        if isValidHostname(options.namenode_host):
            #print "Valid hostname provided"
            hostOrIP = options.namenode_host
        else:
            print "Invalid IP address provided, exiting script.\n"
            sys.exit(1)


    nnURL += hostOrIP + ':' + str(options.namenode_port)


    if options.getimage:
        if options.release == 1:
            nnURL += '/getimage?getimage=1'
        else:
            nnURL += '/getimage?getimage=1&txid=latest'

        print "Attempting to retrieve fsimage file from:\n%s\n" % nnURL

        backupFilename = backupDir + '/' + 'fsimage-' + hostOrIP + '-' + timestamp() 

        print "Backup file will be written to: %s.\n" % backupFilename
        if downloadFile(nnURL, backupFilename):
            print "File successfully downloaded and written to backup directory."
            fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
            with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
            
            print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
        else:
            print "Error: Could not retrieve the fsimage file; exiting script."
            sys.exit(1)


    if options.getedits:
        if (options.startTxId is None) or (options.endTxId is None):
            print "Error: The --getedits option was specified, this requires the --startTxId and --endTxId are also specified.\n"
            print scriptUsage
        else:
            if options.release == 1:
                nnURL += '/getimage?getedit=1'
            else:
                nnURL += '/getimage?getedit=1&startTxId=%s&endTxId=%s' % (options.startTxId, options.endTxId) 
            
            print "Attempting to retrieve edits file from:\n%s" % nnURL
             
            backupFilename = backupDir + '/' + 'edits-' + options.startTxId + '-' + options.endTxId + '-' + hostOrIP + '-' + timestamp()

            print "Backup file will be written to: %s\n" % backupFilename
            if downloadFile(nnURL, backupFilename):
                print "File successfully downloaded and written to backup directory."
                fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
                with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
                
                print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
            else:
                print "Error: Could not retrieve the fsimage file; exiting script."
                sys.exit(1)

