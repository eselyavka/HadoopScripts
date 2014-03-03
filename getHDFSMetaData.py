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
import tarfile
import time

''' TODO
1. Allow command line specified backup directory path 
2. Discover NameNode IP and port from config file, but this assumes the script is running on a cluster node or that a local copy of the configuration files are present (DONE)
3. Modify script to handle both Apache Hadoop 1.0/CDH3 and Apache Hadoop 2.0/CDH4 method of retrieving the metadata files  
    a) Modify the script to allow user specified txid for fsimage rather than always retrieving the latest version
    b) Improve error handling of command line options, i.e. an --endTxId option specified with --getimage makes no sense (DONE)
5. Add support for incremental backups of edits file:  
    a) Write backup info to a log file then use it to determine the last transaction range that was last backed up for the edits file
    b) Alternatively we could just parse the file name of the last edits file backup
6. Add support for compressing metadata files
    a) Write function for compressing files (DONE)
    b) Add parameter to enable compression
7. Move code for retrieving fsimage and edits files into separate functions rather than in main body.
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

def getVersion(host,port):
    '''Return the version/release of Hadoop that the NameNode is running'''
    nnHomePage = 'http://'+host+':'+port+'/'+'dfshealth.jsp' 
    request = urllib2.urlopen(nnHomePage)
    page = request.read()
    
    match = re.search(r'(Version:</td><td>)(.*),', page)
    if match:
        version =  match.group(2)        
        if re.search(r'2.[0-9]', version):
            return "Apache Hadoop 2.0"
        elif re.search(r'1.[0-9]', version):
            return "Apache Hadoop 1.0"

def isVersionMismatch(reportedVersion, actualVersion):
    '''Returns True if the version of Hadoop specified on the command line does not match the version of Hadoop reported by the server (extracted from the NameNode web interface)'''
    if reportedVersion == 1.0:
        reportedVersion = 'Apache Hadoop 1.0'
    elif reportedVersion == 2.0:
        reportedVersion = 'Apache Hadoop 2.0'
    
    if reportedVersion == actualVersion:
        return False
    return True
            
def timestamp(format='%Y-%m-%d-%H-%M-%S'):
    ''' Returns a timestamp in the format YYY-MM-DD-HH-MM-SS to be used to name our backup files '''
    return datetime.datetime.now().strftime(format)

def dlProgress(block_count, block_size, total_size):
    total_kb = total_size/1024
    print "%d kb of %d kb downloaded" %(block_count * (block_size/1024),total_kb )

def downloadFile(URL,fileDest,fileMode='600'):
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

def compressFile(filepath):
    print "Creating archive..."
    out = tarfile.open(filepath+'.tar.gz', mode='w:gz')
    try:
        basename = os.path.basename(filepath)
        out.add(filepath,arcname=basename)
        out.add(filepath+'.sha1',arcname=basename+'.sha1')
        print "Archive created, deleting original files..."
        #os.remove(filepath)
        #os.remove(filepath+'.sha1')
        print "Finished deleting files, backup complete."
    finally:
        out.close()

def getEditsTransactionNumber(editsFilePath, timeDelta):
    print "Try to find out startTxId and endTxId"    

    if not os.path.exists(editsFilePath):
        print "Error: the edits directory %s does not exist. Can't determinate startTxId and endTxId.\n" % editsFilePath
        return False
    if not os.access(editsFilePath, os.R_OK):
        print "Error: the edits directory %s is not readable by the user (%s) executing this script. Can't determinate startTxId and endTxId.\n" % (editsFilePath,os.getlogin())
        return False
    if not timeDelta.isdigit():
         print "Error: time delta %s contains non digit characters.\n" % timeDelta
         return timeDelta.isdigit()
    interval = int(time.time()-int(timeDelta))
    editsDict = {}
    for file in os.listdir(editsFilePath):
        matchEditFile = re.match(r'^edits_(.*)\-(.*)', file, re.M|re.I)
        if matchEditFile:
            fileMtime = int(os.stat(editsFilePath + '/' + matchEditFile.group()).st_mtime)
            if ( fileMtime >= interval ):
                 editsDict[matchEditFile.group()] = matchEditFile.group(1) + ';' + matchEditFile.group(2)
    return editsDict                  
        
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
    backupDir = '/tmp/hadoop.backup'
    
    # Location where edits stored and time delta for retrieving edits file
    editsFilePath = '/hadoop/namenode/dfs/sn/current/'
    timeDelta = '86400' # seconds

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
    
    
    HadoopRelease = getVersion(hostOrIP,str(options.namenode_port))
    if isVersionMismatch(options.release,HadoopRelease):
        print "\nError: ",options.release," was specified via the -r paremeter, however, server reports it is running: ",HadoopRelease
        print scriptUsage
        sys.exit(1)
    
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
            compressFile(backupFilename)
        else:
            print "Error: Could not retrieve the fsimage file; exiting script."
            sys.exit(1)


    if options.getedits:
        if (options.startTxId is None) or (options.endTxId is None):
            nnURL += '/getimage?getedit=1'
            if options.release == 1:
                backupFilename = backupDir + '/' + 'edits-' + '-' + hostOrIP + '-' + timestamp()
                
                print "Attempting to retrieve edits file from:\n%s" % nnURL

                print "Backup file will be written to: %s\n" % backupFilename
 
                if downloadFile(nnURL, backupFilename):
                    print "File successfully downloaded and written to backup directory."
                
                    fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
                    with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
                
                    print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
                    compressFile(backupFilename)

                else:
                    print "Error: Could not retrieve the edits file; exiting script."
                    sys.exit(1)
            else:
                editsDict = getEditsTransactionNumber(editsFilePath, timeDelta)
                if editsDict:
                    for editsFile in editsDict.keys():
                        nnUrlVar = nnURL
                        editsArr = editsDict[editsFile].split(';')
                        nnUrlVar += '&startTxId=%s&endTxId=%s' % (editsArr[0], editsArr[1])
                        
                        print "Attempting to retrieve edits file from:\n%s" % nnUrlVar
        
                        backupFilename = backupDir + '/' + 'edits-' + editsArr[0] + '-' + editsArr[1] + '-' + hostOrIP + '-' + timestamp()
                        
                        print "Backup file will be written to: %s\n" % backupFilename
 
                        if downloadFile(nnUrlVar, backupFilename):
                            print "File successfully downloaded and written to backup directory."
                
                            fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
                            with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
                
                            print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
                            compressFile(backupFilename)

                        else:
                            print "Error: Could not retrieve the fsimage file; exiting script."
                            sys.exit(1)
                else:
                    print "Error: The --getedits option was specified, this requires the --startTxId and --endTxId are also specified.\n"
                    print scriptUsage
        else:
            nnURL += '/getimage?getedit=1'
            if options.release == 1:
                backupFilename = backupDir + '/' + 'edits-' + '-' + hostOrIP + '-' + timestamp()
                
                print "Attempting to retrieve edits file from:\n%s" % nnURL

                print "Backup file will be written to: %s\n" % backupFilename
 
                if downloadFile(nnURL, backupFilename):
                    print "File successfully downloaded and written to backup directory."
                
                    fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
                    with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
                
                    print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
                    compressFile(backupFilename)

                else:
                    print "Error: Could not retrieve the edits file; exiting script."
                    sys.exit(1)
            else:
                nnURL += '&startTxId=%s&endTxId=%s' % (options.startTxId, options.endTxId) 
            
                print "Attempting to retrieve edits file from:\n%s" % nnURL
            
                backupFilename = backupDir + '/' + 'edits-' + options.startTxId + '-' + options.endTxId + '-' + hostOrIP + '-' + timestamp()
 
                print "Backup file will be written to: %s\n" % backupFilename
                if downloadFile(nnURL, backupFilename):
                    print "File successfully downloaded and written to backup directory."
                
                    fileHash = hashlib.sha1(open(backupFilename,'rb').read()).hexdigest()
                    with open (backupFilename + '.sha1','a') as f: f.write(fileHash)
                 
                    print "Hash (%s) of file written to: %s\n" % (fileHash,backupFilename)
                    compressFile(backupFilename)

                else:
                    print "Error: Could not retrieve the edits file; exiting script."
                    sys.exit(1)
