Hadoop Scripts
==============

This repository is for scripts for monitoring, managing or querying information from an Apache Hadoop Cluster.

getHDFSMetadata.py
------------------
The NameNode daemon in a Hadoop cluster runs a HTTP web interface (HTTP Servlet). One of the features exposed by this interface is the ability to retrieve the fsimage and edits HDFS filesystem metadata files.

The fsimage file contains a serialised form of the filesystem metadata (file names, permissions, owner etc) and the edits file contains a log of all file system changes (i.e. file or directory create operations, permission changes etc).

The fsimage is stored on disk and in memory but when changes are made to HDFS these are reflected in the in-memory version of fsimage and the edits log but not on the fsimage stored on disk (although it is periodically updated through a checkpoint process and on NameNode startup).

This script can be used to retrieve the fsimage and edits file from a NameNode.

Modules:
The script requires the following python modules - optparse, socket,urllib2sys, re, os,datetime,hashlib.

Python Version:
The script has been tested with on CentOS with Python 2.6

Apache Hadoop Release:
The script has been written to work with Apache 1.0/CDH3 and Apache 2.0/CDH4, however, at present it has only been tested with Apache 2.0/CDH4.

It should be noted that in Apache 1.0/CDH3 the NameNode stores only one fsimage and edits files that can be retrieve over HTTP as follows:
    fsimage: http://<namenode>:50070/getimage?getimage=1
    edits: http://<namenode>:50070/getimage?getedit=1

With Apache 2.0/CDH4, the NameNode now stores multiple fsimage and edits files to aid recovery, this complicates things somewhat. The new method of retrieving the files is as follows:

    fsimage: http://<namenode>:50070/getimage?getimage=1&txid=latest
    edits: http://<namdenode>:50070/getimage?getedit=1&startTxId=X&endTxID=Y

Where X and Y are the starting and ending transcation IDs. In release 2.0 the dfs.name.dir or dfs.namenode.name.dir directory will contain multiple fsimages:
    fsimage_0000000000000003100
    fsimage_0000000000000003400
- the number at the end indicates the last transaction that the fsimage covers. At present the script only retrieves the latest version of the fsimage and does not provide the user with the option of retrieving an fsimage with a specific transaction ID.

