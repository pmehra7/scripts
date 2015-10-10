from subprocess import check_output, call
import json
import os
import time
import pickle


masterNodesName = 'HadoopMaster_DataSci-v1.0.0'
slaveNodesName = 'HadoopSlave_DataSci-v1.0.0'

user = ''
url = 'https://192.168.56.102:8443/agility/api/v3.2/compute'

def getIPaddress(id):
    urlnode = url + '/' + str(id)
    nodeInfoJson = check_output(['curl', '-u', user, '-k', urlnode, '-HAccept:application/json', '-X', 'GET'])
    nodeDict = json.loads(str(nodeInfoJson))
    nodeIPpublic = str(nodeDict['publicAddress'])
    nodeIPprivate = str(nodeDict['privateAddress'])
    return nodeIPpublic, nodeIPprivate

def getPublicDNS(public_ip_address):
    # ec2-52-23-180-195.compute-1.amazonaws.com
    # ec2-54-91-149-88.compute-1.amazonaws.com
    ip = public_ip_address.replace('.', '-')
    dns = 'ec2-' + ip + '.compute-1.amazonaws.com'
    return dns

def generateXMLfile(inputXML, addressList):
    xmlOut = ''
    for addy in addressList:
        if addy[0] == 'master':
            dnsValue = addy[5]
            replaced = 'hdfs://' + dnsValue + ':8020'
            xmlOut = inputXML.replace('hdfs://ec2-54-197-12-119.compute-1.amazonaws.com:9000', replaced)
    return xmlOut

def generateSparkConf(inputSpark, addressList):
    sparkOut = ''
    for addy in addressList:
        if addy[0] == 'master':
            dnsValue = addy[5]
            replaced = 'export STANDALONE_SPARK_MASTER_HOST=' + dnsValue
            replaced1 = 'export SPARK_LOCAL_IP=' + dnsValue
            sparkOut = inputSpark.replace('export STANDALONE_SPARK_MASTER_HOST=`hostname`', replaced).replace('export SPARK_LOCAL_IP=`hostname`', replaced1)
    return sparkOut

# Curl script to get all compute nodes
computeJson = check_output(['curl', '-u', user, '-k', url, '-HAccept:application/json', '-X', 'GET'])
# Parse json output into dictionary
computeDict = json.loads(str(computeJson))
# Gets count of compute nodes
nodeCount = len(computeDict['links'])
# Store master or slave, node name, id, ip address
addresses = []

# Find the compute nodes we care about --> e.g HADOOPSPARKDEMO
for i in range(0, nodeCount):
    if masterNodesName in str(computeDict['links'][i]['name']):
        ipPublic, ipPrivate = getIPaddress(computeDict['links'][i]['id'])
        dnsPublic = getPublicDNS(ipPublic)
        addresses.append(('master', str(computeDict['links'][i]['name']), computeDict['links'][i]['id'], ipPublic, ipPrivate, dnsPublic ))
    if slaveNodesName in str(computeDict['links'][i]['name']):
        ipPublic, ipPrivate = getIPaddress(computeDict['links'][i]['id'])
        dnsPublic = getPublicDNS(ipPublic)
        addresses.append(('slave', str(computeDict['links'][i]['name']), computeDict['links'][i]['id'], ipPublic, ipPrivate, dnsPublic ))


with open('hdp_nodes', 'wb') as f:
    pickle.dump(addresses, f)
