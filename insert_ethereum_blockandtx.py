from web3 import Web3, HTTPProvider, IPCProvider
from attributedict.collections import AttributeDict
from datetime import datetime
from hexbytes import HexBytes

import json
import pymongo
import threading
import sys
import time
import subprocess
import binascii

### docker run command ###
# docker run -itd --restart=always --name ethereum-node\
# -v /home/daphneashin/ethereum:/root/.ethereum -p 8545:8545 -p 30303:30303 \
# ethereum/client-go:latest --ropsten --syncmode "full" --http --http.addr="0.0.0.0" --http.api="db,eth,net,web3,personal"
# docker exec -it ethereum-node geth attach 

### DB Connection Info ###
connectionLocalIP = pymongo.MongoClient('127.0.0.1',1000)
localDB = connectionLocalIP.get_database('Ethereum')
block_Collection = localDB.get_collection('Block')
transaction_Collection = localDB.get_collection('Transaction')

##### Ethereum Node Connection ######
web3 = Web3(HTTPProvider("http://127.0.0.1:8545"))

### block data encoding ###
def blockData_encode(block_height):
    getBlock_data = web3.eth.getBlock(block_height)        
    block_data = {}

    for n in getBlock_data:
        element_type = type(getBlock_data[n])

        if element_type is list:
            list_length = len(getBlock_data[n])
            tx_list = []

            if list_length == 0:
                block_data[n] = tx_list
                continue
            else:
                for i in range(0, list_length):
                    temp = '0x'+str(binascii.hexlify(getBlock_data[n][i]).decode('utf-8'))
                    tx_list.append(temp)

                block_data[n] = tx_list

        elif element_type is HexBytes:
            block_data[n] = '0x'+str(binascii.hexlify(getBlock_data[n]).decode('utf-8'))
        else:
            # element_type in (int, float, str)
            block_data[n] = getBlock_data[n]
    
    return block_data

def Web3_getTransaction(transaction_id):
    temp = web3.eth.getTransaction(transaction_id)
    txData = eth_getTransactionData_encode(temp)
    return txData

def Web3_getTransactionReceipt(transaction_id):
    temp = web3.eth.getTransactionReceipt(transaction_id)
    txData = eth_getTransactionData_encode(temp)
    return txData

### transaction Data encoding ###
def eth_getTransactionData_encode(transaction_data):
        data = {}

        for n in transaction_data:
            element_type = type(transaction_data[n])

            if element_type in (int, float, str, bool):
                data[n] = transaction_data[n]

            elif element_type is list:
                list_length = len(transaction_data[n])
                list_ = []

                if list_length == 0:
                    data[n] = list_
                    continue
                else:
                    for i in range(0, list_length):
                        if type(transaction_data[n][i]) is not list:
                            temp_ = {}

                            for key_ in transaction_data[n][i]:
                                value_type = type(transaction_data[n][i][key_])

                                if value_type in (int, float, str, bool):
                                    temp_[key_] = transaction_data[n][i][key_]
                                
                                elif value_type is list:
                                    key_length = len(transaction_data[n][i][key_])
                                    value_ = []

                                    if key_length == 0:
                                        temp_[key_] = value_
                                        continue
                                    else:
                                        for r in range(0, key_length): 
                                            value_.append('0x'+str(bytearray(transaction_data[n][i][key_][r]).hex()))         

                                        temp_[key_] = value_

                                elif value_type is HexBytes:
                                    temp_[key_] = '0x'+str(bytearray(transaction_data[n][i][key_]).hex())

                                else:
                                    print('Element Type error '+str(element_type))
                                    exit()

                            list_.append(temp_)
                        else:                           
                            temp = '0x'+str(bytearray(transaction_data[n][i]).hex())
                            list_.append(temp)

                    data[n] = list_

            elif element_type is HexBytes:
                data[n] = '0x'+str(bytearray(transaction_data[n]).hex())   
            
            elif transaction_data[n] is None:
                data[n] = str(transaction_data[n])
            
            else:
                print('Element Type error '+str(element_type))
                exit()
        
        return data 

### DB bestBlockHeight ###
def database_bestBlockHeight():
    block_count = block_Collection.find({}).sort("_id",-1).count()

    #print(str(lastBlockHeight)+' ,'+str(block_count)) 
    if block_count !=0 : 
        lastBlock = block_Collection.find({}).sort("_id",-1).limit(1)
        lastBlockHeight = lastBlock[0]['_id']
        
        gap = block_count - lastBlockHeight
        #print('lastBlock'+str(lastBlock))

        #return lastBlockHeight + 1
        if gap == 1 :
            temp = lastBlockHeight + 1
            removeTx(temp)
            return temp
        else :
            return -1    
    else :
        return 0

def removeTx(block_height):
    getBlock_data = web3.eth.getBlock(block_height)
    nTx = int(len(getBlock_data['transactions']))
        
    for index in range(0, nTx):
        txid = '0x'+str(binascii.hexlify(getBlock_data['transactions'][index]).decode('utf-8'))    
        transaction_Collection.remove({'_id': txid})
        print(str(index+1)+'/'+str(nTx)+' - '+str(getBlock_data['transactions'][index]))
            
def insertBlock():
    syninfo = web3.eth.syncing
    currentBlock = int(syninfo['currentBlock'])

    start = int(database_bestBlockHeight()) 
    end = currentBlock + 1
    print("start block height: "+ str(start) +", end block height: "+str(end-1))
    print()

    for index in range(start, end):        
        block_data = blockData_encode(index)
        nTx = int(len(block_data['transactions']))

        #print(block_data)
        block_dic = {
            '_id': int(block_data['number']),
            'hash': block_data['hash'], 
            'timestamp': int(block_data['timestamp']),
            'date time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(block_data['timestamp']))),
            'nTx': int(len(block_data['transactions'])),
            'transactionsRoot': block_data['transactionsRoot'],
            'transactions': block_data['transactions'],
            'receiptsRoot': block_data['receiptsRoot'],
            'logsBloom': block_data['logsBloom'],
            'stateRoot': block_data['stateRoot'],
            'sha3Uncles': block_data['sha3Uncles'],
            'uncles': block_data['uncles'],
            'miner': block_data['miner'],
            'gasUsed': float(block_data['gasUsed']),
            'gasLimit': float(block_data['gasLimit']),           
            'difficulty': float(block_data['difficulty']),
            'totalDifficulty': float(block_data['totalDifficulty']),
            'size': float(block_data['size']),
            'nonce': block_data['nonce'],
            'mixHash': block_data['mixHash'],
            'extraData': block_data['extraData'],
            'parentHash': block_data['parentHash'],
        }

        tx_dics = []
        if nTx is not 0 :
            tx_dics = insertTransaction(nTx, block_data['transactions'], int(block_data['timestamp']))
            transaction_Collection.insert_many(tx_dics)

        block_Collection.insert(block_dic)         
        print("insert block height: "+ str(index)+', nTx: '+str(nTx))
    
    return end

def insertTransaction(nTx, transactionList, timestamp_):
    tx_dics = []

    for transaction_id in transactionList:
        encoding_transactionData = Web3_getTransaction(transaction_id)
        encoding_transactionDataReceipt = Web3_getTransactionReceipt(transaction_id)
        transaction_data = {}

        transaction_data = {
            '_id': encoding_transactionData['hash'],               
            'blockNumber':int(encoding_transactionData['blockNumber']),
            'blockHash': encoding_transactionData['blockHash'],
            'timestamp': int(timestamp_),
            'date time': str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp_))),
            'transactionIndex':int(encoding_transactionDataReceipt['transactionIndex']),
            'from':encoding_transactionData['from'],
            'root':encoding_transactionDataReceipt['root'],
            'to':encoding_transactionData['to'],
            'value':float(encoding_transactionData['value']),
            'input':encoding_transactionData['input'],
            'nonce':int(encoding_transactionData['nonce']),
            'v':int(encoding_transactionData['v']),                
            'r':encoding_transactionData['r'],
            's':encoding_transactionData['s'],
            'gas':encoding_transactionData['gas'],
            'gasPrice':float(encoding_transactionData['gasPrice']),
            'gasUsed':float(encoding_transactionDataReceipt['gasUsed']),
            'cumulativeGasUsed':float(encoding_transactionDataReceipt['cumulativeGasUsed']),
            'contractAddress':encoding_transactionDataReceipt['contractAddress'],
            'logs':encoding_transactionDataReceipt['logs'],
            'logsBloom':encoding_transactionDataReceipt['logsBloom'],              
            'transaction Type': '',
            'type':encoding_transactionData['type'],  
        }

        if len(transaction_data['logs'])==0:
            transaction_data['transaction Type'] = 'Normal Transaction'
        else:
            transaction_data['transaction Type'] = 'Smart Contract Transaction'

        tx_dics.append(transaction_data)

    return tx_dics

#insertBlock()
if __name__=="__main__":    
    while(1):
        insertBlock()

        now = time.localtime()
        print()
        print('%04d/%02d/%02d %02d:%02d:%02d'% (now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec))
        time.sleep(60)

        now = time.localtime()
        print('%04d/%02d/%02d %02d:%02d:%02d'% (now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec))
        print()
