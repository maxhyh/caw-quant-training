import etherscan.accounts as accounts
#import etherscan.blocks as blocks       Error: No module named 'etherscan.blocks'
from etherscan.contracts import Contract
from etherscan.proxies import Proxies
import etherscan.stats as stats
import etherscan.tokens as tokens
#import etherscan.transactions as transactions         Error: No module named 'etherscan.transactions'

import json
import os
import pandas as pd
import requests
import unittest
import time

class account():
    
    def __init__(self, key, address):
        # Setup api for single address
        self.key = key
        self.address = address
        self.api = accounts.Account(address=address, api_key=key)

    def balance(self):
        balance = self.api.get_balance()
        balance = [balance]
        return pd.DataFrame(balance, columns=['Account Balance'])

    def collect_transactions(self, page, offset):
        collect_transactions = self.api.get_transaction_page(page=page, offset=offset)
        return pd.DataFrame(collect_transactions)

    def trans (self):
        return self.api.get_all_transactions()

    def erc20_trans(self, erc20):
        # Collect ERC20 Transactions
        erc20_trans = self.api.get_transaction_page(erc20=erc20)
        return pd.DataFrame(erc20_trans)

    def collect_blocks_minds(self, page, offset):
        collect_blocks_minds = self.api.get_blocks_mined_page(page=page, offset=offset)
        return pd.DataFrame(collect_blocks_minds)

    def block_minds(self):
        return self.api.get_all_blocks_mined()

"""
class bolcks():     Error: No module named 'etherscan.blocks'

    def __init__(self, key):
        self.api = blocks.Blocks(api_key=key)

    def reward (self, block):
        block = 2165403
        return self.api.get_block_reward(block=block)
"""

class block():

    def __init__(self):
        self.url = 'https://api.etherscan.io/api?module=block'

    def _safeRequest(self, url):
        while True:
            try:
                response = requests.get(url)
            except Exception as e:
                print(f'Connection Failed: {e}. Reconnecting...')
                time.sleep(1)
            else:
                break
        resp = response.json()
        if response.status_code != 200:
            raise Exception(resp)
      
        return resp

    def get_reward(self, blockno, key):
        reward_url = '&action=getblockreward&blockno={}&apikey={}'.format(blockno, key)
        return pd.DataFrame(self._safeRequest(self.url + reward_url))

    def get_count(self, blockno, key):
        count_url = '&action=getblockcountdown&blockno={}&apikey={}'.format(blockno, key)
        return pd.DataFrame(self._safeRequest(self.url + count_url), index=[0])


class contracts():

    def __init__(self, key, address):
        # Setup api for single address
        self.key = key
        self.address = address
        self.api = Contract(address=address, api_key=key)

    def get_contract(self):
        # cannot transfer to DataFrame
        return self.api.get_abi()

    def get_sourcecode(self):
        sourcecode = self.api.get_sourcecode()
        return pd.DataFrame(sourcecode)

class proxies():

    def __init__(self, key):
        self.key = key
        self.api = Proxies(api_key = key)
    '''
    def gas_price(self):
        return self.api.gas_price()
    '''
    def get_block(self, number):
        block_number = self.api.get_block_by_number(block_number=number)
        return pd.DataFrame.from_dict(block_number, orient='index')

    def tx_count(self, number):
        tx_count=  self.api.get_block_transaction_count_by_number(block_number= number)
        tx_count=[int(tx_count, 16)]
        return pd.DataFrame(tx_count, ['block_transaction_count_by_number'])
    '''
    def get_code(self, address):
 Error: Exception has occurred: AttributeError. 'Proxies' object has no attribute 'get_code'
        return self.api.get_code(address = address)
    '''
    def get_recent_block(self):
        block = self.api.get_most_recent_block()
        block = [int(block, 16)]
        return pd.DataFrame(block, columns=['most recent block'])
    '''
    def value(self):
   Error: no attribute
        return self.api.get_storage_at('0x6e03d9cce9d60f3e9f2597e13cd4c54c55330cfd', 0x0)
    '''
    def Transactions(self, number, index):
        return self.api.get_transaction_by_blocknumber_index(block_number=number,
                                                       index=index)
                        
    def transaction_hash(self, TX_HASH):
        transaction = self.api.get_transaction_by_hash(tx_hash=TX_HASH)
        return transaction
    
    def transaction_count(self, address):
        count = self.api.get_transaction_count(address = address)
        count = [int(count, 16)]
        return count

    def transaction_receipt(self,TX_HASH):
        receipt = self.api.get_transaction_receipt(tx_hash=TX_HASH)
        return receipt

    def uncle(self, number, index):
        uncel = self.api.get_uncle_by_blocknumber_index(block_number=number,
                                                        index=index)
        return uncel

class stat():

    def __init__(self, key):
        self.key = key
        self.api = stats.Stats(api_key = key)

    def last_price(self):
        last_price = self.api.get_ether_last_price()
        return pd.DataFrame(last_price, index=[0])

    def total_supply(self):
        total_supply = self.api.get_total_ether_supply()
        return total_supply

class token():
    
    def __init__(self, key, contract_address):
        self.key = key
        self.contract_address = contract_address
        self.api = tokens.Tokens(contract_address=contract_address, api_key=key)

    def token_balance(self,address):
        token_balance = self.api.get_token_balance(address=address)
        return token_balance

    def total_supply(self):
        return self.api.get_total_supply()

"""
class transaction():      Error: No module named 'etherscan.transactions'

    def __init__(self, key):
        self.key = key
        self.api = stats.Stats(api_key = key)

    def status(self, TX_HASH):
        status = self.api.get_status(tx_hash=TX_HASH)
        return status

    def receipt_status(self, TX_HASH):
        receipt_status = self.api.get_tx_receipt_status(tx_hash=TX_HASH)
        return receipt_status
"""

class transaction():

    def __init__(self):
        self.url = 'https://api.etherscan.io/api?module=transaction'

    def _safeRequest(self, url):
        while True:
            try:
                response = requests.get(url)
            except Exception as e:
                print(f'Connection Failed: {e}. Reconnecting...')
                time.sleep(1)
            else:
                break
        resp = response.json()
        if response.status_code != 200:
            raise Exception(resp)
        return resp

    def get_status(self, txhash, key):
        status_url = '&action=getstatus&txhash={}&apikey={}'.format(txhash, key)
        return pd.DataFrame(self._safeRequest(self.url + status_url))

    def get_txreceipt_status(self, txhash, key):
        status_url = '&action=gettxreceiptstatus&txhash={}&apikey={}'.format(txhash, key)
        return pd.DataFrame(self._safeRequest(self.url + status_url))


DATA_DIR = '/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task3/'

with open('/Users/yihuihuang/Desktop/Crypto/caw-quant-training/section1/task3/api_key.json', mode='r') as key_file:
    key = json.loads(key_file.read())['key']

address = '0x9dd134d14d1e65f84b706d6f205cd5b1cd03a46b'

account = account(key=key, address = address)

balance = account.balance()
balance.to_csv(os.path.join(DATA_DIR, "account_balance.csv"), index=False)
