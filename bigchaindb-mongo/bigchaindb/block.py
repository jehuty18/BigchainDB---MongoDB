import logging
import multiprocessing as mp
import queue

#uncheck to use rethinkdb
#import rethinkdb as r
import pymongo

import bigchaindb
from bigchaindb import Bigchain
from bigchaindb.monitor import Monitor


logger = logging.getLogger(__name__)


class Block(object):
    def __init__(self, q_new_transaction):
        self._q_new_transaction = q_new_transaction
        self.q_new_transaction = None
        self.q_tx_to_validate = mp.Queue()
        self.q_tx_validated = mp.Queue()
        self.q_tx_delete = mp.Queue()
        self.q_block = mp.Queue()
        self.initialized = mp.Event()
        self.monitor = Monitor()

    def filter_by_assignee(self):
        """
        Handle transactions that are assigned to me
        """

        # create a bigchain instance
        b = Bigchain()

        while True:
            tx = self.q_new_transaction.get()

            # poison pill
            if tx == 'stop':
                self.q_tx_to_validate.put('stop')
                return

            if tx['assignee'] == b.me:
                tx.pop('assignee')
                self.q_tx_to_validate.put(tx)

    def validate_transactions(self):
        """
        Checks if the incoming transactions are valid
        """

        # create a bigchain instance
        b = Bigchain()

        while True:
            self.monitor.gauge('tx_queue_gauge',
                               self.q_tx_to_validate.qsize(),
                               rate=bigchaindb.config['statsd']['rate'])
            tx = self.q_tx_to_validate.get()

            # poison pill
            if tx == 'stop':
                self.q_tx_delete.put('stop')
                self.q_tx_validated.put('stop')
                return

            self.q_tx_delete.put(tx['_id'])

            with self.monitor.timer('validate_transaction', rate=bigchaindb.config['statsd']['rate']):
                is_valid_transaction = b.is_valid_transaction(tx)

            if is_valid_transaction:
                self.q_tx_validated.put(tx)

    def create_blocks(self):
        """
        Create a block with valid transactions
        """

        # create a bigchain instance
        b = Bigchain()
        stop = False

        while True:

            # read up to 1000 transactions
            validated_transactions = []
            for i in range(1000):
                try:
                    tx = self.q_tx_validated.get(timeout=5)
                except queue.Empty:
                    break

                # poison pill
                if tx == 'stop':
                    stop = True
                    break

                validated_transactions.append(tx)

            # if there are no transactions skip block creation
            if validated_transactions:
                # create block
                block = b.create_block(validated_transactions)
                self.q_block.put(block)

            if stop:
                self.q_block.put('stop')
                return

    def write_blocks(self):
        """
        Write blocks to the bigchain
        """

        # create bigchain instance
        b = Bigchain()

        # Write blocks
        while True:
            block = self.q_block.get()

            # poison pill
            if block == 'stop':
                return

            with self.monitor.timer('write_block'):
                b.write_block(block)

    def delete_transactions(self):
        """
        Delete transactions from the backlog
        """
        # create bigchain instance
        b = Bigchain()
        client=pymongo.MongoClient()
        backlog=client.bigchain.backlog
        stop = False

        while True:
            # try to delete in batch to reduce io
            tx_to_delete = []
            for i in range(1000):
                try:
                    tx = self.q_tx_delete.get(timeout=5)
                except queue.Empty:
                    break

                # poison pill
                if tx == 'stop':
                    stop = True
                    break
                tx_to_delete.append(tx)

            if len(tx_to_delete)>0:
                #r.table('backlog').get_all(*tx_to_delete).delete(durability='soft').run(b.conn)
                for transaction in tx_to_delete:
                    backlog.remove({'_id' : transaction})
            if stop:
                return


    def bootstrap(self):
        """
        Get transactions from the backlog that may have been assigned to this while it was
        online (not listening to the changefeed)
        """
        # create bigchain instance
        b = Bigchain()
        #inizializzo il client mongo
        client=pymongo.MongoClient()
        #indirizzo la collection backlog
        backlog=client.bigchain.backlog

        # create a queue to store initial results
        q_initial = mp.Queue()

        # get initial results
        #initial_results = r.table('backlog').between([b.me, r.minval], [b.me, r.maxval], index='assignee__transaction_timestamp').
                            #order_by(index=r.asc('assignee__transaction_timestamp')).run(b.conn)
        '''
        Nel momento in cui avviene il bootstrap e quindi l'accensione del bigchaindb viene analizzata la backlog per 
        rilevare tutte le transazioni non processate che sono state inserite in coda in attesa di essere processate 
        mentre il bigchaindb non era online.

        Il porting da rethinkdb può avvenire con un semplice cursore che recupera tutti i documenti da backlog
        ordinati secondo l'indice stabilito in 'db/utils.py'
        '''

        initial_results=[]
        for tx in backlog.find( { 
            "$and":[ 
                { "assignee": { "$lte": b.me } } , 
                { "assignee": { "$gt": b.me } } 
                ] } ).sort("assignee",pymongo.ASCENDING):
        
            initial_results.append(tx)


        # add results to the queue
        for result in initial_results:
            q_initial.put(result)
        q_initial.put('stop')

        return q_initial



    def start(self):
        """
        Bootstrap and start the processes
        """
        logger.info('bootstraping block module...')
        self.q_new_transaction = self.bootstrap()
        logger.info('finished reading past transactions')
        self._start()
        logger.info('finished bootstraping block module...')

        logger.info('starting block module...')
        self.q_new_transaction = self._q_new_transaction

        # signal initialization complete
        self.initialized.set()

        self._start()
        logger.info('exiting block module...')

    def _start(self):
        """
        Initialize, spawn, and start the processes
        """

        # initialize the processes
        p_filter = mp.Process(name='filter_transactions', target=self.filter_by_assignee)
        p_validate = mp.Process(name='validate_transactions', target=self.validate_transactions)
        p_blocks = mp.Process(name='create_blocks', target=self.create_blocks)
        p_write = mp.Process(name='write_blocks', target=self.write_blocks)
        p_delete = mp.Process(name='delete_transactions', target=self.delete_transactions)

        # start the processes
        p_filter.start()
        p_validate.start()
        p_blocks.start()
        p_write.start()
        p_delete.start()

        # join processes
        p_filter.join()
        p_validate.join()
        p_blocks.join()
        p_write.join()
        p_delete.join()