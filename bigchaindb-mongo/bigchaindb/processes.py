import logging
import multiprocessing as mp

#uncheck this to use rethinkdb
#import rethinkdb as r
import pymongo
#modulo per inserimento/recupero da file
import pickle
from datetime import datetime
from bson import Timestamp

import bigchaindb
from bigchaindb import Bigchain
from bigchaindb.voter import Voter
from bigchaindb.block import Block
from bigchaindb.web import server


logger = logging.getLogger(__name__)

BANNER = """
****************************************************************************
*                                                                          *
*   Initialization complete. BigchainDB is ready and waiting for events.   *
*   You can send events through the API documented at:                     *
*    - http://docs.bigchaindb.apiary.io/                                   *
*                                                                          *
*   Listening to client connections on: {:<15}                    *
*                                                                          *
****************************************************************************
"""

class Processes(object):

    def __init__(self):
        # initialize the class
        self.q_new_block = mp.Queue()
        self.q_new_transaction = mp.Queue()

    def map_backlog(self):
        # listen to changes on the backlog and redirect the changes
        # to the correct queues

        # create a bigchain instance
        b = Bigchain()

        #uncheck this to use rethinkdb
        '''
        for change in r.table('backlog').changes().run(b.conn):

            # insert
            if change['old_val'] is None:
                self.q_new_transaction.put(change['new_val'])

            # delete
            if change['new_val'] is None:
                pass

            # update
            if change['new_val'] is not None and change['old_val'] is not None:
                pass
        '''
        #inserisco il codice mongo di seguito
        #inizializzo la connessione tramite la variabile client
        client=pymongo.MongoClient('localhost',27017)
        #inizializzo l'indirizzamento verso la collection bigchain
        bigchain=client.bigchain.bigchain
        #inizializzo l'indirizzamento verso la collection oplog
        oplog=client.local.oplog.rs

        """
        #creo un cursore contenente i blocchi ordinati dall'ultimo inserito al primo
        #questo cursore mi serve per scorrere i blocchi nel caso in cui non fossero ancora stati validati
        blocco=bigchain.find().sort('block_number', pymongo.DESCENDING).limit(-1)
        #...estraggo il timestamp del blocco valido...
        ts=blocco[0]['block']['timestamp']
        """

        #piuttosto che eseguire una dispendiosa quanto inutile "find().sort()" recupero l'ultimo timestamp
        #dal file .dat con cui effettuare i confronti in oplog
        fd=open("timestamp.dat","rb")
        ts=pickle.load(fd)
        fd.close()

        #...per effettuare la ricerca in oplog delle operazioni effettuate in backlog
        cursor = oplog.find({'ts': {'$gt': ts},'ns':'bigchain.backlog'}, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for change in cursor:
                #insert
                if change['op'] == 'i':
                    self.q_new_transaction.put(change['o'])
                    #segno l'ultima transazione processata
                    #se il nodo va in down l'elaborazione dovrebbe riprendere da questo ultimo timestamp
                    fd=open("timestamp.dat","wb")
                    pickle.dump(Timestamp(datetime.utcnow(),1),fd)
                    fd.close()

                #delete e update
                else:
                    pass

    def map_bigchain(self):
        # listen to changes on the bigchain and redirect the changes
        # to the correct queues

        # create a bigchain instance
        b = Bigchain()
        '''
        for change in r.table('bigchain').changes().run(b.conn):

            # insert
            if change['old_val'] is None:
                self.q_new_block.put(change['new_val'])

            # delete
            elif change['new_val'] is None:
                pass

            # update
            elif change['new_val'] is not None and change['old_val'] is not None:
                pass
        '''
        #inserisco il codice mongo di seguito
        #inizializzo la connessione tramite la variabile client
        client=pymongo.MongoClient('localhost',27017)
        #inizializzo l'indirizzamento verso la collection bigchain
        bigchain=client.bigchain.bigchain
        backlog=client.bigchain.backlog
        #inizializzo l'indirizzamento verso la collection oplog
        oplog=client.local.oplog.rs

        """
        QUESTO CODICE E' DA RIVEDERE CONSIDERANDO L'UTILIZZO DI UNA VARIABILE GLOBALE IN CUI SALVARE 
        L'ULTIMO TIMESTAMP UTILIZZATO
        #individuo il timestamp di new primary, che corrisponde ad una nuova connessione avvenuta al database di mongo
        #in modo tale da avere un termine 'ts' di paragone valido con cui andare ad effettuare la ricerca in bigchain
        primary=oplog.find({'ns': " "}).sort('ts', pymongo.DESCENDING)
        for i in primary: 
            if i['o']['msg']=='new primary'
            #assegno il timestamp alla variabile ts
            ts=i['ts']
            break
        QUESTO CODICE E' DA RIVEDERE CONSIDERANDO L'UTILIZZO DI UNA VARIABILE GLOBALE IN CUI SALVARE 
        L'ULTIMO TIMESTAMP UTILIZZATO
        """

        #piuttosto che eseguire una dispendiosa quanto inutile "find().sort()" recupero l'ultimo timestamp
        #dal file .dat con cui effettuare i confronti in oplog
        fd=open("timestamp.dat","rb")
        ts=pickle.load(fd)
        fd.close()

        #creo il cursore per la ricerca in bigchain dei documenti inseriti per poter essere processati
        cursor = oplog.find({'ts': {'$gte': ts},'ns':'bigchain.bigchain'}, cursor_type=pymongo.CursorType.TAILABLE_AWAIT, oplog_replay=True)
        while cursor.alive:
            for change in cursor:
                #insert
                if change['op'] == 'i':
                    self.q_new_block.put(change['o'])
                #delete e update
                else:
                    pass


    def start(self):
        logger.info('Initializing BigchainDB...')

        # instantiate block and voter
        block = Block(self.q_new_transaction)

        # start the web api
        app_server = server.create_server(bigchaindb.config['server'])
        p_webapi = mp.Process(name='webapi', target=app_server.run)
        p_webapi.start()

        # initialize the processes
        p_map_bigchain = mp.Process(name='bigchain_mapper', target=self.map_bigchain)
        p_map_backlog = mp.Process(name='backlog_mapper', target=self.map_backlog)
        p_block = mp.Process(name='block', target=block.start)
        p_voter = Voter(self.q_new_block)

        # start the processes
        logger.info('starting bigchain mapper')
        p_map_bigchain.start()
        logger.info('starting backlog mapper')
        p_map_backlog.start()
        logger.info('starting block')
        p_block.start()

        logger.info('starting voter')
        p_voter.start()

        # start message
        block.initialized.wait()
        p_voter.initialized.wait()
        logger.info(BANNER.format(bigchaindb.config['server']['bind']))