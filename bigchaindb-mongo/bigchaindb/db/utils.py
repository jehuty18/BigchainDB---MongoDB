"""Utils to initialize and drop the database."""

import logging

#uncheck this to use rethinkdb
#import rethinkdb as r
import pymongo

import bigchaindb
from bigchaindb import exceptions


logger = logging.getLogger(__name__)


def get_conn():
    '''Get the connection to the database.'''

    '''return r.connect(bigchaindb.config['database']['host'],
                     bigchaindb.config['database']['port'])
    '''
    client=pymongo.MongoClient(bigchaindb.config['database']['host'],bigchaindb.config['database']['port'])
    return client    

def init():
    # Try to access the keypair, throws an exception if it does not exist
    b = bigchaindb.Bigchain()

    conn = get_conn()
    dbname = bigchaindb.config['database']['name']

    #uncheck these rows to use rethinkdb
    #if r.db_list().contains(dbname).run(conn):
    #controllo se il nome del db (bigchain in questo caso ndr.) è già stato creato ed utilizzato
    #nel caso esistesse viene lanciata una eccezione, altrimenti si prosegue con l'inizializzazione del database
    if dbname in conn.database_names():
        raise exceptions.DatabaseAlreadyExists('Database `{}` already exists'.format(dbname))

    logger.info('Create:')
    logger.info(' - database `%s`', dbname)
    #uncheck this to use rethinkdb function
    #r.db_create(dbname).run(conn)
    #in mongodb per creare un db bisogna prima instanziare almeno una collection
    #dunque procedo con l'indirizzamento del database ed in seguito ne creo le
    #collection che mi interesseranno
    bigchain=conn.get_database('bigchain')

    logger.info(' - tables')
    # create the tables
    #uncheck these rows to use rethinkdb functions
    #r.db(dbname).table_create('bigchain').run(conn)
    #r.db(dbname).table_create('backlog').run(conn)
    #creo le collections di bigchain (backlog e bigchain)
    bigchain.create_collection('bigchain')
    bigchain.create_collection('backlog')

    #dopo aver creato le collections procedo con la creazione degli indici

    logger.info(' - indexes')
    # create the secondary indexes
    '''
    # to order blocks by timestamp    
    r.db(dbname).table('bigchain').index_create('block_timestamp', r.row['block']['timestamp']).run(conn)
    # to order blocks by block number
    r.db(dbname).table('bigchain').index_create('block_number', r.row['block']['block_number']).run(conn)
    # to order transactions by timestamp
    r.db(dbname).table('backlog').index_create('transaction_timestamp', r.row['transaction']['timestamp']).run(conn)
    # to query the bigchain for a transaction id
    r.db(dbname).table('bigchain').index_create('transaction_id',
                                                r.row['block']['transactions']['id'], multi=True).run(conn)
    # compound index to read transactions from the backlog per assignee
    r.db(dbname).table('backlog')\
        .index_create('assignee__transaction_timestamp', [r.row['assignee'], r.row['transaction']['timestamp']])\
        .run(conn)
    # secondary index for payload hash
    r.db(dbname).table('bigchain')\
        .index_create('payload_hash', r.row['block']['transactions']['transaction']['data']['hash'], multi=True)\
        .run(conn)
    '''

    #inserire qui il codice appropriato da sopra
    #indice per l'ordinamento dei blocchi per timestamp
    index1=pymongo.IndexModel([("block.timestamp",pymongo.ASCENDING)],name="block_timestamp")
    bigchain.bigchain.create_indexes([index1])
    #indice per l'ordinamento dei blocchi per block number
    index2=pymongo.IndexModel([("block.block_number",pymongo.ASCENDING)],name="block_number")
    bigchain.bigchain.create_indexes([index2])
    #indice per l'ordinamento delle transazioni per timestamp
    index3=pymongo.IndexModel([("transaction.timestamp",pymongo.ASCENDING)],name="transaction_timestamp")
    bigchain.backlog.create_indexes([index3])
    #indice per la call a bigchain per transaction_id
    '''<--da controllare bene-->
    dopo aver fatto controlli e accertamenti sul codice testandolo a parte
    questo dovrebbe andare bene (mongodb crea indici per ogni elemento dell'array 
    che matchano con la query)'''
    index4=pymongo.IndexModel([("block.transactions._id",pymongo.ASCENDING)],name="transaction_id")
    bigchain.bigchain.create_indexes([index4])
    #indice composto per la lettura di transazioni da backlog per assegnamento
    index5=pymongo.IndexModel([("assignee",pymongo.ASCENDING),("transaction.timestamp",pymongo.ASCENDING)],name="assignee__transaction_timestamp")
    bigchain.backlog.create_indexes([index5])
    #per ultimo, indice secondario per il payload hash
    '''<--da controllare bene-->
    come sopra, la creazione dell'indice sottostante non dovrebbe lanciare eccezioni
    per il funzionamento di mongodb'''
    index6=pymongo.IndexModel([("block.transactions.transaction.data.hash",pymongo.ASCENDING)],name="payload_hash")
    bigchain.bigchain.create_indexes([index6])


    # wait for rethinkdb to finish creating secondary indexes
    #r.db(dbname).table('backlog').index_wait().run(conn)
    #r.db(dbname).table('bigchain').index_wait().run(conn)
    #le funzioni sopra non sono necessarie in mongodb in quanto la creazione di indici secondari non avviene mai in background
    #se non specificato dall'amministratore stesso (opzione 'background:True')
    #a queste condizioni il database arresta qualsiasi esecuzione su quegli indici finché non vengono creati e validati

    logger.info(' - genesis block')
    b.create_genesis_block()
    logger.info('Done, have fun!')


def drop(assume_yes=False):
    conn = get_conn()
    dbname = bigchaindb.config['database']['name']

    if assume_yes:
        response = 'y'
    else:
        response = input('Do you want to drop `{}` database? [y/n]: '.format(dbname))

    if response == 'y':
        try:
            logger.info('Drop database `%s`', dbname)
            #r.db_drop(dbname).run(conn)
            #sostituita con la rispettiva in pymongo
            conn.drop_database(dbname)
            logger.info('Done.')
        #except r.ReqlOpFailedError:
        #sostituito dall'eccezione corrispondente in pymongo
        except pymongo.errors.InvalidOperation:
            raise exceptions.DatabaseDoesNotExist('Database `{}` does not exist'.format(dbname))
    else:
        logger.info('Drop aborted')