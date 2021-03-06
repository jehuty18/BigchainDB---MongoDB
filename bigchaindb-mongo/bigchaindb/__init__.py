import copy
import logging
# from functools import reduce
# PORT_NUMBER = reduce(lambda x, y: x * y, map(ord, 'BigchainDB')) % 2**16
# basically, the port number is 9984

#module for write/read from file
import pickle
from datetime import datetime
from bigchaindb import util
from bson import Timestamp

#variabile globale utile per salvare il timestamp da file
TIMESTAMP=None
#file descriptor per scrivere/leggere da file
fd=None

#inizializzo una istanza dell'oggetto di logging per tenere traccia della creazione del file .dat e della prima scrittura
logger = logging.getLogger(__name__)

#tento di accedere al file "timestamp.dat" aprendolo come file binario, se non esiste lo creo
try:
    fd=open("timestamp.dat","rb")
except:
    logger.info('creating the \'timestamp.dat\' file...')

    #con il "modo" 'wb' apro il file in scrittura, se non esiste lo crea automaticamente
    fd=open("timestamp.dat","wb")
    fd.close()

#tento di caricare il TIMESTAMP da file, se il file è vuoto vuol dire che non è mai stato utilizzato il
#bigchaindb, per cui prendo come riferimento il timestamp attuale
try:
    fd=open("timestamp.dat","rb")
    TIMESTAMP=pickle.load(fd)
except:
    logger.info('initializing the first timestamp of bigmongo...')

    #avendo creato il file in precedenza, nel try viene aperto in lettura prima di lanciare l'eccezione
    #per cui provvedo a chiuderlo preventivamente...
    fd.close()
    #...per poi aprirlo in scrittura
    fd=open("timestamp.dat","wb")
    #richiamo la funzione utcnow di datetime per ottenere il timestamp corrente...
    TIMESTAMP=Timestamp(datetime.utcnow(),1)
    #...e lo inserisco nel file
    pickle.dump(TIMESTAMP,fd)
#provvedo a chiudere il file
fd.close()


config = {
    'server': {
        # Note: this section supports all the Gunicorn settings:
        #       - http://docs.gunicorn.org/en/stable/settings.html
        'bind': 'localhost:9984',
        'workers': None, # if none, the value will be cpu_count * 2 + 1
        'threads': None, # if none, the value will be cpu_count * 2 + 1
    },
    'database': {
        'host': 'localhost',
        'port': 27017,
        'name': 'bigchain',
    },
    'keypair': {
        'public': None,
        'private': None,
    },
    'keyring': [],
    'statsd': {
        'host': 'localhost',
        'port': 8125,
        'rate': 0.01,
    },
    'api_endpoint': 'http://localhost:9984/api/v1',
    'consensus_plugin': 'default',
}

# We need to maintain a backup copy of the original config dict in case
# the user wants to reconfigure the node. Check ``bigchaindb.config_utils``
# for more info.
_config = copy.deepcopy(config)
from bigchaindb.core import Bigchain  # noqa
from bigchaindb.version import __version__  # noqa
