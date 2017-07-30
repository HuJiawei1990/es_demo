from .es_server import es_server
import logging

logging.basicConfig(filename='../logs/datageek.log',
                    format='%(asctime)s\t%(filename)s[%(lineno)d]\t%(levelname)s\t%(message)s',
                    datefmt='[%Y-%m_%d %H:%M:%S]',
                    level=logging.INFO)
