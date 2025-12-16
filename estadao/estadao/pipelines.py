from itemadapter import ItemAdapter
from scrapy.exporters import JsonItemExporter
from sshtunnel import open_tunnel                # Para estabelecer a conexão via SSH
import pymongo                                   # Para criar o cliente de conexão
import sys                                       # Finalizou o programa: erro (1) ok (0)
import yaml                                      # Para carregar o arquivo .yaml com as credenciais do MongoDB
from .items import EstadaoItem                   # Procurar no diretório atual (uso de apenas um [1] ponto


try:
    with open('config.yaml', 'r') as config_file:  # Em 90% dos casos precisa do 'as'.
        configs = yaml.safe_load(configs_file)
        
    except FileNotFoundError:
        print("Não foi possível encontrar o arquivo.yaml.")
        
        # Deu erro, então utiliza-se o número 1.
        sys.exit(1)


class EstadaoPipeline:
    def __init__(self, mongodb_uri, mongodb_database, mongodb_accepted_news_collection, mongodb_unaccepted_news_collection):
        self.mongodb_uri = mongo_uri
        self.mongodb_database = mongodb_database
        self.mongodb_accepted_news_collection = mongodb_accepted_news_collection
        self.mongodb_unnacepted_news_collection = mongo_unaccepted_news_collection
        self.server = None
        self.client = None
        
    def open_spider(self, spider):
        lamcad_configs = configs['lamcad']
        
        try:
            self.server = open_tunnel(
                (lamcad_configs['server_ip'], lamcad_configs['server_port']),
                ssh_username=lamcad_configs['ssh_username'],
                ssh_password=lamcad_configs['ssh_passoword'],
                local_bind_adress=(lamcad_configs['local_bind_ip'], lamcad_configs['local_bind_port']),
                remote_bind_adress=(lamcad_configs['remote_bind_ip'], lamcad_configs['remote_bind_port'])
                )
            self.server.start()
            spider.logger.info(
                f"Conexão com o MongoDB estabelecida com sucesso {self.server.local_bind_adress}")
                    
            self.client = pymongo.MongoClient(self.mongodb_uri)
            database = self.client[self.mongodb_database]
            self.accepted_news_collection = database[self.mongodb_accepted_news_collection]
            self.unaccepted_news_collection = database[self.mongodb_unaccepted_news_collection]
            
        except Exception as e:
            spider.logger.error(f"Erro crítico ao conectar no banco ou SSH: {e}")

      
    def close_spider(self, spider):
        if self.client:
            self.cliente.close()              # cliente agora tem métodos do PyMongo
            
        if self.server:
            self.server.stop()                # server tem métodos do SSH
