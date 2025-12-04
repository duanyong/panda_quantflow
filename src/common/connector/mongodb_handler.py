import pymongo
import urllib.parse
import sys

class DatabaseHandler:
    _instance = None
    DEFAULT_MONGO_DB = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseHandler, cls).__new__(cls)
        return cls._instance

    def __init__(self, config):
        # 如果已经初始化过，就直接返回，避免重复连接
        if hasattr(self, 'initialized') and self.initialized:
            return

        self.config = config
        self.DEFAULT_MONGO_DB = config['MONGO_DB']
        self.mongo_client = None  # 先确保属性存在并为 None

        try:
            # 1. 构建连接字符串
            encoded_password = urllib.parse.quote_plus(config["MONGO_PASSWORD"])
            mongo_uri = f'mongodb://{config["MONGO_USER"]}:{encoded_password}@{config["MONGO_URI"]}/{config["MONGO_DB"]}'
            
            # 2. 准备连接参数
            client_kwargs = {
                'readPreference': 'secondaryPreferred',
                'w': 'majority',
                'retryWrites': True,
                'socketTimeoutMS': 30000,
                'connectTimeoutMS': 20000,
                'serverSelectionTimeoutMS': 30000,
                'authSource': config["MONGO_AUTH_DB"],
            }

            if config['MONGO_TYPE'] == 'standalone':
                client_kwargs['directConnection'] = True
            elif config['MONGO_TYPE'] == 'replica_set':
                mongo_uri += f'?replicaSet={config["MONGO_REPLICA_SET"]}'
                client_kwargs['heartbeatFrequencyMS'] = 10000

            # 3. 打印屏蔽了密码的 URI，用于调试
            masked_uri = mongo_uri.replace(encoded_password, "****")
            print(f"Attempting to connect to MongoDB: {masked_uri}")

            # 4. 尝试连接并创建客户端
            self.mongo_client = pymongo.MongoClient(mongo_uri, **client_kwargs)

            # 5. 发送 ping 命令以验证连接
            self.mongo_client.admin.command('ping')
            
            print("MongoDB connection successful.")
            self.initialized = True

        except Exception as e:
            # 6. 如果以上任何一步失败，都会进入这里
            print(f"FATAL: MongoDB connection failed. Reason: {e}", file=sys.stderr)
            
            # 关闭可能已部分创建的连接
            if self.mongo_client:
                self.mongo_client.close()
            
            # 抛出异常，终止程序启动
            raise ConnectionError("Could not connect to MongoDB. Application cannot start.") from e

    def mongo_insert(self, db_name, collection_name, document):
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.insert_one(document).inserted_id

    def mongo_find(self, db_name, collection_name, query, hint=None, sort=None, projection=None):
        """
        Find documents in MongoDB collection

        Args:
            db_name: Database name
            collection_name: Collection name
            query: Query dictionary
            hint: Optional index hint
            sort: Optional sort specification
            projection: Optional projection (field selection)

        Returns:
            List of documents
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        cursor = collection.find(query, projection)  # Adding projection here
        if hint:
            cursor = cursor.hint(hint)
        if sort:
            cursor = cursor.sort(sort)
        return list(cursor)

    def mongo_update(self, db_name, collection_name, query, update):
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.update_many(query, {'$set': update}).modified_count

    def mongo_update_one(self, db_name, collection_name, query, update, upsert=False, **kwargs):
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.update_many(
            filter=query,
            update=update,
            upsert=upsert,
            **kwargs
        )

    def mongo_delete(self, db_name, collection_name, query):
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.delete_many(query).deleted_count

    def get_mongo_collection(self, db_name, collection_name):
        return self.mongo_client[db_name][collection_name]

    def get_mongo_db(self,db_name=DEFAULT_MONGO_DB or "panda"):
        return self.mongo_client[db_name]

    def mongo_insert_many(self, db_name, collection_name, documents):
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.insert_many(documents).inserted_ids

    def mongo_aggregate(self, db_name, collection_name, aggregation_pipeline):
        collection = self.get_mongo_collection(db_name, collection_name)
        return list(collection.aggregate(aggregation_pipeline)) 
    
    def get_distinct_values(self, db_name, collection_name, field):
        """Get distinct values for a field"""
        collection = self.get_mongo_collection(db_name, collection_name)
        return collection.distinct(field)

    def mongo_find_one(self, db_name, collection_name, query, hint=None, projection=None, sort=None):
        """
        Find a single document in MongoDB collection

        Args:
            db_name: Database name
            collection_name: Collection name
            query: Query dictionary
            hint: Optional index hint
            projection: Optional projection dictionary to specify fields to include/exclude
            sort: Optional sort specification

        Returns:
            Single document or None if not found
        """
        collection = self.get_mongo_collection(db_name, collection_name)
        find_args = {}

        if hint:
            find_args['hint'] = hint
        if projection:
            find_args['projection'] = projection
        if sort:
            find_args['sort'] = sort

        return collection.find_one(query, **find_args)