import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from panda_server.config.env import (
    MONGO_URI,
    DATABASE_NAME,
    RUN_MODE,
    MONGO_USER,
    MONGO_PASSWORD,
    MONGO_AUTH_DB,
    MONGO_TYPE,
    MONGO_REPLICA_SET,
)
from panda_server.config.mongodb_index_config import init_all_indexes

logger = logging.getLogger(__name__)

class MongoDB:
    """
    MongoDB 数据库连接管理类
    提供数据库连接、关闭和集合获取的功能
    """

    client: AsyncIOMotorClient = None
    db = None

    @classmethod
    async def connect_db(cls):
        """
        建立数据库连接，并验证连接有效性
        """
        logger.info("Attempting to connect to MongoDB...")

        client_kwargs = {
            "retryWrites": True,
            "w": "majority",
            "serverSelectionTimeoutMS": 5000,  # Faster timeout for server selection
        }

        if MONGO_TYPE == "standalone":
            client_kwargs["directConnection"] = True
        elif MONGO_TYPE == "replica_set":
            client_kwargs["replicaSet"] = MONGO_REPLICA_SET

        if MONGO_USER and MONGO_PASSWORD:
            client_kwargs["username"] = MONGO_USER
            client_kwargs["password"] = MONGO_PASSWORD
            client_kwargs["authSource"] = MONGO_AUTH_DB or DATABASE_NAME

        try:
            cls.client = AsyncIOMotorClient(MONGO_URI, **client_kwargs)
            cls.db = cls.client.get_database(DATABASE_NAME)
            
            # Ping the database to verify connection
            await asyncio.wait_for(cls.db.command("ping"), timeout=5)
            
            logger.info(f"Successfully connected to MongoDB, database: '{DATABASE_NAME}'")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            if cls.client:
                cls.client.close()
            raise Exception(f"MongoDB Connection Error: {e}")
        
        return cls.db

    @classmethod
    async def init_local_db(cls):
        """Initialize database indexes and other operations"""
        if cls.db is None:
            logger.warning("Database not connected, skipping initialization.")
            return
            
        logger.info(f"Current running environment: {RUN_MODE}")
        if RUN_MODE == "LOCAL":
            logger.info("Local environment, starting database index initialization...")
            try:
                await init_all_indexes(cls)
                logger.info("Database index initialization completed")
            except Exception as e:
                logger.error(f"Database index initialization failed: {e}")
        else:
            logger.info("Cloud environment, skipping database index initialization")
    
    @classmethod
    async def close_db(cls):
        """
        关闭数据库连接
        """
        if cls.client:
            cls.client.close()
            logger.info("MongoDB connection closed.")

    @classmethod
    def get_collection(cls, collection_name: str):
        """
        获取指定名称的集合
        """
        if cls.db is None:
            raise Exception("Database not connected. Call connect_db() first.")
        return cls.db[collection_name]

# 创建数据库连接实例
mongodb = MongoDB()
