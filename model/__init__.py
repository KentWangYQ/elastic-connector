import config
import motor.motor_asyncio

_mongo_client = motor.motor_asyncio.AsyncIOMotorClient(config.CONFIG.MONGO.get('uri'))
_db = _mongo_client.get_database()

merchant = _db.merchants
impression_track = _db.impressiontracks
act_share_detail = _db.actsharedetails
