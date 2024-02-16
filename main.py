from logging import getLogger, StreamHandler

from fastapi import FastAPI
from imfs.main import router as imfs_router

app = FastAPI()

logger = getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel("INFO")


app.include_router(imfs_router, prefix="/imfs")
