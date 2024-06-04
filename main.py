from logging import getLogger, StreamHandler

from fastapi import FastAPI
from imfs.main import router as imfs_router
from v1.main import router as v1_router

app = FastAPI()

logger = getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel("INFO")


app.include_router(imfs_router, prefix="/imfs")
app.include_router(v1_router, prefix="/v1")
