from enum import Enum

from loguru import logger
from fastapi.responses import JSONResponse, FileResponse
from fastapi import FastAPI, Request, status, HTTPException
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException
from labelu.internal.common.config import settings

import pkg_resources
import os
import traceback

# common init error code
COMMON_INIT_CODE = 30000

# user init error code
USER_INIT_CODE = 40000

# task init error code
TASK_INIT_CODE = 50000

# export
EXPORT_INIT_CODE = 60000

UNEXPECTED_ERROR_CODE = 99999


class ErrorCode(Enum):
    """
    business error code
    """
    UNEXPECTED_ERROR = (
        UNEXPECTED_ERROR_CODE,
        "Internal Error",
    )

    # common init error code
    CODE_30000_SQL_ERROR = (
        COMMON_INIT_CODE,
        "Excute SQL Error",
    )
    CODE_30001_NO_PERMISSION = (
        COMMON_INIT_CODE + 1,
        "Forbidden, No permission",
    )
    CODE_30002_VALIDATION_ERROR = (
        COMMON_INIT_CODE + 2,
        "validation error for request",
    )
    CODE_30003_CLIENT_ERROR = (
        COMMON_INIT_CODE + 3,
        "Error request",
    )

    # user error code
    CODE_40000_USERNAME_OR_PASSWORD_INCORRECT = (
        USER_INIT_CODE,
        "Incorrect username or password",
    )
    CODE_40001_USERNAME_ALREADY_EXISTS = (
        USER_INIT_CODE + 1,
        "Username Already exists in the system",
    )
    CODE_40002_USER_NOT_FOUND = (USER_INIT_CODE + 2, "User not found")
    CODE_40003_CREDENTIAL_ERROR = (USER_INIT_CODE + 3, "Could not validate credentials")
    CODE_40004_NOT_AUTHENTICATED = (USER_INIT_CODE + 4, "Not authenticated")

    # task error code
    CODE_50000_TASK_ERROR = (TASK_INIT_CODE, "Internal Error")
    CODE_50001_TASK_FINISHED_ERROR = (TASK_INIT_CODE + 1, "Task is finished")
    CODE_50002_TASK_NOT_FOUND = (TASK_INIT_CODE + 2, "Task not found")
    CODE_50003_COLLABORATOR_ALREADY_EXISTS = (
        TASK_INIT_CODE + 3,
        "Collaborator already exists")

    CODE_50004_COLLABORATOR_NOT_FOUND = (
        TASK_INIT_CODE + 4,
        "Collaborator not found",
    )
    # task attachment error code
    CODE_51000_CREATE_ATTACHMENT_ERROR = (
        TASK_INIT_CODE + 1000,
        "Upload attachment error, save file failure",
    )
    CODE_51001_TASK_ATTACHMENT_NOT_FOUND = (
        TASK_INIT_CODE + 1001,
        "Attachment file not found",
    )

    CODE_51002_TASK_ATTACHMENT_ALREADY_EXISTS =(
        TASK_INIT_CODE + 1002,
        "Attachment file already exists",
    )
    # task sample error code
    CODE_55000_SAMPLE_LIST_PARAMETERS_ERROR = (
        TASK_INIT_CODE + 5000,
        "Paramenters error: 'after', 'before', 'page' only one must be Ture, page can be 0",
    )
    CODE_55001_SAMPLE_NOT_FOUND = (TASK_INIT_CODE + 5001, "Sample not found")
    CODE_55002_SAMPLE_FORMAT_ERROR = (
        TASK_INIT_CODE + 5002,
        "Sample result format error",
    )
    CODE_55002_SAMPLE_NAME_EXISTS = (
        TASK_INIT_CODE + 5003,
        "Sample name exists",
    )
    CODE_61000_NO_DATA = (
        EXPORT_INIT_CODE + 1000,
        "No data",
    )


class LabelUException(HTTPException):
    def __init__(
        self, code: ErrorCode, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    ):
        self.msg = code.value[1]
        self.code = code.value[0]
        self.status_code = status_code


async def labelu_exception_handler(request: Request, exc: LabelUException):
    logger.error(exc)
    return JSONResponse(
        status_code=exc.status_code,
        content={"msg": exc.msg, "err_code": exc.code},
    )


# customize http exception
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    logger.error(exc)
    if (
        exc.status_code == status.HTTP_404_NOT_FOUND
        and not request.url.path.startswith(settings.API_V1_STR)
    ):
        return FileResponse(
            os.path.join(
                pkg_resources.resource_filename('labelu.internal', 'statics'),
                'index.html'
                ),
            status_code=200,
            headers={'Content-Type': 'text/html', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive'}
            )
    elif exc.status_code == status.HTTP_403_FORBIDDEN:
        JSONResponse(
            status_code=exc.status_code,
            content={
                "msg": str(exc.detail),
                "err_code": ErrorCode.CODE_40004_NOT_AUTHENTICATED.value[0],
            },
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "msg": str(exc.detail),
            "err_code": ErrorCode.CODE_30003_CLIENT_ERROR.value[0],
        },
    )


# only for sqlalchemy
async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError):
    logger.error(exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "msg": str(exc),
            "err_code": ErrorCode.CODE_30000_SQL_ERROR.value[0],
        },
    )


# for http request
async def validation_exception_handler(request, exc):
    logger.error(exc)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "msg": str(exc),
            "err_code": ErrorCode.CODE_30002_VALIDATION_ERROR.value[0],
        },
    )

async def unexpected_exception_handler(request: Request, exc: Exception):
    logger.error(traceback.format_exc())
    logger.error(exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "msg": str(exc),
            "err_code": ErrorCode.UNEXPECTED_ERROR.value[0],
        },
    )

def add_exception_handler(app: FastAPI):
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(LabelUException, labelu_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(SQLAlchemyError, sqlalchemy_exception_handler)
    app.add_exception_handler(Exception, unexpected_exception_handler)
