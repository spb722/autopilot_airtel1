from typing import List, Optional
import sys
import uvicorn
from fastapi import Depends, FastAPI, HTTPException

from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

import services.rfm_service as rfm
import services.preprocess_service as ps
import services.ml_service as ml
import services.operations as op
import dask

import pandas as pd

import services.feature_engineering_service as fes

# import services.test as test_service

import sql_app.models as models
import sql_app.schemas as schemas
from db import get_db, engine

dask.config.set({'temporary_directory': 'C:/Users/spb72/OneDrive/Documents/6d/etl_files/tmp'})

app = FastAPI(title="AUTOPILOT APPLICATION Version 1.1",
              description=" Swagger at 9000 and Sqlalchemy orm used",
              version="1.0.0", )
sys.setrecursionlimit(1500)
models.Base.metadata.create_all(bind=engine)


@app.exception_handler(Exception)
def validation_exception_handler(request, err):
    base_error_message = f"Failed to execute: {request.method}: {request.url}"
    return JSONResponse(status_code=400, content={"message": f"{base_error_message}. Detail: {err}"})


#
# @app.middleware("http")
# async def add_process_time_header(request, call_next):
#     print('inside middleware!')
#     start_time = time.time()
#     response = await call_next(request)
#     process_time = time.time() - start_time
#     response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
#     return response


@app.post('/usage_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def pre_process_transactions(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return fes.usage_process(dag_run_id)


@app.post('/recharge_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def recharge_process(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return fes.recharge_process(dag_run_id)


@app.post('/purchase_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def purchase_process(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return fes.purchase_process(dag_run_id)


@app.post('/rfm_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def rfm_process(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return rfm.rfm_process_quantile_method(dag_run_id)


@app.post('/status_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def status_process(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return fes.status_process(dag_run_id)


@app.post('/segmentation', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def status_process(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return fes.segementation(dag_run_id)


@app.post('/k_modes', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def k_modes(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ml.k_modes(dag_run_id)


@app.post('/feature_selection', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def feature_selection(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ml.feature_selection(dag_run_id)


@app.post('/rule_extraction', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def rule_extraction(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ml.rule_extraction(dag_run_id, db)


@app.post('/matrix_filter', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def matrix_filter(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ml.matrix_filter(dag_run_id)


@app.post('/association_process', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def status_process(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ml.association_process(dag_run_id, db)


@app.post('/pre_process_one', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def pre_process_transactions(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return ps.pre_process(dag_run_id)


@app.post('/matrix_operations', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def matrix_operations(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return op.matrix_operations(dag_run_id)


@app.post('/matrix_operations_one', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def matrix_operations(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return op.matrix_operations_one(dag_run_id)


@app.post('/matrix_operations_two', tags=["auto_pilot"], response_model=schemas.BaseResponse, status_code=201)
async def matrix_operations(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return op.matrix_operations_two(dag_run_id)


@app.post('/data_preprocess_next_purchase', tags=["NEXT_PURCHASE"], response_model=schemas.BaseResponse,
          status_code=201)
async def data_preprocess_next_purchase(dag_run_id: Optional[str] = None):
    """
    read the datas and preprocess it
    """
    dag_run_id = dag_run_id[0:27]

    print("dag_run_id is ", dag_run_id)

    return nps.data_preprocess_next_purchase(dag_run_id)


#
# @app.post('/matrix_operations', tags=["NEXT_PURCHASE"], response_model=schemas.BaseResponse, status_code=201)
# async def matrix_operations(dag_run_id: Optional[str] = None):
#     """
#     read the datas and preprocess it
#     """
#     dag_run_id = dag_run_id[0:27]
#
#     print("dag_run_id is ", dag_run_id)
#
#     return op.matrix_operations(dag_run_id)


# @app.post('/readPreprocess', tags=["RFM"], response_model=schemas.BaseResponse, status_code=201)
# async def read_preprocess(dag_run_id: Optional[str] = None):
#     """
#     read the datas and preprocess it
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)

#     return rfm.read_preprocess(dag_run_id)


# @app.post('/recency', tags=["RFM"], response_model=schemas.BaseResponse, status_code=201)
# async def find_recency(dag_run_id: Optional[str] = None):
#     """
#     find recency from the purchase
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)

#     return rfm.find_recency(dag_run_id)


# @app.post('/frequency', tags=["RFM"], response_model=schemas.BaseResponse, status_code=201)
# async def find_frequency(dag_run_id: Optional[str] = None):
#     """
#     find frequency from the purchase
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)
#     return rfm.find_frequency(dag_run_id)


# @app.post('/monetary', tags=["RFM"], response_model=schemas.BaseResponse, status_code=201)
# async def find_monetary(dag_run_id: Optional[str] = None):
#     """
#     find monetary from the purchase
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)

#     return rfm.find_monetary(dag_run_id)


# @app.post('/clustering', tags=["RFM"], response_model=schemas.BaseResponse, status_code=201)
# async def clustering(dag_run_id: Optional[str] = None):
#     """
#     form hig mid low dataset
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)
#     return rfm.clustering(dag_run_id)


# @app.post('/fpGrowth', tags=["UPSELL_CROSSELL"], response_model=schemas.BaseResponse, status_code=201)
# async def fp_growth(dag_run_id: Optional[str] = None, filename: Optional[str] = None):
#     """
#    performing fp growth algoritm to form associations
#     """
#     print("dag_run_id is ", dag_run_id)
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)
#     print(f"the file_name is {filename}")
#     return fp_service.fp_growth(dag_run_id, filename)


# @app.post('/upsell', tags=["UPSELL_CROSSELL"], response_model=schemas.BaseResponse, status_code=201)
# async def form_upsell_cross_sell(dag_run_id: Optional[str] = None, filename: Optional[str] = None,
#                                  db: Session = Depends(get_db)):
#     """
#     getting upsella nd cross sell from associations
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)
#     print(f"the file_name is {filename}")
#     return fp_service.form_upsell_cross_sell(dag_run_id, db)


# @app.post('/final_preprocess', tags=["UPSELL_CROSSELL"], response_model=schemas.BaseResponse, status_code=201)
# async def final_preprocess(dag_run_id: Optional[str] = None):
#     """
#     final filtering of the assocoations
#     """
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)
#     return fp_service.final_preprocess(dag_run_id)


# @app.post('/data_preprocess', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def data_preprocess(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)

#     """
#     Create an Item and store it in the database
#     """

#     return ml_service.data_preprocess(dag_run_id, db)


# @app.post('/data_preprocess_non_sub', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def data_preprocess_non_sub(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     dag_run_id = dag_run_id[0:27]

#     print("dag_run_id is ", dag_run_id)

#     """
#     Create an Item and store it in the database
#     """

#     return ml_service.data_preprocess_non_sub(dag_run_id, db)


# @app.post('/data_formation', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def data_formation(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     # dag_run_id = dag_run_id[1:len(dag_run_id) - 1]

#     dag_run_id = dag_run_id[0:27]
#     """
#     Create an Item and store it in the database
#     """

#     return ml_service.data_formation(dag_run_id, db)


# @app.post('/rule_extraction', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def rule_extraction(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     # dag_run_id = dag_run_id[1:len(dag_run_id) - 1]

#     dag_run_id = dag_run_id[0:27]
#     """
#     Create an Item and store it in the database
#     """
#     print("the dag run id is ,", dag_run_id)

#     return ml_service.rule_extraction(dag_run_id, db)


# @app.post('/filter_rules', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def filter_rules(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     # dag_run_id = dag_run_id[1:len(dag_run_id) - 1]

#     dag_run_id = dag_run_id[0:27]
#     """
#     Create an Item and store it in the database
#     """
#     print("the dag run id is ,", dag_run_id)

#     # return schemas.BaseResponse(statusCode=200, message=" filter_rules", status="success")

#     return ml_service.filter_rules(dag_run_id, db)


# @app.post('/test', tags=["ML"], response_model=schemas.BaseResponse, status_code=201)
# async def filter_rules(dag_run_id: Optional[str] = None, db: Session = Depends(get_db)):
#     # dag_run_id = dag_run_id[1:len(dag_run_id) - 1]

#     # dag_run_id = 'manual__2022-07-20T06:55:36'
#     dag_run_id = dag_run_id[0:27]
#     """
#     Create an Item and store it in the database
#     """
#     print("the dag run id is ,", dag_run_id)

#     return ml_service.alter_rules(dag_run_id, db)


if __name__ == "__main__":
    uvicorn.run("main:app", port=9000, host="0.0.0.0", reload=True)
