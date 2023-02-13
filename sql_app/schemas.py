from typing import List, Optional

from pydantic import BaseModel


class Stats(BaseModel):
    dag_run_id: str
    samples: str
    accuracy: str
    recall = str
    precision = str


class StatsCreate(Stats):
    pass


class AssociationInfo(BaseModel):
    dag_run_id: Optional[str] = None
    current_pack: Optional[str] = None
    number_of_current_packs: Optional[int] = None
    recommended_pack: Optional[str] = None
    support: Optional[float] = None
    lift: Optional[float] = None
    confidence: Optional[float] = None
    service_type: Optional[str] = None
    type_info: Optional[str] = None
    segement_name: Optional[str] = None



class SegementInfo(BaseModel):
    end_date: Optional[str] = None
    dag_run_id: Optional[str] = None
    current_product: Optional[str] = None
    current_products_names: Optional[str] = None
    recommended_product_id: Optional[str] = None
    recommended_product_name: Optional[str] = None
    predicted_arpu: Optional[int] = None
    current_arpu: Optional[int] = None
    segment_length: Optional[str] = None
    rule: Optional[str] = None
    actual_rule: Optional[str] = None
    uplift: Optional[float] = None
    incremental_revenue: Optional[float] = None
    campaign_type: Optional[str] = None
    campaign_name: Optional[str] = None
    action_key: Optional[str] = None
    robox_id: Optional[str] = None
    samples: Optional[int] = None
    segment_name: Optional[str] = None
    current_ARPU_band: Optional[str] = None
    current_revenue_impact: Optional[str] = None
    customer_status: Optional[str] = None
    query: Optional[str] = None
    cluster_no: Optional[int] = None
    confidence: Optional[float] = None


class CreateSegment(SegementInfo):
    pass


class FlowInfo(SegementInfo):
    dag_run_id: str
    segement_name: str
    segement_path: str


class SessionInfo(BaseModel):
    dag_run_id: str
    prev_month_loc: str
    current_month_loc: str
    processed_prev_data_loc: str
    processed_current_data_loc: str
    processed_prev_purchase_loc: str
    processed_current_purchasde_loc: str
    recall = str
    precision = str


class BaseResponse(BaseModel):
    statusCode: int
    status: str
    message: str


class User(BaseModel):
    email: str
    count: int


class CreateUser(User):
    pass


class Response(BaseModel):
    id: int
    count: int
