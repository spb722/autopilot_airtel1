from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime,Boolean
from sqlalchemy.orm import relationship
import datetime
from db import Base
from sqlalchemy.dialects.mysql import LONGTEXT


# class Item(Base):
#     __tablename__ = "items"

#     id = Column(Integer, primary_key=True, index=True)
#     name = Column(String(80), nullable=False, unique=True, index=True)
#     price = Column(Float(precision=2), nullable=False)
#     description = Column(String(200))
#     store_id = Column(Integer, ForeignKey('stores.id'), nullable=False)

#     def __repr__(self):
#         return 'ItemModel(name=%s, price=%s,store_id=%s)' % (self.name, self.price, self.store_id)


class Stats(Base):
    __tablename__ = "stats"
    id = Column(Integer, primary_key=True, index=True)
    dag_run_id = Column(String(80), nullable=False, unique=True)
    samples = Column(String(80), nullable=False, unique=True)
    accuracy = Column(String(80), nullable=False, unique=True)
    recall = Column(String(80), nullable=False, unique=True)
    precision = Column(String(80), nullable=False, unique=True)

#
# class FlowInfo(Base):
#     __tablename__ = "flow_information"
#     id = Column(Integer, primary_key=True, index=True)
#     dag_run_id = Column(String(80), nullable=False, unique=True)
#     segement_name = Column(String(80), nullable=False, unique=True)
#     segement_path = Column(String(80), nullable=False, unique=True)

class ServiceInfo(Base):
    __tablename__ = 'service_info'
    id = Column(Integer, primary_key=True, index=True)
    trend = Column(String(80), nullable=True, unique=False)
    rfm = Column(String(80), nullable=True, unique=False)
    Upsell = Column(Boolean, nullable=True, unique=False)
    crossell = Column(Boolean, nullable=True, unique=False)
    churn = Column(Boolean, nullable=True, unique=False)
    nbo = Column(Boolean, nullable=True, unique=False)
    inactive = Column(Boolean, nullable=True, unique=False)
class SegmentInformation(Base):
    __tablename__ = "segment_information_new_7"
    id = Column(Integer, primary_key=True, index=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    end_date = Column(DateTime)
    current_product = Column(String(80), nullable=True, unique=False)
    current_products_names = Column(String(200), nullable=True, unique=False)
    recommended_product_id = Column(String(80), nullable=True, unique=False)
    recommended_product_name = Column(String(80), nullable=True, unique=False)
    predicted_arpu = Column(Integer, nullable=True)
    current_arpu = Column(Integer, nullable=True)
    segment_length = Column(String(80), nullable=True, unique=False)
    rule = Column(LONGTEXT, nullable=True)
    actual_rule = Column(LONGTEXT, nullable=True)
    uplift = Column(Float(precision=2), nullable=True)
    incremental_revenue = Column(Float(precision=2), nullable=True)
    campaign_type = Column(String(80), nullable=True, unique=False)
    campaign_name = Column(String(80), nullable=True, unique=False)
    action_key = Column(String(80), nullable=True, unique=False)
    robox_id = Column(String(80), nullable=True, unique=False)
    dag_run_id = Column(String(80), nullable=True, unique=False)
    samples = Column(Integer, nullable=False)
    segment_name = Column(String(80), nullable=True, unique=False)
    current_ARPU_band = Column(String(80), nullable=True, unique=False)
    current_revenue_impact = Column(String(80), nullable=True, unique=False)
    customer_status = Column(String(80), nullable=True, unique=False)
    query = Column(LONGTEXT, nullable=True, unique=False)
    cluster_no = Column(Integer, nullable=True)
    confidence = Column(Float(precision=2), nullable=True)
    recommendation_type = Column(String(80), nullable=True, unique=False)

    def __repr__(self):
        return 'SegmentInformation(name=%s)' % self.name


class UserInfo(Base):
    __tablename__ = "USER_INFO"
    id = Column(Integer, primary_key=True, index=True)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    email = Column(String(80), nullable=True, unique=False)
    count = Column(Integer, nullable=True)
    current_arpu = Column(Integer, nullable=True)


class AssociationInfo(Base):
    __tablename__ = "ASSOCIATION_INFO"
    id = Column(Integer, primary_key=True, index=True)
    dag_run_id = Column(String(80), nullable=False)
    created_date = Column(DateTime, default=datetime.datetime.utcnow)
    current_pack_ids=Column(String(80), nullable=True, unique=False)
    current_pack=Column(String(200), nullable=True, unique=False)
    number_of_current_packs = Column(Integer, nullable=True)
    recommended_pack = Column(String(80), nullable=True, unique=False)
    recommended_pack_id=Column(String(80), nullable=True, unique=False)
    support = Column(Float(precision=2), nullable=True)
    lift = Column(Float(precision=2), nullable=True)
    confidence = Column(Float(precision=2), nullable=True)
    service_type = Column(String(80), nullable=True, unique=False)
    type_info = Column(String(80), nullable=True, unique=False)
    segement_name = Column(String(80), nullable=True, unique=False)
    recommendation_type = Column(String(80), nullable=True, unique=False)
