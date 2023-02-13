from sqlalchemy.orm import Session

from . import models, schemas


class SegementRepo:
    def create(db: Session, segement: schemas.SegementInfo):
        db_item = models.SegmentInformation(dag_run_id=segement.dag_run_id, actual_rule=segement.actual_rule,
                                            campaign_type=segement.campaign_type,
                                            current_arpu=segement.current_arpu, predicted_arpu=segement.predicted_arpu,
                                            uplift=segement.uplift, incremental_revenue=segement.incremental_revenue,
                                            rule=segement.rule, samples=segement.samples,
                                            campaign_name=segement.campaign_name,
                                            recommended_product_id=segement.recommended_product_id,
                                            recommended_product_name=segement.recommended_product_name,
                                            current_product=segement.current_product,
                                            current_products_names=segement.current_products_names,
                                            segment_length=segement.segment_length,
                                            current_ARPU_band=segement.current_ARPU_band,
                                            current_revenue_impact=segement.current_revenue_impact,
                                            customer_status=segement.customer_status,
                                            segment_name=segement.segment_name, query=segement.query,
                                            cluster_no=segement.cluster_no,confidence= segement.confidence)
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item

    def findByAutoPilotIdAndClusterNo(db: Session, _id, cluster_no):
        return db.query(models.SegmentInformation) \
            .filter(models.SegmentInformation.dag_run_id == _id) \
            .filter(models.SegmentInformation.cluster_no == cluster_no) \
            .all()

    def findByAutoPilotIdAndSegementName(db: Session, _id, segement_name, cluster_number):
        return db.query(models.SegmentInformation) \
            .filter(models.SegmentInformation.dag_run_id == _id) \
            .filter(models.SegmentInformation.segment_name == segement_name) \
            .filter(models.SegmentInformation.cluster_no == cluster_number).first()

    def findByAutoPilotId(db: Session, _id):
        return db.query(models.SegmentInformation).filter(models.SegmentInformation.dag_run_id == _id).all()

    def deleteById(db: Session, _ids):
        for id in _ids:
            db.query(models.SegmentInformation).filter(models.SegmentInformation.id == id).delete()
            db.commit()

    def update(db: Session, item_data):
        updated_item = db.merge(item_data)
        db.commit()
        return updated_item


class AssociationRepo:
    def create(db: Session, info: schemas.AssociationInfo):
        db_item = models.AssociationInfo(dag_run_id=info.dag_run_id, lift=info.lift,
                                         service_type=info.service_type,
                                         current_pack=info.current_pack, type_info=info.type_info,
                                         confidence=info.confidence,
                                         number_of_current_packs=info.number_of_current_packs,
                                         support=info.support, recommended_pack=info.recommended_pack,
                                         segement_name=info.segement_name
                                         )
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item


class StatsRepo:
    async def create(db: Session, stat: schemas.StatsCreate):
        db_item = models.Stats(dag_run_id=stat.dag_run_id, samples=stat.samples, accuracy=stat.accuracy,
                               precision=stat.precision)
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item


class UserInfoRepo:
    async def create(db: Session, stat: schemas.User):
        db_item = models.UserInfo(email=stat.email, samples=stat.count, )
        db.add(db_item)
        db.commit()
        db.refresh(db_item)
        return db_item

    def findByEmail(db: Session, _id):
        return db.query(models.UserInfo).filter(models.UserInfo.email == _id).first()
