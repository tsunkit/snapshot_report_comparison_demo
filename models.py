from sqlalchemy import Column, String, Date, Numeric, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class RegulatoryReport(Base):
    __tablename__ = 'regulatory_report'
    
    report_date = Column(String, primary_key=True)
    reference_id = Column(String)
    bis_category = Column(String)
    bis_type = Column(String)
    guarantee_type = Column(String)
    business_type = Column(String)
    method_type = Column(String)
    formula = Column(String)
    exposure_amt = Column(Numeric)
    guaranteed_amt = Column(Numeric)
    risk_weight = Column(Numeric)
    exposure_amount = Column(Numeric)
    guarantee_amount = Column(Numeric)
    risk_amount = Column(Numeric)
    pre_calc_value = Column(Numeric)
    final_value = Column(Numeric)