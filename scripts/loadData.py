#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import declarative_base
from shapeData import shapeData

engine = create_engine("sqlite:///output/covid19-etl.sqlite")

Base = declarative_base()

class covidData(Base):
    __tablename__ = 'covidData'

    id                = Column(Integer, primary_key=True)
    country           = Column(String, nullable=False)
    cases             = Column(Integer, nullable=False)
    deaths            = Column(Integer, nullable=False)
    recovered         = Column(Integer, nullable=False)
    updated_utc       = Column(String, nullable=False)
    pipeline_run_date = Column(String, nullable=False)

Base.metadata.create_all(engine)

covidDF = shapeData()
covidDF.to_sql('covidData', con=engine, if_exists='append', index=False)
