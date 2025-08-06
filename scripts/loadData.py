#!/usr/bin/env python3

from sqlalchemy import Column, DateTime, Integer, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from shapeData import shapeData

engine = create_engine("sqlite:///output/covid19-etl.sqlite")

Base = declarative_base()

class covidData(Base):
    __tablename__ = 'covidData'

    id                = Column(Integer, primary_key=True)
    country           = Column(String, nullable=False, unique=True)
    cases             = Column(Integer, nullable=False)
    deaths            = Column(Integer, nullable=False)
    recovered         = Column(Integer, nullable=False)
    updated_utc       = Column(DateTime(timezone=True), nullable=False)
    pipeline_run_date = Column(DateTime(timezone=True), nullable=False, unique=True)

Base.metadata.create_all(engine)

covidDF = shapeData()
rows = [covidData(country = row['country'], cases=row['cases'], deaths=row['deaths'], recovered=row['recovered'], updated_utc=row['updated_utc'], pipeline_run_date=row['pipeline_run_date']) for _, row in covidDF.iterrows()]

Session = sessionmaker(bind=engine)
session = Session()


session.bulk_save_objects(rows)
session.commit()
