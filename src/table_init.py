from sqlalchemy import create_engine, Column, String, Integer, Boolean, DateTime, text, Float
from sqlalchemy.dialects.mysql import TEXT
from sqlalchemy.ext.declarative import declarative_base

import logging as logger

from settings import PSQL_DB_URI

Base = declarative_base()
engine = create_engine(url=PSQL_DB_URI, echo=False)


class Contact(Base):
    __tablename__ = "contact"
    ContactID = Column(String(50), primary_key=True)
    Company = Column(String(250))
    City = Column(String(50))
    Country = Column(String(50))
    Title = Column(String(250))
    Industry = Column(String(50))
    AnnualRevenue = Column(Float)
    EmailDomain = Column(String(50))
    CreatedAt = Column(DateTime(timezone=True))
    UpdatedAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Account(Base):
    __tablename__ = "account"
    CompanyID = Column(String(50), primary_key=True)
    Company = Column(String(100))
    Country = Column(String(100))
    Address = Column(String(100))
    City = Column(String(100))
    StateOrProvince = Column(String(100))
    ZipCode = Column(String(50))
    BusinessPhone = Column(String(50))
    CreatedAt = Column(DateTime(timezone=True))
    UpdatedAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class EmailOpen(Base):
    __tablename__ = "email_open"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    VisitorId = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    VisitorExternalId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    EmailSendType = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class EmailClickthrough(Base):
    __tablename__ = "email_clickthrough"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    VisitorId = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    VisitorExternalId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    EmailSendType = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class EmailSend(Base):
    __tablename__ = "email_send"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    EmailSendType = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class FormSubmit(Base):
    __tablename__ = "form_submit"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    VisitorId = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    VisitorExternalId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Subscribe(Base):
    __tablename__ = "subscribe"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Unsubscribe(Base):
    __tablename__ = "unsubscribe"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class WebVisit(Base):
    __tablename__ = "web_visit"
    ActivityId = Column(String(50), primary_key=True)
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    VisitorId = Column(String(50))
    VisitorExternalId = Column(String(50))
    ExternalId = Column(String(50))
    Duration = Column(String(50))
    NumberOfPages = Column(Integer)
    ReferrerUrl = Column(TEXT)
    FirstPageViewUrl = Column(TEXT)
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class PageView(Base):
    __tablename__ = "page_view"
    ActivityId = Column(String(50), primary_key=True)
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    VisitorId = Column(String(50))
    VisitorExternalId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    WebVisitId = Column(String(50))
    ReferrerUrl = Column(TEXT)
    Url = Column(TEXT)
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Bounceback(Base):
    __tablename__ = "bounceback"
    ActivityId = Column(String(50), primary_key=True)
    AssetName = Column(String(200))
    SmtpMessage = Column(String(1000))
    ContactId = Column(String(50))
    ActivityType = Column(String(50))
    AssetType = Column(String(50))
    AssetId = Column(String(50))
    CampaignId = Column(String(50))
    ExternalId = Column(String(50))
    SmtpErrorCode = Column(String(50))
    SmtpStatusCode = Column(String(50))
    ActivityDate = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Campaign(Base):
    __tablename__ = "campaign"
    id = Column(String(50), primary_key=True)
    name = Column(String(200))
    campaignCategory = Column(String(50))
    currentStatus = Column(String(50))
    isEmailMarketingCampaign = Column(Boolean)
    country = Column(String(50))
    businessUnit = Column(String(50))
    stage = Column(String(50))
    createdAt = Column(DateTime(timezone=True))
    updatedAt = Column(DateTime(timezone=True))
    endAt = Column(DateTime(timezone=True))
    startAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Form(Base):
    __tablename__ = "form"
    id = Column(String(50), primary_key=True)
    name = Column(String(200))
    currentStatus = Column(String(50))
    isResponsive = Column(Boolean)
    isArchived = Column(Boolean)
    createdAt = Column(DateTime(timezone=True))
    updatedAt = Column(DateTime(timezone=True))
    endAt = Column(DateTime(timezone=True))
    startAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class LandingPage(Base):
    __tablename__ = "landing_page"
    id = Column(String(50), primary_key=True)
    name = Column(String(200))
    description = Column(String(200))
    currentStatus = Column(String(50))
    createdAt = Column(DateTime(timezone=True))
    updatedAt = Column(DateTime(timezone=True))
    endAt = Column(DateTime(timezone=True))
    startAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class Email(Base):
    __tablename__ = "email"
    id = Column(String(50), primary_key=True)
    name = Column(String(200))
    description = Column(String(200))
    currentStatus = Column(String(50))
    emailGroupId = Column(String(50))
    createdAt = Column(DateTime(timezone=True))
    updatedAt = Column(DateTime(timezone=True))
    endAt = Column(DateTime(timezone=True))
    startAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class EmailGroup(Base):
    __tablename__ = "email_group"
    id = Column(String(50), primary_key=True)
    name = Column(String(200))
    description = Column(String(200))
    createdAt = Column(DateTime(timezone=True))
    updatedAt = Column(DateTime(timezone=True))
    endAt = Column(DateTime(timezone=True))
    startAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


class CDO(Base):
    __tablename__ = "cdo_wehale_lead_status"
    ContactID = Column(String(50), primary_key=True)
    Company = Column(String(250))
    LeadTypeIdentifiedContactTimestamp = Column(DateTime(timezone=True))
    LeadTypeMarketingLeadTimestamp = Column(DateTime(timezone=True))
    LeadTypeSalesLeadTimestamp = Column(DateTime(timezone=True))
    Country = Column(String(50))
    Proffesion = Column(String(50))
    PageLang = Column(String(50))
    SourceURL = Column(String(250))
    GoogleUTMCampaignMostRecent = Column(String(50))
    GoogleUTMMediumMostRecent = Column(String(50))
    GoogleUTMSourceMostRecent = Column(String(50))
    DataCardExternalId = Column(String(50))
    DataCardCreatedAt = Column(DateTime(timezone=True))
    DataCardUpdatedAt = Column(DateTime(timezone=True))
    dbLastUpdatedUTC = Column(DateTime(timezone=True), server_default=text("CURRENT_TIMESTAMP"))


def create_tables():
    Base.metadata.create_all(engine)
    logger.debug("All tables created.")


# run locally only once to set up DB
if __name__ == "__main__":
    create_tables()
