from contextlib import contextmanager
from datetime import datetime

from sqlalchemy.orm import Session

from table_init import *

import logging as logger


# manage psql session
@contextmanager
def start_psql_session():
    session = Session(bind=engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def sort_upsert_data(session_, type, data):
    ids_to_update = []
    result = dict()
    existing_data = None
    
    if type == "contact":
        ids = [entry["ContactID"] for entry in data]
        existing_data = session_.query(Contact.ContactID).filter(Contact.ContactID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["ContactID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["ContactID"] not in ids_to_update]
        
    if type == "account":
        ids = [entry["CompanyID"] for entry in data]
        existing_data = session_.query(Account.CompanyID).filter(Account.CompanyID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["CompanyID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["CompanyID"] not in ids_to_update]
        
    if type in ["EmailOpen", "EmailClickthrough", "EmailSend", "FormSubmit", "Subscribe",
                "Unsubscribe", "WebVisit", "PageView", "Bounceback"]:
        ids = [entry["ActivityId"] for entry in data]
        if type == "EmailOpen":
            existing_data = session_.query(EmailOpen.ActivityId).filter(EmailOpen.ActivityId.in_(ids)).all()
        if type == "EmailClickthrough":
            existing_data = session_.query(EmailClickthrough.ActivityId).filter(EmailClickthrough.ActivityId.in_(ids)).all()
        if type == "EmailSend":
            existing_data = session_.query(EmailSend.ActivityId).filter(EmailSend.ActivityId.in_(ids)).all()
        if type == "FormSubmit":
            existing_data = session_.query(FormSubmit.ActivityId).filter(FormSubmit.ActivityId.in_(ids)).all()
        if type == "Subscribe":
            existing_data = session_.query(Subscribe.ActivityId).filter(Subscribe.ActivityId.in_(ids)).all()
        if type == "Unsubscribe":
            existing_data = session_.query(Unsubscribe.ActivityId).filter(Unsubscribe.ActivityId.in_(ids)).all()
        if type == "WebVisit":
            existing_data = session_.query(WebVisit.ActivityId).filter(WebVisit.ActivityId.in_(ids)).all()
        if type == "PageView":
            existing_data = session_.query(PageView.ActivityId).filter(PageView.ActivityId.in_(ids)).all()
        if type == "Bounceback":
            existing_data = session_.query(Bounceback.ActivityId).filter(Bounceback.ActivityId.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["ActivityId"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["ActivityId"] not in ids_to_update]
        
    if type in ["campaigns", "forms", "landingPages", "emails", "email/groups"]:
        ids = [entry["id"] for entry in data]
        if type == "campaigns":
            existing_data = session_.query(Campaign.id).filter(Campaign.id.in_(ids)).all()
        if type == "forms":
            existing_data = session_.query(Form.id).filter(Form.id.in_(ids)).all()
        if type == "landingPages":
            existing_data = session_.query(LandingPage.id).filter(LandingPage.id.in_(ids)).all()
        if type == "emails":
            existing_data = session_.query(Email.id).filter(Email.id.in_(ids)).all()
        if type == "email/groups":
            existing_data = session_.query(EmailGroup.id).filter(EmailGroup.id.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["id"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["id"] not in ids_to_update]

    if type == "cdo":
        ids = [entry["ContactID"] for entry in data]
        existing_data = session_.query(CDO.ContactID).filter(CDO.ContactID.in_(ids)).all()
        for entry in existing_data:
            ids_to_update.append(entry[0])
        result["entries_to_update"] = [entry for entry in data if entry["ContactID"] in ids_to_update]
        result["entries_to_insert"] = [entry for entry in data if entry["ContactID"] not in ids_to_update]

    return result


def pre_process_asset_data(data):
    if data:
        for entry in data:
            if entry.get("isEmailMarketingCampaign", None) == "false":
                entry["isEmailMarketingCampaign"] = False
            if entry.get("isEmailMarketingCampaign", None) == "true":
                entry["isEmailMarketingCampaign"] = True
            if entry.get("isResponsive", None) == "false":
                entry["isResponsive"] = False
            if entry.get("isResponsive", None) == "true":
                entry["isResponsive"] = True
            if entry.get("archived", None) == "false":
                entry["archived"] = False
            if entry.get("archived", None) == "true":
                entry["archived"] = True
            if entry.get("createdAt", None):
                date_time = datetime.fromtimestamp(int(entry.get("createdAt", None)))
                entry["createdAt"] = str(date_time.date()) + " " + str(date_time.time())
            if entry.get("updatedAt", None):
                date_time = datetime.fromtimestamp(int(entry.get("updatedAt", None)))
                entry["updatedAt"] = str(date_time.date()) + " " + str(date_time.time())
            if entry.get("endAt", None):
                date_time = datetime.fromtimestamp(int(entry.get("endAt", None)))
                entry["endAt"] = str(date_time.date()) + " " + str(date_time.time())
            if entry.get("startAt", None):
                date_time = datetime.fromtimestamp(int(entry.get("startAt", None)))
                entry["startAt"] = str(date_time.date()) + " " + str(date_time.time())
    return data


def upsert_email_open(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="EmailOpen", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    EmailOpen.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                VisitorId=data_to_insert[i].get("VisitorId", None) or None,
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                VisitorExternalId=data_to_insert[i].get("VisitorExternalId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                EmailSendType=data_to_insert[i].get("EmailSendType", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'email_open'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'email_open': {0}".format(e))

    else:
        logger.debug("No data to insert/update to table 'email_open'")


def upsert_email_clickthrough(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="EmailClickthrough", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    EmailClickthrough.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                VisitorId=data_to_insert[i].get("VisitorId", None) or None,
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                VisitorExternalId=data_to_insert[i].get("VisitorExternalId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                EmailSendType=data_to_insert[i].get("EmailSendType", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'email_clickthrough'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'email_clickthrough': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'email_clickthrough'")


def upsert_email_send(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="EmailSend", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    EmailSend.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                EmailSendType=data_to_insert[i].get("EmailSendType", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'email_send'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'email_send': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'email_send'")


def upsert_form_submit(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="FormSubmit", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    FormSubmit.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                VisitorId=data_to_insert[i].get("VisitorId", None) or None,
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                VisitorExternalId=data_to_insert[i].get("VisitorExternalId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                        )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'form_submit'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'form_submit': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'form_submit'")


def upsert_subscribe(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="Subscribe", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    Subscribe.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'subscribe'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'subscribe': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'subscribe'")


def upsert_unsubscribe(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="Unsubscribe", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    Unsubscribe.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'unsubscribe'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'unsubscribe': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'unsubscribe'")


def upsert_web_visit(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="WebVisit", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    WebVisit.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                VisitorId=data_to_insert[i].get("VisitorId", None) or None,
                                VisitorExternalId=data_to_insert[i].get("VisitorExternalId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                Duration=data_to_insert[i].get("Duration", None) or None,
                                NumberOfPages=data_to_insert[i].get("NumberOfPages", None) or None,
                                ReferrerUrl=data_to_insert[i].get("ReferrerUrl", None) or None,
                                FirstPageViewUrl=data_to_insert[i].get("FirstPageViewUrl", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'web_visit'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'web_visit': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'web_visit'")


def upsert_page_view(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="PageView", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    PageView.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                VisitorId=data_to_insert[i].get("VisitorId", None) or None,
                                VisitorExternalId=data_to_insert[i].get("VisitorExternalId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                WebVisitId=data_to_insert[i].get("WebVisitId", None) or None,
                                ReferrerUrl=data_to_insert[i].get("ReferrerUrl", None) or None,
                                Url=data_to_insert[i].get("Url", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'page_view'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'page_view': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'page_view'")


def upsert_bounceback(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="Bounceback", data=data)
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_insert:
            try:
                session_.execute(
                    Bounceback.__table__.insert(),
                    [
                        dict
                            (
                                ActivityId=data_to_insert[i].get("ActivityId", None),
                                AssetName=data_to_insert[i].get("AssetName", None) or None,
                                SmtpMessage=data_to_insert[i].get("SmtpMessage", None) or None,
                                ContactId=data_to_insert[i].get("ContactId", None),
                                ActivityType=data_to_insert[i].get("ActivityType", None),
                                AssetType=data_to_insert[i].get("AssetType", None) or None,
                                AssetId=data_to_insert[i].get("AssetId", None) or None,
                                CampaignId=data_to_insert[i].get("CampaignId", None) or None,
                                ExternalId=data_to_insert[i].get("ExternalId", None) or None,
                                SmtpErrorCode=data_to_insert[i].get("SmtpErrorCode", None) or None,
                                SmtpStatusCode=data_to_insert[i].get("SmtpStatusCode", None) or None,
                                ActivityDate=data_to_insert[i].get("ActivityDate", None)
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'bounceback'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'bounceback': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'bounceback'")


def upsert_contact(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="contact", data=data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        Contact.__table__.update()
                            .values
                                (
                                    Company=data_to_update[i].get("Company", None) or None,
                                    City=data_to_update[i].get("City", None) or None,
                                    Country=data_to_update[i].get("Country", None) or None,
                                    Title=data_to_update[i].get("Title", None) or None,
                                    Industry=data_to_update[i].get("Industry", None) or None,
                                    AnnualRevenue=data_to_update[i].get("AnnualRevenue", None) or None,
                                    EmailDomain=data_to_update[i].get("EmailDomain", None) or None,
                                    CreatedAt=data_to_update[i].get("CreatedAt", None) or None,
                                    UpdatedAt=data_to_update[i].get("UpdatedAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(Contact.ContactID == data_to_update[i]["ContactID"])
                    )
                logger.debug("Finish updating {0} records to table 'contact'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'contact': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    Contact.__table__.insert(),
                    [
                        dict
                            (
                                ContactID=data_to_insert[i].get("ContactID", None) or None,
                                Company=data_to_insert[i].get("Company", None) or None,
                                City=data_to_insert[i].get("City", None) or None,
                                Country=data_to_insert[i].get("Country", None) or None,
                                Title=data_to_insert[i].get("Title", None) or None,
                                Industry=data_to_insert[i].get("Industry", None) or None,
                                AnnualRevenue=data_to_insert[i].get("AnnualRevenue", None) or None,
                                EmailDomain=data_to_insert[i].get("EmailDomain", None) or None,
                                CreatedAt=data_to_insert[i].get("CreatedAt", None) or None,
                                UpdatedAt=data_to_insert[i].get("UpdatedAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'contact'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'contact': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'contact'")


def upsert_account(session_, data):
    if data:
        upsert_data = sort_upsert_data(session_=session_, type="account", data=data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        Account.__table__.update()
                            .values
                                (
                                    Company=data_to_update[i].get("Company", None) or None,
                                    Country=data_to_update[i].get("Country", None) or None,
                                    Address=data_to_update[i].get("Address", None) or None,
                                    City=data_to_update[i].get("City", None) or None,
                                    StateOrProvince=data_to_update[i].get("StateOrProvince", None) or None,
                                    ZipCode=data_to_update[i].get("ZipCode", None) or None,
                                    BusinessPhone=data_to_update[i].get("BusinessPhone", None) or None,
                                    CreatedAt=data_to_update[i].get("CreatedAt", None) or None,
                                    UpdatedAt=data_to_update[i].get("UpdatedAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(Account.CompanyID == data_to_update[i]["CompanyID"])
                    )
                logger.debug("Finish updating {0} records to table 'account'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'account': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    Account.__table__.insert(),
                    [
                        dict
                            (
                                CompanyID=data_to_insert[i].get("CompanyID", None) or None,
                                Company=data_to_insert[i].get("Company", None) or None,
                                Country=data_to_insert[i].get("Country", None) or None,
                                Address=data_to_insert[i].get("Address", None) or None,
                                City=data_to_insert[i].get("City", None) or None,
                                StateOrProvince=data_to_insert[i].get("StateOrProvince", None) or None,
                                ZipCode=data_to_insert[i].get("ZipCode", None) or None,
                                BusinessPhone=data_to_insert[i].get("BusinessPhone", None) or None,
                                CreatedAt=data_to_insert[i].get("CreatedAt", None) or None,
                                UpdatedAt=data_to_insert[i].get("UpdatedAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'account'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'account': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'account'")


def upsert_campaign(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="campaigns", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        Campaign.__table__.update()
                            .values
                                (
                                    name=data_to_update[i].get("name", None) or None,
                                    campaignCategory=data_to_update[i].get("campaignCategory", None) or None,
                                    currentStatus=data_to_update[i].get("currentStatus", None) or None,
                                    isEmailMarketingCampaign=data_to_update[i].get("isEmailMarketingCampaign", None) or None,
                                    country=data_to_update[i]["fieldValues"][0].get("value", None) or None if data_to_update[i].get("fieldValues", None) else None,
                                    businessUnit=data_to_update[i]["fieldValues"][1].get("value", None) or None if data_to_update[i].get("fieldValues", None) else None,
                                    stage=data_to_update[i]["fieldValues"][2].get("value", None) or None if data_to_update[i].get("fieldValues", None) else None,
                                    createdAt=data_to_update[i].get("createdAt", None) or None,
                                    updatedAt=data_to_update[i].get("updatedAt", None) or None,
                                    endAt=data_to_update[i].get("endAt", None) or None,
                                    startAt=data_to_update[i].get("startAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(Campaign.id == data_to_update[i]["id"])
                    )
                logger.debug("Finish updating {0} records to table 'campaign'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'campaign': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    Campaign.__table__.insert(),
                    [
                        dict
                            (
                                id=data_to_insert[i].get("id", None) or None,
                                name=data_to_insert[i].get("name", None) or None,
                                campaignCategory=data_to_insert[i].get("campaignCategory", None) or None,
                                currentStatus=data_to_insert[i].get("currentStatus", None) or None,
                                isEmailMarketingCampaign=data_to_insert[i].get("isEmailMarketingCampaign", None) or None,
                                country=data_to_insert[i]["fieldValues"][0].get("value", None) or None if data_to_insert[i].get("fieldValues", None) else None,
                                businessUnit=data_to_insert[i]["fieldValues"][1].get("value", None) or None if data_to_insert[i].get("fieldValues", None) else None,
                                stage=data_to_insert[i]["fieldValues"][2].get("value", None) or None if data_to_insert[i].get("fieldValues", None) else None,
                                createdAt=data_to_insert[i].get("createdAt", None) or None,
                                updatedAt=data_to_insert[i].get("updatedAt", None) or None,
                                endAt=data_to_insert[i].get("endAt", None) or None,
                                startAt=data_to_insert[i].get("startAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'campaign'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'campaign': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'campaign'")


def upsert_form(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="forms", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        Form.__table__.update()
                            .values
                                (
                                    name=data_to_update[i].get("name", None) or None,
                                    currentStatus=data_to_update[i].get("currentStatus", None) or None,
                                    isResponsive=data_to_update[i].get("isResponsive", None) or None,
                                    isArchived=data_to_update[i].get("archived", None) or None,
                                    createdAt=data_to_update[i].get("createdAt", None) or None,
                                    updatedAt=data_to_update[i].get("updatedAt", None) or None,
                                    endAt=data_to_update[i].get("endAt", None) or None,
                                    startAt=data_to_update[i].get("startAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(Form.id == data_to_update[i]["id"])
                    )
                logger.debug("Finish updating {0} records to table 'form'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'form': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    Form.__table__.insert(),
                    [
                        dict
                            (
                                id=data_to_insert[i].get("id", None) or None,
                                name=data_to_insert[i].get("name", None) or None,
                                currentStatus=data_to_insert[i].get("currentStatus", None) or None,
                                isResponsive=data_to_insert[i].get("isResponsive", None) or None,
                                isArchived=data_to_insert[i].get("archived", None) or None,
                                createdAt=data_to_insert[i].get("createdAt", None) or None,
                                updatedAt=data_to_insert[i].get("updatedAt", None) or None,
                                endAt=data_to_insert[i].get("endAt", None) or None,
                                startAt=data_to_insert[i].get("startAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'form'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'form': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'form'")


def upsert_landing_page(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="landingPages", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        LandingPage.__table__.update()
                            .values
                                (
                                    name=data_to_update[i].get("name", None) or None,
                                    description=data_to_update[i].get("description", None) or None,
                                    currentStatus=data_to_update[i].get("currentStatus", None) or None,
                                    createdAt=data_to_update[i].get("createdAt", None) or None,
                                    updatedAt=data_to_update[i].get("updatedAt", None) or None,
                                    endAt=data_to_update[i].get("endAt", None) or None,
                                    startAt=data_to_update[i].get("startAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(LandingPage.id == data_to_update[i]["id"])
                    )
                logger.debug("Finish updating {0} records to table 'landing_page'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'landing_page': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    LandingPage.__table__.insert(),
                    [
                        dict
                            (
                                id=data_to_insert[i].get("id", None) or None,
                                name=data_to_insert[i].get("name", None) or None,
                                description=data_to_insert[i].get("description", None) or None,
                                currentStatus=data_to_insert[i].get("currentStatus", None) or None,
                                createdAt=data_to_insert[i].get("createdAt", None) or None,
                                updatedAt=data_to_insert[i].get("updatedAt", None) or None,
                                endAt=data_to_insert[i].get("endAt", None) or None,
                                startAt=data_to_insert[i].get("startAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'landing_page'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'landing_page': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'landing_page'")


def upsert_email(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="emails", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        Email.__table__.update()
                            .values
                                (
                                    name=data_to_update[i].get("name", None) or None,
                                    description=data_to_update[i].get("description", None) or None,
                                    currentStatus=data_to_update[i].get("currentStatus", None) or None,
                                    emailGroupId=data_to_update[i].get("emailGroupId", None) or None,
                                    createdAt=data_to_update[i].get("createdAt", None) or None,
                                    updatedAt=data_to_update[i].get("updatedAt", None) or None,
                                    endAt=data_to_update[i].get("endAt", None) or None,
                                    startAt=data_to_update[i].get("startAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(Email.id == data_to_update[i]["id"])
                    )
                logger.debug("Finish updating {0} records to table 'email'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'email': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    Email.__table__.insert(),
                    [
                        dict
                            (
                                id=data_to_insert[i].get("id", None) or None,
                                name=data_to_insert[i].get("name", None) or None,
                                description=data_to_insert[i].get("description", None) or None,
                                currentStatus=data_to_insert[i].get("currentStatus", None) or None,
                                emailGroupId=data_to_insert[i].get("emailGroupId", None) or None,
                                createdAt=data_to_insert[i].get("createdAt", None) or None,
                                updatedAt=data_to_insert[i].get("updatedAt", None) or None,
                                endAt=data_to_insert[i].get("endAt", None) or None,
                                startAt=data_to_insert[i].get("startAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'email'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'email': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'email'")


def upsert_email_group(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="emails", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        EmailGroup.__table__.update()
                            .values
                                (
                                    name=data_to_update[i].get("name", None) or None,
                                    description=data_to_update[i].get("description", None) or None,
                                    createdAt=data_to_update[i].get("createdAt", None) or None,
                                    updatedAt=data_to_update[i].get("updatedAt", None) or None,
                                    endAt=data_to_update[i].get("endAt", None) or None,
                                    startAt=data_to_update[i].get("startAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(EmailGroup.id == data_to_update[i]["id"])
                    )
                logger.debug("Finish updating {0} records to table 'email_group'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'email_group': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    EmailGroup.__table__.insert(),
                    [
                        dict
                            (
                                id=data_to_insert[i].get("id", None) or None,
                                name=data_to_insert[i].get("name", None) or None,
                                description=data_to_insert[i].get("description", None) or None,
                                createdAt=data_to_insert[i].get("createdAt", None) or None,
                                updatedAt=data_to_insert[i].get("updatedAt", None) or None,
                                endAt=data_to_insert[i].get("endAt", None) or None,
                                startAt=data_to_insert[i].get("startAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'email_group'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'email_group': {0}".format(e))
                
    else:
        logger.debug("No data to insert/update to table 'email_group'")


def upsert_cdo(session_, data):
    if data:
        processed_data = pre_process_asset_data(data=data)
        upsert_data = sort_upsert_data(session_=session_, type="cdo", data=processed_data)
        data_to_update = upsert_data.get("entries_to_update", [])
        data_to_insert = upsert_data.get("entries_to_insert", [])

        if data_to_update:
            try:
                db_last_updated_utc = str(datetime.utcnow().date()) + " " + str(datetime.utcnow().time())
                for i in range(0, len(data_to_update)):
                    session_.execute(
                        CDO.__table__.update()
                            .values
                                (
                                    Company=data_to_update[i].get("Company", None) or None,
                                    LeadTypeIdentifiedContactTimestamp=data_to_update[i].get("LeadTypeIdentifiedContactTimestamp", None) or None,
                                    LeadTypeMarketingLeadTimestamp=data_to_update[i].get("LeadTypeMarketingLeadTimestamp", None) or None,
                                    LeadTypeSalesLeadTimestamp=data_to_update[i].get("LeadTypeSalesLeadTimestamp", None) or None,
                                    Country=data_to_update[i].get("Country", None) or None,
                                    Proffesion=data_to_update[i].get("Proffesion", None) or None,
                                    PageLang=data_to_update[i].get("PageLang", None) or None,
                                    SourceURL=data_to_update[i].get("SourceURL", None) or None,
                                    GoogleUTMCampaignMostRecent=data_to_update[i].get("GoogleUTMCampaignMostRecent", None) or None,
                                    GoogleUTMMediumMostRecent=data_to_update[i].get("GoogleUTMMediumMostRecent", None) or None,
                                    GoogleUTMSourceMostRecent=data_to_update[i].get("GoogleUTMSourceMostRecent", None) or None,
                                    DataCardExternalId=data_to_update[i].get("DataCardExternalId", None) or None,
                                    DataCardCreatedAt=data_to_update[i].get("DataCardCreatedAt", None) or None,
                                    DataCardUpdatedAt=data_to_update[i].get("DataCardUpdatedAt", None) or None,
                                    dbLastUpdatedUTC=db_last_updated_utc
                                )
                            .where(CDO.ContactID == data_to_update[i]["ContactID"])
                    )
                logger.debug("Finish updating {0} records to table 'cdo_wehale_lead_status'".format(len(data_to_update)))
            except Exception as e:
                logger.error("Error updating to table 'cdo_wehale_lead_status': {0}".format(e))

        if data_to_insert:
            try:
                session_.execute(
                    CDO.__table__.insert(),
                    [
                        dict
                            (
                                ContactID=data_to_insert[i].get("ContactID", None) or None,
                                Company=data_to_insert[i].get("Company", None) or None,
                                LeadTypeIdentifiedContactTimestamp=data_to_insert[i].get("LeadTypeIdentifiedContactTimestamp", None) or None,
                                LeadTypeMarketingLeadTimestamp=data_to_insert[i].get("LeadTypeMarketingLeadTimestamp", None) or None,
                                LeadTypeSalesLeadTimestamp=data_to_insert[i].get("LeadTypeSalesLeadTimestamp", None) or None,
                                Country=data_to_insert[i].get("Country", None) or None,
                                Proffesion=data_to_insert[i].get("Proffesion", None) or None,
                                PageLang=data_to_insert[i].get("PageLang", None) or None,
                                SourceURL=data_to_insert[i].get("SourceURL", None) or None,
                                GoogleUTMCampaignMostRecent=data_to_insert[i].get("GoogleUTMCampaignMostRecent", None) or None,
                                GoogleUTMMediumMostRecent=data_to_insert[i].get("GoogleUTMMediumMostRecent", None) or None,
                                GoogleUTMSourceMostRecent=data_to_insert[i].get("GoogleUTMSourceMostRecent", None) or None,
                                DataCardExternalId=data_to_insert[i].get("DataCardExternalId", None) or None,
                                DataCardCreatedAt=data_to_insert[i].get("DataCardCreatedAt", None) or None,
                                DataCardUpdatedAt=data_to_insert[i].get("DataCardUpdatedAt", None) or None
                            )
                        for i in range(0, len(data_to_insert))
                    ]
                )
                logger.debug("Finish inserting {0} records to table 'cdo_wehale_lead_status'".format(len(data_to_insert)))
            except Exception as e:
                logger.error("Error inserting to table 'cdo_wehale_lead_status': {0}".format(e))

    else:
        logger.debug("No data to insert/update to table 'email_group'")