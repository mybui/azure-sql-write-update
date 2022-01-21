from datetime import datetime
from datetime import timedelta

from dea.bulk.api import BulkClient
from dea.bulk.definitions import ExportDefinition
from dea.rest.api.cdo import RestCdoClient
from requests.auth import HTTPBasicAuth

from elq_config import *


class ElqClient:
    def __init__(self, username, password, base_url):
        self.bulk_client = BulkClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)
        self.rest_client = RestCdoClient(auth=HTTPBasicAuth(username=username, password=password), base_url=base_url)

    def get_last_24_hours_date_time(self):
        past = datetime.utcnow() - timedelta(hours=24)
        date_from = str(past.date()) + " " + str(past.time()).split(".")[0]
        return date_from
    
    def get_last_6_months_date_time(self):
        past = datetime.utcnow() - timedelta(days=183)
        date_from = str(past.date()) + " " + str(past.time()).split(".")[0]
        return date_from

    def export_activities(self, type, first_run=False):
        if type in activity_export_def.keys():
            if first_run:
                past_time = self.get_last_6_months_date_time()
            else:
                past_time = self.get_last_24_hours_date_time()
            filter = "'{0}'='{1}' AND '{2}'>'{3}'".format("{{Activity.Type}}", type, "{{Activity.CreatedAt}}", past_time)
            export_def = ExportDefinition(name="{0} Activity Export".format(type),
                                          fields=activity_export_def[type],
                                          filter=filter)
            activities = list(self.bulk_client.bulk_activities.exports.create_export(export_def=export_def,
                                                                                     delete_export_on_close=True,
                                                                                     sync_limit=50000))
            return activities
        return None

    def export_contacts(self, first_run=False):
        if first_run:
            filter = "'{0}'='1' AND ('{1}' = 'ES' OR '{2}' = 'DE' OR '{3}' = 'PL' OR '{4}' = 'DK' OR '{5}' = 'FI')".format(
                "{{Contact.Field(C_Wehale_Contact1)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}")
        else:
            past_time = self.get_last_24_hours_date_time()
            filter = "'{0}'>'{1}' AND '{2}'='1' AND ('{3}' = 'ES' OR '{4}' = 'DE' OR '{5}' = 'PL' OR '{6}' = 'DK' OR '{7}' = 'FI')".format(
                "{{Contact.Field(C_DateModified)}}",
                past_time,
                "{{Contact.Field(C_Wehale_Contact1)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}",
                "{{Contact.Field(C_Country)}}")

        export_def = ExportDefinition(name="Contact Export", fields=contact_export_def, filter=filter)
        contacts = list(self.bulk_client.bulk_contacts.exports.create_export(export_def=export_def,
                                                                             delete_export_on_close=True,
                                                                             sync_limit=50000))
        return contacts or None

    def export_accounts(self, first_run=False):
        if first_run:
            export_def = ExportDefinition(name="Account Export", fields=account_export_def)
            accounts = list(self.bulk_client.bulk_accounts.exports.create_export(export_def=export_def,
                                                                                 delete_export_on_close=True,
                                                                                 sync_limit=50000))
        else:
            past_time = self.get_last_24_hours_date_time()
            filter = "'{0}'>'{1}'".format("{{Account.Field(M_DateModified)}}", past_time)
            export_def = ExportDefinition(name="Account Export", fields=account_export_def, filter=filter)
            accounts = list(self.bulk_client.bulk_accounts.exports.create_export(export_def=export_def,
                                                                                 delete_export_on_close=True,
                                                                                 sync_limit=50000))
        return accounts or None

    def export_assets(self, type, first_run=False):
        if type in ["campaigns", "forms", "landingPages", "emails", "email/groups"]:
            if first_run:
                past_time = None
            else:
                past_time = self.get_last_24_hours_date_time()

            if past_time:
                # set depth = partial for emails to get info on email group id
                if type == "emails" or type == "campaigns":
                    assets = list(self.rest_client.get(self.rest_client.rest_url_for(
                        path="assets/emails?depth=partial&search=updatedAt>{0}".format(past_time), version="2.0"))[
                                      "elements"])
                elif type == "campaigns":
                    assets = list(self.rest_client.get(self.rest_client.rest_url_for(
                        path="assets/campaigns?depth=partial&search=updatedAt>{0}".format(past_time), version="2.0"))[
                                      "elements"])
                else:
                    filter = "?search=updatedAt>'{0}'".format(past_time)
                    assets = list(self.rest_client.get(
                        self.rest_client.rest_url_for(path="assets/{0}{1}".format(type, filter), version="2.0"))[
                                      "elements"])
            else:
                if type == "emails":
                    assets = list(self.rest_client.get(
                        self.rest_client.rest_url_for(path="assets/emails?depth=partial", version="2.0"))["elements"])
                elif type == "campaigns":
                    assets = list(self.rest_client.get(
                        self.rest_client.rest_url_for(path="assets/campaigns?depth=partial", version="2.0"))["elements"])
                else:
                    assets = list(self.rest_client.get(
                        self.rest_client.rest_url_for(path="assets/{0}".format(type), version="2.0"))["elements"])
            return assets
        return None

    def export_cdo(self, first_run=False):
        if first_run:
            export_def = ExportDefinition(name="CDO 58 Export", fields=cdo_export_def, parent_id=cdo_id)
            cdo = list(self.bulk_client.bulk_cdo.exports.create_export(parent_id=cdo_id, export_def=export_def,
                                                                       delete_export_on_close=True, sync_limit=50000))
        else:
            past_time = self.get_last_24_hours_date_time()
            filter = "'{0}'>'{1}'".format("{{CustomObject[58].UpdatedAt}}", past_time)
            export_def = ExportDefinition(name="CDO 58 Export", fields=cdo_export_def, parent_id=cdo_id, filter=filter)
            cdo = list(self.bulk_client.bulk_cdo.exports.create_export(parent_id=cdo_id, export_def=export_def,
                                                                       delete_export_on_close=True, sync_limit=50000))
        return cdo or None