import argparse

from db_crud import *
from elq_client import ElqClient
from settings import ELQ_USER, ELQ_PASSWORD, ELQ_BASE_URL

elq_client = ElqClient(username=ELQ_USER, password=ELQ_PASSWORD, base_url=ELQ_BASE_URL)


def main():
    # add first run arg
    parser = argparse.ArgumentParser()
    parser.add_argument("--first_run", type=int, help="input 1 for first run, default is 0", nargs='?', default=0)
    first_run = parser.parse_args().first_run

    email_send = elq_client.export_activities(type="EmailSend", first_run=first_run)
    email_open = elq_client.export_activities(type="EmailOpen", first_run=first_run)
    email_clickthrough = elq_client.export_activities(type="EmailClickthrough", first_run=first_run)
    subscribe = elq_client.export_activities(type="Subscribe", first_run=first_run)
    unsubscribe = elq_client.export_activities(type="Unsubscribe", first_run=first_run)
    bounceback = elq_client.export_activities(type="Bounceback", first_run=first_run)
    form_submit = elq_client.export_activities(type="FormSubmit", first_run=first_run)
    web_visit = elq_client.export_activities(type="WebVisit", first_run=first_run)
    page_view = elq_client.export_activities(type="PageView", first_run=first_run)
    contact = elq_client.export_contacts(first_run=first_run)
    account = elq_client.export_accounts(first_run=first_run)
    campaign = elq_client.export_assets(type="campaigns", first_run=first_run)
    form = elq_client.export_assets(type="forms", first_run=first_run)
    landing_page = elq_client.export_assets(type="landingPages", first_run=first_run)
    email = elq_client.export_assets(type="emails", first_run=first_run)
    email_group = elq_client.export_assets(type="email/groups", first_run=first_run)
    cdo = elq_client.export_cdo(first_run=first_run)

    with start_psql_session() as session:
        # upsert all activities
        upsert_email_send(session_=session, data=email_send)
        upsert_email_open(session_=session, data=email_open)
        upsert_email_clickthrough(session_=session, data=email_clickthrough)
        upsert_subscribe(session_=session, data=subscribe)
        upsert_unsubscribe(session_=session, data=unsubscribe)
        upsert_bounceback(session_=session, data=bounceback)
        upsert_form_submit(session_=session, data=form_submit)
        upsert_web_visit(session_=session, data=web_visit)
        upsert_page_view(session_=session, data=page_view)

        # upsert contact
        upsert_contact(session_=session, data=contact)

        # upsert account
        upsert_account(session_=session, data=account)

        # upsert all assets
        upsert_campaign(session_=session, data=campaign)
        upsert_form(session_=session, data=form)
        upsert_landing_page(session_=session, data=landing_page)
        upsert_email(session_=session, data=email)
        upsert_email_group(session_=session, data=email_group)

        # upsert cdo
        upsert_cdo(session_=session, data=cdo)


def lambda_handler(event, context):
    return main()


# run locally only once to set up DB
# if __name__ == "__main__":
#     main()