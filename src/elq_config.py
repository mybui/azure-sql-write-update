activity_export_def = {
    "EmailOpen": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "VisitorId": "{{Activity.Visitor.Id}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "EmailSendType": "{{Activity.Field(EmailSendType)}}"
    },
    "EmailClickthrough": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "VisitorId": "{{Activity.Visitor.Id}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "EmailSendType": "{{Activity.Field(EmailSendType)}}"
    },
    "EmailSend": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "EmailSendType": "{{Activity.Field(EmailSendType)}}"
    },
    "FormSubmit": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "VisitorId": "{{Activity.Visitor.Id}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}"
    },
    "Subscribe": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}"
    },
    "Unsubscribe": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}"
    },
    "WebVisit": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "VisitorId": "{{Activity.Visitor.Id}}",
        "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "Duration": "{{Activity.Field(Duration)}}",
        "NumberOfPages": "{{Activity.Field(NumberOfPages)}}",
        "ReferrerUrl": "{{Activity.Field(ReferrerUrl)}}",
        "FirstPageViewUrl": "{{Activity.Field(FirstPageViewUrl)}}"
    },
    "PageView": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "VisitorId": "{{Activity.Visitor.Id}}",
        "VisitorExternalId": "{{Activity.Visitor.ExternalId}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "ReferrerUrl": "{{Activity.Field(ReferrerUrl)}}",
        "Url": "{{Activity.Field(Url)}}",
        "WebVisitId": "{{Activity.Field(WebVisitId)}}"

    },
    "Bounceback": {
        "ActivityId": "{{Activity.Id}}",
        "ContactId": "{{Activity.Contact.Id}}",
        "ActivityType": "{{Activity.Type}}",
        "ActivityDate": "{{Activity.CreatedAt}}",
        "AssetType": "{{Activity.Asset.Type}}",
        "AssetName": "{{Activity.Asset.Name}}",
        "AssetId": "{{Activity.Asset.Id}}",
        "CampaignId": "{{Activity.Campaign.Id}}",
        "ExternalId": "{{Activity.ExternalId}}",
        "SmtpErrorCode": "{{Activity.Field(SmtpErrorCode)}}",
        "SmtpStatusCode": "{{Activity.Field(SmtpStatusCode)}}",
        "SmtpMessage": "{{Activity.Field(SmtpMessage)}}"
    }
}

contact_export_def = {
    "ContactID": "{{Contact.Id}}",
    "Company": "{{Contact.Field(C_Company)}}",
    "City": "{{Contact.Field(C_City)}}",
    "Country": "{{Contact.Field(C_Country)}}",
    "Title": "{{Contact.Field(C_Title)}}",
    "CreatedAt": "{{Contact.Field(C_DateCreated)}}",
    "UpdatedAt": "{{Contact.Field(C_DateModified)}}",
    "Industry": "{{Contact.Field(C_Industry1)}}",
    "AnnualRevenue": "{{Contact.Field(C_Annual_Revenue1)}}",
    "EmailDomain": "{{Contact.Field(C_EmailAddressDomain)}}",
    "WehaleContact": "{{Contact.Field(C_Wehale_Contact1)}}"
}

account_export_def = {
    "CompanyID": "{{Account.Field(CompanyIDExt)}}",
    "Company": "{{Account.Field(M_CompanyName)}}",
    "Country": "{{Account.Field(M_Country)}}",
    "Address": "{{Account.Field(M_Address1)}}",
    "City": "{{Account.Field(M_City)}}",
    "StateOrProvince": "{{Account.Field(M_State_Prov)}}",
    "ZipCode": "{{Account.Field(M_Zip_Postal)}}",
    "BusinessPhone": "{{Account.Field(M_BusPhone)}}",
    "CreatedAt": "{{Account.Field(M_DateCreated)}}",
    "UpdatedAt": "{{Account.Field(M_DateModified)}}"
}

cdo_id = "58"

cdo_export_def = {
    "ContactID": "{{CustomObject[58].Contact.Id}}",
    "EmailAddress": "{{CustomObject[58].Field[757]}}",
    "LeadTypeIdentifiedContactTimestamp": "{{CustomObject[58].Field[758]}}",
    "LeadTypeMarketingLeadTimestamp": "{{CustomObject[58].Field[759]}}",
    "LeadTypeSalesLeadTimestamp": "{{CustomObject[58].Field[760]}}",
    "Country": "{{CustomObject[58].Field[839]}}",
    "FirstName": "{{CustomObject[58].Field[840]}}",
    "LastName": "{{CustomObject[58].Field[841]}}",
    "Profession": "{{CustomObject[58].Field[842]}}",
    "PageLang": "{{CustomObject[58].Field[843]}}",
    "SourceUrl": "{{CustomObject[58].Field[844]}}",
    "GoogleUTMCampaignMostRecent": "{{CustomObject[58].Field[845]}}",
    "GoogleUTMMediumMostRecent": "{{CustomObject[58].Field[846]}}",
    "GoogleUTMSourceMostRecent": "{{CustomObject[58].Field[847]}}",
    "DataCardExternalId": "{{CustomObject[58].ExternalId}}",
    "DataCardCreatedAt": "{{CustomObject[58].CreatedAt}}",
    "DataCardUpdatedAt": "{{CustomObject[58].UpdatedAt}}"
}