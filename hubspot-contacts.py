
# return a list of hubspot contacts: https://developers.hubspot.com/docs/methods/contacts/get_contacts

import requests
import json

# main function entry point
def flexio_handler(flex):

    auth_token = '' # TODO: populate the authorization token

    # get the contacts
    url = 'https://api.hubapi.com/contacts/v1/lists/all/contacts/all?hapikey=' + auth_token

    columns = ['firstname', 'lastname', 'jobtitle', 'email', 'phone', 'mobilephone', 'address', 'city', 'state', 'zip', 'country', 'linkedinbio', 'createdate', 'lastmodifieddate']
    count = "&count=100"
    properties = "&property=" + "&property=".join(columns)
    url = url + count + properties

    response = requests.get(url)
    content = response.json()

    # return the info
    results = []
    results.append(columns)

    contacts = content.get('contacts')
    for contact in contacts:
        row = []
        for column in columns:
            row.append(contact.get('properties').get(column,{}).get('value',''))
        results.append(row)

    flex.output.content_type = "application/json"
    flex.output.write(results)
