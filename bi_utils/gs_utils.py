"""
Created by anna.anisienia on 25/11/2019
"""
import pandas as pd
import pickle
import time
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path
from bi_utils.utils import set_logging


def load_google_spreadsheet_to_df(spreadsheet_id, cell_range, header=True, render_format='FORMATTED_VALUE', num_retries=6, interval_retry=10):
    """
    Uses Sheets API. The file token.pickle stores the user's access and refresh tokens, and is created automatically
    when the authorization flow completes for the first time.
    Docs: https://developers.google.com/resources/api-libraries/documentation/sheets/v4/python/latest/sheets_v4.spreadsheets.values.html

    To use this function, make sure that you have credentials.json + optionally also token.pickle

    :param render_format: whether to use 'FORMATTED_VALUE' or 'UNFORMATTED_VALUE' - default FORMATTED
    :param spreadsheet_id: ex. '1pyx0eyQPsWrHEaacK0dwaJL4zO-LNVGh0RHZueox_lA' for this sheet: Product seasonality
                https://docs.google.com/spreadsheets/d/1pyx0eyQPsWrHEaacK0dwaJL4zO-LNVGh0RHZueox_lA/edit#gid=0
    :param cell_range: ex. 'Sheet1' or 'Sheet1!A2:E10'
    :param header: if True, the first row, which is the header (list of column names) will be removed
    :param num_retries: how often the code should try to read data from google spreadsheet, as it sometimes fails due to connection issues.
    :param interval_retry: how long to wait between each retry in seconds
    :return: pandas df
    """
    logger = set_logging()
    # If modifying these scopes, delete the file token.pickle.
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    # -----------------------------------------------------------------------------------------------------------------
    # Get values from the spreadsheet
    # -----------------------------------------------------------------------------------------------------------------
    for attempt in range(num_retries):
        try:
            service = build('sheets', 'v4', credentials=creds)  # Call the Sheets API - version 4
            sheet_values = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                               valueRenderOption=render_format,  # could be also UNFORMATTED_VALUE
                                                               range=cell_range).execute()
        except:
            time.sleep(interval_retry)
        else:
            logger.info('Succeeded in try number {attempt}'.format(attempt=attempt+1))
            break
    data = sheet_values.get('values')
    if not data:
        logger.info('No data found in the spreadsheet.')
    else:
        if header:
            dataframe = pd.DataFrame.from_records(data, columns=data[0])  # first row = header
            dataframe.drop(dataframe.index[0], inplace=True)  # remove 1st row from data (header)
        else:
            dataframe = pd.DataFrame.from_records(data)
        logger.info("Data extracted successfully from GS to df.")
        return dataframe
