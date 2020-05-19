"""
Created on 8/11/2019
"""
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
import pandas as pd
import logging
import sys
import os
import pyexasol
import requests
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import json
from functools import reduce
from os.path import join, dirname
from dotenv import load_dotenv

loggers = {}


def set_logging(name="logger"):
    """
    To avoid repeating logging set-up
    :return: logger
    """
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        loggers[name] = logger
        return logger


def deployment(env=None, prod=True, dev=True):
    """
    Helper function to define in which ENV the script should run
        - without any args it runs in prod and dev
        - deployment(env, prod = True, dev = False) runs in prod only if env = 'prod'
        - deployment(env, prod = True, dev = False) exits script if env = 'dev'
        - deployment(env, prod = False, dev = True) runs in dev only if env = 'dev'
        - deployment(env, prod = False, dev = True) exits script if env = 'prod'
        - deployment() runs in both prod and dev

    Usage:
        - make sure to always pass ENV=dev or ENV=prod in the .env file for the respective bi-python instance
        - from utils import deployment
        - then call the function: ex. deployment()

    :param env: argument from .env file -> should be 'dev' for DEV and 'prod' for PROD env
    :param prod: should be set to True if the script should run in Prod
    :param dev: should be set to True if the script should run in Dev
    :return: nothing, just exit the script, if the script should not run in this particular env
    """
    logger = set_logging()
    try:
        # BOTH ARE FALSE
        if prod == False and dev == False and env is None:
            logger.info('runs neither in prod nor dev')
            exit()
        elif prod == False and dev == False and env == 'prod':
            logger.info('runs neither in prod nor dev')
            exit()
        elif prod == False and dev == False and env == 'dev':
            logger.info('runs neither in prod nor dev')
            exit()
        # BOTH ARE TRUE
        elif env is None:
            logger.info('runs in prod and dev')
            pass
        elif prod == True and dev == True and env == 'prod':
            logger.info('runs in prod and dev')
            pass
        elif prod == True and dev == True and env == 'dev':
            logger.info('runs in prod and dev')
            pass
        # ONLY ONE IS TRUE
        elif prod == True and dev == False and env == 'prod':
            logger.info('runs in prod only')
            pass
        elif prod == True and dev == False and env == 'dev':
            logger.info('not running in dev - exit script')
            exit()
        elif prod == False and dev == True and env == 'dev':
            logger.info('runs in dev only')
            pass
        elif prod == False and dev == True and env == 'prod':
            logger.info('not running in prod - exit script')
            exit()
    except Exception as exc:
        logger.info('Wrong input to the deployment function. Not running in any env. \nError message: {}'.format(exc))


def send_slack_alert(hook_url, slack_channel, slack_msg_text):
    """
    Send slack message by using Incoming Webhook.
    :param hook_url: to generate Hook URL, follow the instructions from https://api.slack.com/messaging/webhooks
    :param slack_channel: ex. #general
    :param slack_msg_text: string of message
    :return: nothing, just post HTTP request to slack
    """
    logger = set_logging()
    slack_message = {'channel': slack_channel, 'text': slack_msg_text}

    req = Request(hook_url, json.dumps(slack_message).encode('utf-8'))
    try:
        response = urlopen(req)
        response.read()
        logger.info("Message posted to %s", slack_message['channel'])
    except HTTPError as e:
        logger.error("Request failed: %d %s", e.code, e.reason)
    except URLError as e:
        logger.error("Server connection failed: %s", e.reason)


def merge_tmp_into_target_tbl(exa_connection, dataframe, pk_columns,
                              exasol_schema, exasol_table, temp_schema=None, temp_tbl=None):
    """
    This function:
    - truncates the TMP table
    - loads the dataframe into Temp schema in Exasol
    - merges the Temp table into original table
    :param exa_connection: connection object to Exasol
    :param dataframe: any pandas dataframe
    :param pk_columns: PK columns as comma separated string ex. "COL1,COL2"
    :param exasol_schema: ex. STAGE_FLAT_FILE
    :param exasol_table: ex. TABLE_NAME
    :param temp_schema: can be explicitly defined if the use case deviates from standard form: SCHEMA.TBL + SCHEMA_TMP.TBL
    :param temp_tbl: can be explicitly defined if the use case deviates from standard form: SCHEMA.TBL + SCHEMA_TMP.TBL
    :return: nothing, just load transformed df into Exasol
    """
    connection = exa_connection
    logger = set_logging()
    tmp_table = exasol_table if temp_tbl is None else temp_tbl
    tmp_schema = exasol_schema + "_TMP" if temp_schema is None else temp_schema
    logger.info("Truncating table: {}.{}".format(tmp_schema, tmp_table))
    connection.execute('''TRUNCATE TABLE {}.{}'''.format(tmp_schema, tmp_table))
    # import to TMP SCHEMA
    connection.import_from_pandas(dataframe, (tmp_schema, tmp_table))
    logger.info("Data loaded successfully to the Temp table - {}.{}".format(tmp_schema, tmp_table))
    # =================================================================================================================
    logger.info("Start merging...")
    pk_columns_list = [str(item).strip() for item in pk_columns.split(',')]
    merge_query = '''MERGE INTO {schema}.{tbl} target_tbl USING {schema_tmp}.{tbl_tmp} tmp ON ('''
    for i in pk_columns_list:
        merge_query += 'target_tbl.\"' + i + '\"' + ''' = tmp."''' + i + '\" AND '
        # ON (target_tbl."BRAND" = tmp."BRAND" AND target_tbl."CATEGORY" = tmp."CATEGORY")
    merge_query = merge_query[:-5] + ''') '''
    merge_query += '''WHEN MATCHED THEN UPDATE SET target_tbl."UPDATE_TIMESTAMP" = tmp."UPDATE_TIMESTAMP"'''

    # remove pk_columns, otherwise we would be updating columns of ON-condition
    cols_not_to_merge = pk_columns_list + ["INSERT_TIMESTAMP", "UPDATE_TIMESTAMP"]
    # logger.info("COLS NOT TO MERGE: {}".format(cols_not_to_merge))
    dataframe.columns = dataframe.columns.str.strip()
    cols_merge = [col for col in dataframe.columns if col not in cols_not_to_merge]
    # logger.info("COLS TO MERGE: {}".format(cols_merge))
    for i in cols_merge:
        merge_query += ''', target_tbl."''' + i.strip() + '''" = tmp."''' + i.strip() + '''"'''

    # when not matched then insert all cols incl. PK columns
    merge_query += ''' WHEN NOT MATCHED THEN INSERT ("INSERT_TIMESTAMP", "UPDATE_TIMESTAMP", ''' + \
                   '\"' + '", "'.join(pk_columns_list + cols_merge) + '\"' + \
                   ''') VALUES (tmp."INSERT_TIMESTAMP", tmp."UPDATE_TIMESTAMP"'''

    for i in pk_columns_list + cols_merge:
        merge_query += ''', tmp."''' + i.strip() + '''"'''

    merge_query += ''');'''
    connection.execute(merge_query.format(schema=exasol_schema, tbl=exasol_table,
                                          schema_tmp=tmp_schema, tbl_tmp=tmp_table))
    check_df = connection.export_to_pandas(f"""SELECT COUNT(*) AS COUNT_ROWS FROM {exasol_schema}.{exasol_table} 
                                    WHERE TO_DATE(UPDATE_TIMESTAMP) = CURRENT_DATE;""")
    logger.info(f"""{check_df["COUNT_ROWS"][0]} rows inserted today.""")
    logger.info("-------------- MERGE TO {}.{} COMPLETE -----------------------".format(exasol_schema, exasol_table))


def return_exa_conn(exa_user='DWHEXA_USER', exa_pwd='DWHEXA_PASSWORD', exa_dsn='DWHEXA_HOST'):
    """
    :return: connection object
    """
    logger = set_logging()
    exasol_user = os.environ.get(exa_user)
    exasol_pwd = os.environ.get(exa_pwd)
    exasol_dsn = os.environ.get(exa_dsn)
    conn = pyexasol.connect(user=exasol_user, password=exasol_pwd, dsn=exasol_dsn)
    logger.info('Successfully connected to Exasol.')
    return conn


def return_df_from_sql_script(filename, exa_conn):
    """
    :param filename: SQL script ex. "script.sql"
    :param exa_conn: name of the connection object
    :return: pandas df
    """
    logger = set_logging()
    sql_script = open(filename, 'r')  # Open and read the file as a single buffer
    query = sql_script.read()
    sql_script.close()
    try:
        tbl_from_sql = exa_conn.export_to_pandas(query)
        return tbl_from_sql
    except Exception as exc:
        logger.info(f"Couldn't read the query. Error msg: {exc}")


def execute_sql_script(filename, exa_conn):
    """
    :param filename: SQL script ex. "script.sql"
    :param exa_conn: name of the connection object
    :return: nothing, just execute SQL in Exasol
    """
    logger = set_logging()
    sql_script = open(filename, 'r')  # Open and read the file as a single buffer
    query = sql_script.read()
    sql_script.close()
    try:
        exa_conn.execute(query)
    except Exception as exc:
        logger.info(f"Couldn't read the query. Error msg: {exc}")


# =================================================================================================================
# HELPER FUNCTIONS
# =================================================================================================================
def print_full(df_to_print):
    """
    Helper function to print full df into the console
    :param df_to_print: dataframe
    :return: prints the FULL DATAFRAME
    """
    pd.set_option('display.max_rows', len(df_to_print))
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000)
    pd.set_option('display.float_format', '{:20,.2f}'.format)
    # pd.set_option('display.max_colwidth', -1)
    print(df_to_print)
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')
    pd.reset_option('display.max_colwidth')


def print_df_statistics(dataframe):
    """
    Takes df and prints nr of cols, rows and all column names
    :param dataframe: df to print information for
    :return: print info
    """
    logger = set_logging()
    logger.info("------------ Printing Dataframe statistics -----------------------")
    logger.info("Dataframe has {} rows and {} columns.\n".format(len(dataframe), len(dataframe.columns)))
    logger.info("The columns are: {}\n".format(dataframe.columns.tolist()))
    logger.info("------------ End of Dataframe statistics -----------------------")


def establish_boto3_client(aws_access_key, aws_secret_key, aws_service='s3', region='eu-central-1'):
    """
    Helper function to establish Boto3 client
    :param aws_access_key: public key
    :param aws_secret_key: secret key
    :param aws_service: ex. sns, ecr, s3 -> default = 's3'
    :param region: default 'eu-central-1', but can be changed
    :return: s3 client
    """
    logger = set_logging()
    try:
        s3_client = boto3.client(aws_service,
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 region_name=region)
        logger.info("Boto3 client for S3 established.\n")
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except ClientError as exc:
        if exc.response['Error']['Code'] == "404":
            logger.warning("The object does not exist.")
        else:
            logger.warning(f"Unexpected error: {exc}")
        return False
    return s3_client


def extract_key(dictionary, path):
    """
    to get elements from a dict
    :param dictionary: dict from JSON file
    :param path: defines how to access nested objects in a df ex. name.surname instead of [name][surname]
    :return: nested dict element if the key exists in the dict, otherwise returns None
    """
    keys = path.split('.')  # to get deeper level key
    return reduce(lambda d, key: d[int(key)] if isinstance(d, list) else d.get(key) if d else None, keys, dictionary)


def parse_timestamp(x):
    """
    timestamps that are in the format of 2019-12-12T15:22:04.558Z
    :param x: wrong timestamp format
    :return: corrected timestamp format compatible with Exasol as string
    """
    if x is None:
        return None
    else:
        return x[0:10] + ' ' + x[11:-1]


def check_for_key(x, key_name='id'):
    """
    helper method after pd explode
    :param x: dict
    :param key_name: dict key
    :return: value for specific key if this key exists
    """
    if type(x) == dict:
        return x.get(key_name, "empty")
    else:
        return None
        
        
def get_ct_token(CT_CLIENT_ID, CT_CLIENT_PWD):
    """
    simple http request to get bearer token from commercetools 
    :param CT_CLIENT_ID: commercetool client id
    :param CT_CLIENT_PWD: commercetool client password
    :return headers for http request to commercetools
    """
    data = {
        'grant_type': 'client_credentials'
           }

    response = requests.post('https://auth.europe-west1.gcp.commercetools.com/oauth/token', data=data, auth=(CT_CLIENT_ID, CT_CLIENT_PWD))
    headers = { 'Authorization': 'Bearer ' + str(response.json()['access_token']),}
    
    return headers


def print_merge_query(pk_columns,exasol_schema, exasol_table, temp_schema=None, temp_tbl=None,
                      exa_user='DWHEXA_USER', exa_pwd='DWHEXA_PASSWORD', exa_dsn='DWHEXA_HOST', env_file=".env"):
    """Usage example:
    print_merge_query(pk_columns="VISIT_DATE, RETURNING_CUSTOMER, DEVICE_TYPE, \
                        CHANNEL_LVL_1, CHANNEL_LVL_2, COST_CATEGORY, COSTCENTER_NAME, ACCOUNT_NAME, CREDITOR, SITE_DOMAIN",
                        exasol_schema="BUSINESS_VAULT", exasol_table="ATTRIBUTED_MARKETING_COSTS_VISITS")
    """
    dotenv_path = join(dirname(""), '.env')
    load_dotenv(dotenv_path)
    connection = return_exa_conn(exa_user='DWHEXA_USER', exa_pwd='DWHEXA_PASSWORD', exa_dsn='DWHEXA_HOST')
    tmp_table = exasol_table if temp_tbl is None else temp_tbl
    tmp_schema = exasol_schema + "_TMP" if temp_schema is None else temp_schema
    dataframe = connection.export_to_pandas(f"SELECT * FROM {tmp_schema}.{tmp_table}")
    pk_columns_list = [str(item).strip() for item in pk_columns.split(',')]
    merge_query = '''MERGE INTO {schema}.{tbl} target_tbl USING {schema_tmp}.{tbl_tmp} tmp ON ('''
    for i in pk_columns_list:
        merge_query += 'target_tbl.\"' + i + '\"' + ''' = tmp."''' + i + '\" AND '
        # example: ON (target_tbl."BRAND" = tmp."BRAND" AND target_tbl."CATEGORY" = tmp."CATEGORY")
    merge_query = merge_query[:-5] + ''') '''
    merge_query += '''WHEN MATCHED THEN UPDATE SET target_tbl."UPDATE_TIMESTAMP" = tmp."UPDATE_TIMESTAMP"'''

    # remove pk_columns, otherwise we would be updating columns of ON-condition
    cols_not_to_merge = pk_columns_list + ["INSERT_TIMESTAMP", "UPDATE_TIMESTAMP"]
    dataframe.columns = dataframe.columns.str.strip()
    cols_merge = [col for col in dataframe.columns if col not in cols_not_to_merge]
    for i in cols_merge:
        merge_query += ''', target_tbl."''' + i.strip() + '''" = tmp."''' + i.strip() + '''"'''

    # when not matched then insert all cols incl. PK columns
    merge_query += ''' WHEN NOT MATCHED THEN INSERT ("INSERT_TIMESTAMP", "UPDATE_TIMESTAMP", ''' + \
                   '\"' + '", "'.join(pk_columns_list + cols_merge) + '\"' + \
                   ''') VALUES (tmp."INSERT_TIMESTAMP", tmp."UPDATE_TIMESTAMP"'''

    for i in pk_columns_list + cols_merge:
        merge_query += ''', tmp."''' + i.strip() + '''"'''

    merge_query += ''');'''
    print(merge_query.format(schema=exasol_schema, tbl=exasol_table, schema_tmp=tmp_schema, tbl_tmp=tmp_table))


def explode_list_cols_and_normalize_json(dframe, list_cols):
    """
    helper function for commercetool data normalization
    """
    logger = set_logging()
    shape_before_exploding = dframe.shape
    # explode all lists so that we get nice dict
    for col in dframe.columns:
        try:
            if col in list_cols:
                logger.info(f"Exploding {col}")
                dframe = dframe.explode(col).reset_index(drop=True)
                # json_normalize all those columns and concat them back to the original df
                temp_df = dframe[dframe[col].notnull()]
                if not temp_df.empty:
                    temp_df = pd.json_normalize(temp_df[col]).add_prefix(f"{col}__")
                    dframe = pd.concat([dframe, temp_df], axis=1, ignore_index=False)
        except Exception as exc:
            logger.info(f"Error: {exc}")
    shape_after_exploding = dframe.shape
    logger.info(f"Shape before: {shape_before_exploding}, "
          f"Shape after: {shape_after_exploding}")
    return dframe


def check_list_cols_in_df(dframe):
    """
    helper function for commercetool data normalization
    """
    logger = set_logging()
    all_dtypes = (dframe.applymap(type) == list).all()
    list_cols = all_dtypes.index[all_dtypes].tolist()
    if len(list_cols) > 0:
        return True, list_cols
    else:
        return False, list_cols


def process_response_from_commercetools(resp_dict, columns=[]):
    """
    if columns is empty all elements from the response are processed
    if not only relevant columns will get normalized
    :param resp_dict: reponse from API
    :param columns: list of columns to process 
    :return: normalized df
    """
    logger = set_logging()
    
    if len(columns) > 0: # in case only certain columns should be normalized /considered
        df = pd.json_normalize(resp_dict)
        this_df = pd.DataFrame(df[columns])
    else:
        this_df = pd.json_normalize(resp_dict)

    while check_list_cols_in_df(this_df)[0]:  # while True
        all_list_cols = check_list_cols_in_df(this_df)[1]
        this_df = explode_list_cols_and_normalize_json(this_df, all_list_cols)
    else:
        logger.info("No more list cols! All done")
    return this_df


def basic_ct_pagination(CT_CLIENT_ID, CT_CLIENT_PWD, ENDPOINT, columns=[]):
    """
    simple batch pagnination for each endpoint with the option to define whether only certain
    columns should be normalized
    :param CT_CLIENT_ID: CLIENT ID from commercetool for auth
    :param CT_CLIENT_PWD: CLIENT PWD from commercetool for auth
    :param ENDPOINT: e.g. products, categories, ...
    :param columns: default all - otherwise needs specification
    :return: df
    """
    logger = set_logging()
    ''' first making initial API request and then pagination '''
    headers = get_ct_token(CT_CLIENT_ID, CT_CLIENT_PWD)

    x = 0
    logger.info('Current offset: %s', x)
    df = pd.DataFrame()

    initial_request = requests.get('https://api.europe-west1.gcp.commercetools.com/flaconi-dev/' + ENDPOINT + '?limit=500', 
                                    headers=headers)
    
    df = process_response_from_commercetools(initial_request.json()['results'], columns)

    while True:
        
        x +=  initial_request.json()['count'] + initial_request.json()['offset'] 
        logger.info('New offset: : %s', x)

        response = requests.get('https://api.europe-west1.gcp.commercetools.com/flaconi-dev/' + ENDPOINT + '?limit=500&offset=' + str(x), 
                                headers=headers)
        
        if response.json()['offset'] < initial_request.json()['total']:
            tmp = process_response_from_commercetools(response.json()['results'], columns)
            df = pd.concat([tmp, df]) # combine df's
        else:
            break

    return df


def ct_pagination_by_sort_key(CT_CLIENT_ID, CT_CLIENT_PWD, ENDPOINT, SORT_KEY, columns=[]):
    """
    simple batch pagnination for each endpoint with the option to define whether only certain
    columns should be normalized - and results are sorted (recommended way)
    :param CT_CLIENT_ID: CLIENT ID from commercetool for auth
    :param CT_CLIENT_PWD: CLIENT PWD from commercetool for auth
    :param ENDPOINT: e.g. products, categories, ...
    :param SORT_KEY: column to sort by
    :param columns: default all - otherwise needs specification
    :return: df
    """
    logger = set_logging()

    ''' first making initial API request and then pagination '''
    headers = get_ct_token(CT_CLIENT_ID, CT_CLIENT_PWD)

    initial_request = requests.get('https://api.europe-west1.gcp.commercetools.com/flaconi-dev/' + ENDPOINT + '?limit=500&sort=' + SORT_KEY + '+asc', 
                                    headers=headers)
    
    df = process_response_from_commercetools(initial_request.json()['results'], columns)

    last_sort_value = initial_request.json()['results'][-1][SORT_KEY]

    logger.info("First sort value: " + last_sort_value)

    while True:

        response = requests.get('https://api.europe-west1.gcp.commercetools.com/flaconi-dev/' + 
                                ENDPOINT + '?limit=500&sort=' + SORT_KEY + '+asc&where=' + SORT_KEY + '%3E"' + last_sort_value + '"', headers=headers)

        
        if len(response.json()['results']) > 0:
            last_sort_value = response.json()['results'][-1][SORT_KEY]
            logger.info("Current sort value: " + last_sort_value)
            tmp = process_response_from_commercetools(response.json()['results'], columns)
            df = pd.concat([tmp, df], copy = False) # combine df's
            del tmp
            del response
        else:
            break

    return df