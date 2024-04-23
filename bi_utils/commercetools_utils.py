import pandas as pd
from datetime import datetime
import requests
from bi_utils.utils import set_logging, return_exa_conn

logger = set_logging()


def parse_exa_to_ct_timestamp(exa_time):
    """
    Used for the delta load logic so that we can make API request only for data starting from the MAX(timestamp) from Exasol
    :param exa_time: Exasol timestamp from the query
    :return: Commerce Tools friendly timestamp
    """
    timestamp = datetime.strftime(pd.to_datetime(exa_time), "%Y-%m-%dT%H:%M:%S%Z")
    return timestamp


def get_max_modified_date_from_dwh(tbl_name='ORDERS', timestamp_colname='LAST_MODIFIED_AT', diff_in_min=60):
    """
    Get the latest timestamp from CT table - used for Delta Load
    :param tbl_name: CT table
    :param timestamp_colname: timestamp column that we want to use for Delta Load
    :param diff_in_min: go back X number of minutes from the last timestamp
    :return: the latest timestamp
    """
    conn = return_exa_conn()
    # with INTERVAL MINUTE, HOUR and DAY Exasol only allows values below 100, so diff_in_min can be between 0 and 99
    q = f"""SELECT MAX({timestamp_colname}) - INTERVAL '{diff_in_min}' MINUTE FROM STAGE_COMMERCETOOL.{tbl_name};"""
    t = conn.export_to_list(q)
    conn.close()
    if len(t[0]) > 0:  # i.e. if the DWH table is not empty
        max_last_modified_at = parse_exa_to_ct_timestamp(t[0][0])
        logger.info(f"MAX TIMESTAMP from Exasol: {max_last_modified_at}")
    else:
        max_last_modified_at = None
    return max_last_modified_at


def get_ct_token(ct_client_id, clt_client_pwd):
    """
    simple http request to get bearer token from commercetools
    :param ct_client_id: commercetool client id
    :param clt_client_pwd: commercetool client password
    :return headers for http request to commercetools
    """
    data = {'grant_type': 'client_credentials'}
    response = requests.post('https://auth.europe-west1.gcp.commercetools.com/oauth/token', data=data,
                             auth=(ct_client_id, clt_client_pwd))
    headers = {'Authorization': 'Bearer ' + str(response.json()['access_token']), }
    return headers


def explode_list_cols_and_normalize_json(dframe, list_cols):
    """
    Explode all list attributes to separate rows for the relevant attributes.
    After all list columns have been exploded, we are left with many Dictionary values
        - we want to normalize them to DFrame columns by using json_normalize()
    Those normalized values may again have list columns with dictionaries nested inside them - therefore we use while loop
    in process_response_from_commercetools that calls this function until everything is normalized
    :param dframe: dataframe created from response Dict
    :param list_cols: list of columns to process as a list - those should be columns that include lists and dictionaries
    :return: transformed df where all attributed that contained lists
    (ex. list of product prices per country for each product)
    have been exploded to separate rows + all dictionaries have been normalized to DFrame columns
    """
    shape_before_exploding = dframe.shape
    for col in dframe.columns:
        try:
            if col in list_cols:
                logger.info(f"Exploding {col}")
                dframe = dframe.explode(col).reset_index(drop=True)
                logger.info(f"=========== SHAPE AFTER EXPLODING {col}: {dframe.shape} ===============")
                # json_normalize all those columns and concat them back to the original df
                temp_df = dframe[dframe[col].notnull()]  # if list was empty, we get NULLs
                if not temp_df.empty:
                    temp_df = pd.json_normalize(temp_df[col]).add_prefix(f"{col}__")
                    dframe = pd.concat([dframe, temp_df], axis=1, ignore_index=False)
                    del temp_df
            else:
                pass
        except Exception as exc:
            logger.info(f"Error: {exc}")
    shape_after_exploding = dframe.shape
    logger.info(f"Shape before: {shape_before_exploding}, "
                f"Shape after: {shape_after_exploding}")
    return dframe


def check_list_cols_in_df(dframe, cols_to_exclude=None):
    """
    helper function for commercetool data normalization
    :param dframe: dataframe for which we need to check whether there are any list cols. If so we return a tuple: (True, list_cols)
    :param cols_to_exclude: since this function is used to determine which cols to explode (only columns that contain lists must be "exploded"),
            we included this parameter to be able to exclude some cols from being exploded. Reason: to limit the nr of rows that need to be processes
            Ex. we might need to initially explode the column "lineItems" but within it there is a column "variant" which we don't want to explode.
            If we set cols_to_exclude=["variant"], then this attribute will not be exploded.
    :return: a tuple: (True, list_cols) or: (False, [])
    """
    cols_to_exclude_from_explode = cols_to_exclude if cols_to_exclude is not None else []
    all_dtypes = (dframe.applymap(type) == list).all()
    cols_incl_lists = all_dtypes.index[all_dtypes].tolist()
    list_cols = [i for i in cols_incl_lists if i not in cols_to_exclude_from_explode]
    if len(list_cols) > 0:
        return True, list_cols
    else:
        return False, list_cols


def process_response_from_commercetools(resp_dict, columns=None, cols_to_exclude=None):
    """
    if columns=None (default), then all elements from the response dict are processed
    if columns=['col1', 'col2'], then only columns col1 and col2 will get normalized
    Also, we can provide a list of columns to exclude from normalization, which will be passed to check_list_cols_in_df()
    :param resp_dict: reponse from API
    :param columns: list of columns to process - default None to process ALL columns without excluding anything
    :param cols_to_exclude: list of columns to exclude from normalization
            (normalization means: exploding lists and normalizing dictionaries)
    :return: normalized df based on response dictionary
    """
    cols = columns if columns is not None else []
    cols_to_exclude_from_explode = cols_to_exclude if cols_to_exclude is not None else []

    if len(cols) > 0:  # in case only certain columns should be normalized /considered
        df = pd.json_normalize(resp_dict)
        this_df = df.loc[:, df.columns.isin(cols)]  # only filter out cols if they actually exist in the API response
        # this_df = df[cols]  # this would work if we would get all columns that we need from the API - this is not always the case
    else:
        # initial json_normalize() results in many other list and dict column, which we then process in the while loop
        # until all relevant attributes are in form suitable for DWH i.e. strings/numeric columns (no longer lists and dicts)
        this_df = pd.json_normalize(resp_dict)

    while check_list_cols_in_df(this_df, cols_to_exclude_from_explode)[0]:  # while True
        all_list_cols = check_list_cols_in_df(this_df, cols_to_exclude_from_explode)[1]
        this_df = explode_list_cols_and_normalize_json(this_df, all_list_cols)
    else:
        logger.info("No more list cols! All done\n")
    return this_df


def basic_ct_pagination(ct_client_id, ct_client_pwd, endpoint, columns=None, cols_to_exclude=None):
    """
    simple batch pagnination for each endpoint with the option to define whether only certain
    columns should be normalized
    :param ct_client_id: CLIENT ID from commercetool for auth
    :param ct_client_pwd: CLIENT PWD from commercetool for auth
    :param endpoint: e.g. products, categories, orders, ...
    :param columns: default None, which means all columns - otherwise needs column specification
    :param cols_to_exclude: list of columns which we don't want to explode and normalize
    :return: df concatenated from all API requests + transformed
    """
    ''' first making initial API request and then pagination '''
    headers = get_ct_token(ct_client_id, ct_client_pwd)

    x = 0
    logger.info('Current offset: %s', x)
    initial_request = requests.get(
        'https://api.europe-west1.gcp.commercetools.com/flaconi-prod/' + endpoint + '?limit=500',
        headers=headers)
    df = process_response_from_commercetools(initial_request.json()['results'], columns, cols_to_exclude)

    while True:
        x += initial_request.json()['count'] + initial_request.json()['offset']
        logger.info('New offset: : %s', x)
        response = requests.get(
            'https://api.europe-west1.gcp.commercetools.com/flaconi-prod/' + endpoint + '?limit=500&offset=' + str(x),
            headers=headers)

        if response.json()['offset'] < initial_request.json()['total']:
            tmp = process_response_from_commercetools(response.json()['results'], columns, cols_to_exclude)
            df = pd.concat([tmp, df])  # combine df's
        else:
            break
    logger.info(f"Shape of the final df after pagination: {df.shape}")
    return df


def ct_pagination_by_sort_key(ct_client_id, clt_client_pwd, endpoint, sort_key, max_timestamp=None,
                              columns=None, cols_to_exclude=None, staged=True):
    """
    simple batch pagnination for each endpoint with the option to define whether only certain
    columns should be normalized - and results are sorted (recommended way)
    :param ct_client_id: CLIENT ID from commercetool for auth
    :param clt_client_pwd: CLIENT PWD from commercetool for auth
    :param endpoint: e.g. products, categories, orders, ...
    :param sort_key: column to sort by
    :param max_timestamp: MAX(timestamp) from CT table - we then make API request for all entries >= max_timestamp. Fallback: 01.01.2020
    :param columns: default None, which means all columns - otherwise needs column specification
    :param cols_to_exclude: list of columns which we don't want to explode and normalize
    :param staged: if staged is set to False, then &staged=false will be added to the request. It's used for product-projections
            (to just get the current and not staged data) - replaces ct_pagination_current_products_by_sort_key()
    :return: df concatenated from all API requests + transformed
    """
    # make full load from 2020-01-01 if provided max_timestamp is None
    max_time = max_timestamp if max_timestamp is not None else '2020-01-01T00:00:00'  # TODO: adapt after go live if necessary
    logger.info(f"MAX TIMESTAMP provided for the API request: {max_time}")

    headers = get_ct_token(ct_client_id, clt_client_pwd)
    base_url = 'https://api.europe-west1.gcp.commercetools.com/flaconi-prod/'

    # initial request's URL. Example: base_url + orders?where=lastModifiedAt%3E%3D%222020-05-29T18%3A05%3A40%22&limit=500&offset=0&sort=lastModifiedAt%20asc
    init_req_url = base_url + endpoint + '?where=' + sort_key + '%3E%3D%22' + max_time + '%22&limit=500&sort=' + sort_key + '%20asc' + '&withTotal=false'
    if staged:
        full_url_init_req = init_req_url
    else:
        full_url_init_req = init_req_url + '&staged=false'

    # make the initial API request and then start pagination
    logger.info(f"INITIAL REQUEST URL: {full_url_init_req}")
    initial_request = requests.get(full_url_init_req, headers=headers)
    initial_request_json = initial_request.json()
    status_code = initial_request_json.get('statusCode')
    msg = initial_request_json.get('message')
    res = initial_request_json.get('results')
    if not res:  # i.e. res is either None or []
        logger.info(f'Request failed. We got status code: {status_code}. Error message: {msg}. ' +
                    f'Full response JSON: {initial_request_json}')
    else:
        df = process_response_from_commercetools(initial_request_json['results'], columns, cols_to_exclude)
        last_sort_value = initial_request_json['results'][-1][sort_key]
        logger.info("Current sort value: " + last_sort_value)

        while True:
            # make subsequent API requests
            subs_req_url = base_url + endpoint + '?limit=500&withTotal=false&sort=' + sort_key + '+asc&where=' + sort_key + '%3E"' + last_sort_value + '"'
            if staged:
                full_subs_req_url = subs_req_url
            else:
                full_subs_req_url = subs_req_url + '&staged=false'

            logger.info(f'Current URL: {full_subs_req_url}')
            response = requests.get(full_subs_req_url, headers=headers)
            results = response.json().get('results')
            if len(results) > 0:
                last_sort_value = results[-1][sort_key]
                logger.info("Next sort value: " + last_sort_value)
                tmp = process_response_from_commercetools(results, columns, cols_to_exclude)
                df = pd.concat([tmp, df], copy=False)  # combine df's
                del tmp
                del response
            else:
                break
        logger.info(f"Shape of the final df after pagination: {df.shape}")
        return df

def ct_pagination_by_sort_key_limit(ct_client_id, clt_client_pwd, endpoint, sort_key, max_timestamp=None,
                              columns=None, cols_to_exclude=None, staged=True, limit=250, max_iterations=500):
    """
    simple batch pagnination for each endpoint with the option to define whether only certain
    columns should be normalized - and results are sorted (recommended way)
    :param ct_client_id: CLIENT ID from commercetool for auth
    :param clt_client_pwd: CLIENT PWD from commercetool for auth
    :param endpoint: e.g. products, categories, orders, ...
    :param sort_key: column to sort by
    :param max_timestamp: MAX(timestamp) from CT table - we then make API request for all entries >= max_timestamp. Fallback: 01.01.2020
    :param columns: default None, which means all columns - otherwise needs column specification
    :param cols_to_exclude: list of columns which we don't want to explode and normalize
    :param staged: if staged is set to False, then &staged=false will be added to the request. It's used for product-projections
            (to just get the current and not staged data) - replaces ct_pagination_current_products_by_sort_key()
    :param max_iterations: number of batch iterations. we limit the default to 250 to not get more than 125k records to avoid memory issues
                     this is a potential issues. in a follow up ticket we will change this
    :return: df concatenated from all API requests + transformed
    """
    if limit>250:
        logger.info(f'The limit {limit} is higher than allowed. It should not be higher than 250.')
        raise ValueError("An error occurred in ct_pagination_by_sort_key_limit")
    
    # make full load from 2020-01-01 if provided max_timestamp is None
    iteration = 0
    max_time = max_timestamp if max_timestamp is not None else '2020-01-01T00:00:00'  # TODO: adapt after go live if necessary
    logger.info(f"MAX TIMESTAMP provided for the API request: {max_time}")

    headers = get_ct_token(ct_client_id, clt_client_pwd)
    base_url = 'https://api.europe-west1.gcp.commercetools.com/flaconi-prod/'

    # initial request's URL. Example: base_url + orders?where=lastModifiedAt%3E%3D%222020-05-29T18%3A05%3A40%22&limit=500&offset=0&sort=lastModifiedAt%20asc
    init_req_url = base_url + endpoint + '?where=' + sort_key + '%3E%3D%22' + max_time + '%22&limit=' + str(limit) + '&sort=' + sort_key + '%20asc' + '&withTotal=false'
    if staged:
        full_url_init_req = init_req_url
    else:
        full_url_init_req = init_req_url + '&staged=false'

    # make the initial API request and then start pagination
    logger.info(f"INITIAL REQUEST URL: {full_url_init_req}")
    initial_request = requests.get(full_url_init_req, headers=headers)
    initial_request_json = initial_request.json()
    status_code = initial_request_json.get('statusCode')
    msg = initial_request_json.get('message')
    res = initial_request_json.get('results')
    if not res:  # i.e. res is either None or []
        logger.info(f'Request failed. We got status code: {status_code}. Error message: {msg}. ' +
                    f'Full response JSON: {initial_request_json}')
    else:
        df = process_response_from_commercetools(initial_request_json['results'], columns, cols_to_exclude)
        last_sort_value = initial_request_json['results'][-1][sort_key]
        logger.info("Current sort value: " + last_sort_value)

        while True:
            # make subsequent API requests
            subs_req_url = base_url + endpoint + '?limit=' + str(limit) + '&withTotal=false&sort=' + sort_key + '+asc&where=' + sort_key + '%3E"' + last_sort_value + '"'
            if staged:
                full_subs_req_url = subs_req_url
            else:
                full_subs_req_url = subs_req_url + '&staged=false'

            logger.info(f'Current URL: {full_subs_req_url}')
            response = requests.get(full_subs_req_url, headers=headers)
            results = response.json().get('results')
            
            if len(results) > 0 and iteration < max_iterations:
                last_sort_value = results[-1][sort_key]
                logger.info("Next sort value: " + last_sort_value)
                tmp = process_response_from_commercetools(results, columns, cols_to_exclude)
                df = pd.concat([tmp, df], copy=False, ignore_index=True)  # combine df's
                iteration += 1
                logger.info(f'Iteration {iteration} from at most {max_iterations} iterations')
                del tmp
                del response
            else:
                break
        logger.info(f"Shape of the final df after pagination: {df.shape}")
        return df