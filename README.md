# The purpose of this repo 
The goal of this repo is to have a common utility library that will be shared between DE and DS teams to avoid duplication and maintenance efforts

## Content
This directory should contain utility functions so that we can avoid repeating ourselves.
You can install this package in your Docker image via:
    
    RUN pip install git+https://github.com/Flaconi/bi-utils.git

Then in your Python script, you can import it:

    from bi-utils import utils

    # usage
    logger = utils.set_logging()
    utils.deployment(prod=True, dev=True)


### Functions included (among others to come)
- `deployment()` function  - to make sure the code is the same in both envs - it simply not runs in DEV or PROD if this is not desired
- `set_logging()` - to avoid copy-pasting logger setup
- `send_slack_alert()` - generic function to send Slack message
- helper functions ex. `establish_boto3_client()` to establish Boto3 client for any AWS service

# Docs
https://flaconi.atlassian.net/wiki/spaces/1BI/pages/263946847/How+to+use+bi-utils+package+in+bi-python+projects 