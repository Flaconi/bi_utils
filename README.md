# The purpose of this repo 
The goal of this repo is to have a common utils library that will be shared between DE and DS teams to avoid duplication and maintenance efforts

## Content
This directory should contain utility functions so that we can avoid repeating ourselves.
Since we can only package a single directory into a docker image, you should copy the utils function from this directory
and use it for each of your projects (i.e. for each docker image in this repo).

### Functions included
- `deployment()` function  - to make sure the code is the same in both envs - it simply not runs in DEV or PROD if this is not desired
- `set_logging()` - to avoid copy-pasting logger setup
- `send_slack_alert()` - generic function to send Slack message
- helper functions ex. `establish_boto3_client()` to establish Boto3 client for any AWS service

