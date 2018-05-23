from hiev_datacrate import *
from hiev_datacrate.hiev import *


# Set api token for interacting with the HIEv
api_token = os.environ['HIEV_API_KEY']

records = search(api_token, experiments=['82'], upload_from_date="2018-04-01")
# records = search(api_token, uploader_id='16', upload_from_date="2017-04-01", stati='PROCESSED')

# Supply the dataset level metadata upfront before passing everything to be datacrated
dataset_md = {
    "description": "This is a test datacrate of files pulled automatically from the HIEv application",
    "startTime": "2016-01-21T11:00:00+11:00",
    "endTime": "2016-11-21T11:00:00+11:00"
}

datacrate(api_token, records, dataset_md)