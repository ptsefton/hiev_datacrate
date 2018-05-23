from hiev_datacrate import *
from hiev_datacrate.hiev import *


# Set api token for interacting with the HIEv
api_token = os.environ['HIEV_API_KEY']

records = search(api_token, experiments=['82'], upload_from_date="2018-05-01")
datacrate(api_token, records)