'''
Domo Python SDK Usage
!!! NOTICE !!!: This SDK is written for Python3. Python2 is not compatible. Execute all scripts via 'python3 myFile.py'
- To run this example file:
-- Copy and paste the contents of this file
-- Plug in your CLIENT_ID and CLIENT_SECRET (https://developer.domo.com/manage-clients), and execute "python3 run_examples.py"
- These tests clean up after themselves; several objects are created and deleted on your Domo instance
- If you encounter a 'Not Allowed' error, this is a permissions issue. Please speak with your Domo Administrator.
- If you encounter a 'Forbidden' error, your OAuth client is likely missing the scope required for that endpoint
- Note that the Domo objects used in these examples are dictionaries that prevent you from accidentally setting the wrong fields.
- Standard dictionaries may be supplied instead of the defined objects (Schema, DataSetRequest, etc)
- All returned objects are dictionaries
'''

import logging
import os
import random
from pydomo import Domo
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType, Policy
from pydomo.datasets import PolicyFilter, FilterOperator, PolicyType, Sorting
from pydomo.users import CreateUserRequest
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
from pydomo.streams import UpdateMethod, CreateStreamRequest
from pydomo.groups import CreateGroupRequest
from tableschema import Table

inputdir = # PUT YOUR FOLDER LOCATION HERE

# My Domo Client ID and Secret (https://developer.domo.com/manage-clients)
# CLIENT_ID = 'MY_CLIENT_ID'
# CLIENT_SECRET = 'MY_CLIENT_SECRET'

CLIENT_ID = # GET YOUR CLIENT ID AND PUT IT HERE
CLIENT_SECRET = # GET YOUR CLIENT SECRET AND PUT IT HERE

# The Domo API host domain. This can be changed as needed - for use with a proxy or test environment
API_HOST = 'api.domo.com'


class directoryUpload:
	def __init__(self):
		domo = self.init_domo_client(CLIENT_ID, CLIENT_SECRET)
		self.datasets(domo)
		# self.streams(domo)
		# self.users(domo)
		# self.groups(domo)
		# self.pages(domo)

	def init_domo_client(self, CLIENT_ID, CLIENT_SECRET, **kwargs):
		# - Create an API client on https://developer.domo.com
		# - Initialize the Domo SDK with your API client id/secret
		# - If you have multiple API clients you would like to use, simply initialize multiple Domo() instances
		# - Docs: https://developer.domo.com/docs/domo-apis/getting-started
		handler = logging.StreamHandler()
		handler.setLevel(logging.INFO)
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		handler.setFormatter(formatter)
		logging.getLogger().addHandler(handler)
		return Domo(CLIENT_ID, CLIENT_SECRET, logger_name='foo', log_level=logging.INFO, api_host=API_HOST, **kwargs)

	def datasets(self, domo):
		domo.logger.info("\n**** Domo API - DataSet Examples ****\n")
		datasets = domo.datasets
		
		# Define a DataSet Schema
		dsr = DataSetRequest()
		dsr.name = # PUT YOUR DATASET NAME LOGIC HERE
		dsr.description = # DATASET DESCRIPTION
		dsr.schema = Schema(jeff)

		# Create a DataSet with the given Schema
		dataset = datasets.create(dsr)
		domo.logger.info("Created DataSet " + dataset['id'])

		# Get a DataSets's metadata
		retrieved_dataset = datasets.get(dataset['id'])
		domo.logger.info("Retrieved DataSet " + retrieved_dataset['id'])

		# List DataSets
		dataset_list = list(datasets.list(sort=Sorting.NAME))
		domo.logger.info("Retrieved a list containing {} DataSet(s)".format(
                                                        len(dataset_list)))
		csv_file_path = allFiles
		datasets.data_import_from_file(dataset['id'], csv_file_path)
		domo.logger.info("Uploaded data from a file to DataSet {}".format(
			dataset['id']))


for eachCSV in os.listdir(inputdir):
	allFiles = str(os.path.abspath(eachCSV))
	table = Table(allFiles)
	table.infer()
	jeff = table.schema.descriptor
	table.read(keyed=True)
	directoryUpload()