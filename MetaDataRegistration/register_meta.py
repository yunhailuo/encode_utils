#!/bin/env python3

###
#Nathaniel Watson
#2017-10-23
#nathankw@stanford.edu
###

import os
import argparse
import pdb

import encode_utils.connection


RECORD_ID_FIELD = "record_id" 
#RECORD_ID_FIELD is a special field that won't be skipped in the create_payload() function.
# It is used when patching objects to indicate the identifier of the record to patch. 
ENCODE_URL = "https://www.encodeproject.org"
PROFILES_URL = os.path.join(ENCODE_URL,"profiles")

def typecast_value(value,value_type):
	"""
	"""
	if value_type == "integer":
		return int(value)
	return value

def get_profile_schema(profile):
	"""
	Function : Retrieves the JSON schema of the specified profile from the ENCODE Portal.
	Raises   : requests.exceptions.HTTPError if the status code is something other than 200 or 404. 
	Returns  : 404 (int) if profile not found, otherwise a dict representing the profile's JSON schema. 
	"""
	url = os.path.join(PROFILES_URL,profile + ".json?format=json")
	res = requests.get(url,headers={"content-type": "application/json"})
	status_code = res.status_code
	if status_code == 404:
		return 404
	res.raise_for_status()
	return res.json()

def create_payloads(profile,infile):
	"""
	Function : Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal 
						 (such as https://www.encodeproject.org/profiles/biosample.json), generates the payload that can be used to either 
						 register or patch the metadata for each row. This is a generator function. Note that all profiles are given at 
						 https://www.encodeproject.org/profiles/, and that the lower-case name of the profile should be used here. 
	Args     : profile - The profile to submit to (lower-case). See details above.  
						 infile  - The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal
											 to the corresponding names in the profile given on the ENCODE Portal. For fields containing an array as the value,
											 values within the array must be comma-delimited and should not themselves be wrapped in brackets. 
	
											 Non-scematic fields are allowed as long as they begin with a '#'. Such fields will be skipped. 

											 When patching objects, you must specify the 'record_id' field to indicate the identifier of the record. 
											 Note that this a special field that is not present in the ENCODE schema, and doesn't use the '#' prefix to mark it 
											 as non-schematic. Here you can specify any valid record identifier (i.e. UUID, accession, alias). If this 
											 special field is present, it will not be skipped. 

											 Any lines after the header line that start with a '#' will be skipped, as well as any empty lines. 

											 Some profiles (most) require specification of the 'award' and 'lab' attributes. These may be set as fields in the
											 input file, or can be left out, in which case the default values for these attributes will be extracted from the
											 configuration file conf_data.json.

	Yields  : dict. The payload that can be used to either register or patch the metadata for each row.
	"""
	#Fetch the schema from the ENCODE Portal so we can set attr values to the right type when generating the  payload (dict). 
	schema = get_profile_schema(profile)
	schema_props = schema["properties"]
	START_COUNT = -1
	ID_FIELD_NAME = "@id"
	field_index = {}
	fh = open(infile,'r')
	header_fields = fh.readline().strip("\n").split("\t")
	count = START_COUNT
	skip_field_indices = []
	for field in header_fields:
		count += 1
		if field.startswith("#"): #non-schema field
			skip_field_indices.append(count)
			continue
		field_index[count] = field

	for line in fh:
		line = line.strip("\n").split("\t")
		if not line or line[0].startswith("#"):
			continue
		payload = {}
		payload[ID_FIELD_NAME] = "{}/".format(profile)
		if encode_utils.DCC_AWARD_ATTR not in header_fields:
			if profile not in encode_utils.AWARDLESS_PROFILES:
				payload[encode_utils.DCC_AWARD_ATTR] = encode_utils.AWARD
		if encode_utils.DCC_LAB_ATTR not in header_fields:
			if profile not in encode_utils.AWARDLESS_PROFILES:
				payload[encode_utils.DCC_LAB_ATTR] = encode_utils.LAB
		count = START_COUNT
		for val in line:
			count += 1
			val = val.strip()
			if (count in skip_field_indices) or (not val):
				continue
			field = field_index[count]
			val_type = schema_props[field]["type"]
			if val_type == "array":
				item_val_type = schema_props[field]["items"]["type"]
				val = [x.strip() for x in val.split(",")]
				val = [typecast_value(value=x,value_type=item_val_type) for x in val] #could be interger value
			else:
				val = typecast_value(value=val,value_type=val_type)
			payload[field] = val
		yield payload
	

if __name__ == "__main__":


	description = "Given a tab-delimited input file containing records belonging to one of the profiles listed on the ENCODE Portal (such as https://www.encodeproject.org/profiles/biosample.json), either registers or patches metadata for each record. Don't mix input files containing both new records and records to patch - in this case they should be split into separate files."
	parser = argparse.ArgumentParser(description=description,formatter_class=argparse.RawTextHelpFormatter)
	parser.add_argument("-p","--profile",required=True,help="The profile to submit to, i.e. put 'biosample' for https://www.encodeproject.org/profiles/biosample.json. The profile will be pulled down for type-checking in order to type-cast any values in the input file to the proper type (i.e. some values need to be submitted as integers, not strings).")
	parser.add_argument("-m","--dcc-mode",required=True,help="The DCC environment to submit to (either 'dev' or 'prod').")
	parser.add_argument("-i","--infile",help="""The tab-delimited input file with a field-header line as the first line. The field names must be exactly equal
to the corresponding names in the profile given on the ENCODE Portal. For fields containing an array as the value,
values within the array must be comma-delimited and should not themselves be wrapped in brackets. 

Non-scematic fields are allowed as long as they begin with a '#'. Such fields will be skipped. 

When patching objects, you must specify the 'record_id' field to indicate the identifier of the record. 
Note that this a special field that is not present in the ENCODE schema, and doesn't use the '#' prefix to mark it 
as non-schematic. Here you can specify any valid record identifier (i.e. UUID, accession, alias). If this 
special field is present, it will not be skipped. 

Any lines after the header line that start with a '#' will be skipped, as well as any empty lines. 

Some profiles (most) require specification of the 'award' and 'lab' attributes. These may be set as fields in the
input file, or can be left out, in which case the default values for these attributes will be extracted from the
configuration file conf_data.json.""")


	parser.add_argument("--patch",action="store_true",help="Presence of this option indicates to patch an existing DCC record rather than register a new one.")
	parser.add_argument("-e","--error-if-not-found", action="store_true",help="If trying to PATCH a record and the record cannot be found on the ENCODE Portal, the default behavior is to then attempt a POST. Specifying this option causes an Exception to be raised.")
	parser.add_argument("--overwrite-array-values",action="store_true",help="Only has meaning in combination with the --patch option. When this is specified, it means that any keys with array values will be overwritten on the ENCODE Portal with the corresponding value to patch. The default action is to extend the array value with the patch value and then to remove any duplicates.")
	args = parser.parse_args()
	profile = args.profile
	dcc_mode = args.dcc_mode
	error_if_not_found = args.error_if_not_found
	overwrite_array_values = args.overwrite_array_values

	conn = encode_utils.connection.Connection(dcc_username="nathankw",dcc_mode=dcc_mode)

	infile = args.infile
	patch = args.patch
	gen = create_payloads(profile=profile,infile=infile)
	for payload in gen:
		#pdb.set_trace()
		if not patch:
			conn.post(payload=payload) #this method will pop out the '@id' field if it is present.
		else:
			record_id = payload.get(RECORD_ID_FIELD,False)
			if not record_id:
				raise Exception("Can't patch payload {} since there isn't a record_id field indiciating the ID of the record to patch.".format(payload))
			payload.pop(RECORD_ID_FIELD)
			conn.patch(record_id=record_id,payload=payload,error_if_not_found=error_if_not_found,extend_array_values=not overwrite_array_values)
