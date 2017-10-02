import os, boto, boto3, csv, re
import numpy as np
import pandas as pd
from collections import OrderedDict
import time

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# conn = boto.connect_s3(access_key, secret_access_key)
# Read from S3...
# df = pd.read_csv('https://s3.amazonaws.com/christine-test-bucket/cancer.csv')
# Upload file to S3...
# bucket = conn.get_bucket(bucket_name)
# key = bucket.new_key(filename)
# key.set_contents_from_filename(filename)
# print('Files in bucket:', [f.name for f in bucket.list()])

# client = boto3.client('s3') #low-level functional API
# resource = boto3.resource('s3') #high-level object-oriented API
# obj = client.get_object(Bucket='gschoolcapstone', Key='npidata_20050523-20170813FileHeader.csv')
# obj = client.get_object(Bucket='gschoolcapstone', Key='npidata_20050523-20170813.csv')
# f = obj['Body'].read().decode()
# for i,row in enumerate(f.split('\n')):
# 	if i < 10: print(row.split(','))



class NPIparser(object):

	def __init__ (self):
		print('Loading codes...')
		# self.code_dict = self._create_codes()
		# self.codes = np.array([code for code in self.code_dict.keys()])
		self.df = pd.read_csv('data/nucc_taxonomy_171.csv')
		self.codes = np.array(self.df.Code)
		# col_names = df.columns.values
		self.classifications = np.sort(np.array(self.df.Classification.unique()))
		self.states = np.array(['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', \
								'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', \
								'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', \
								'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', \
								'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'])
		self.creds = np.array(['APRN', 'ATC', 'AUD', 'BS', 'CCC', 'DC', 'DDS', 'DO', 'NP', \
								'JD', 'MA', 'MBA', 'MD', 'MS', 'NP', 'OD', 'ORT', 'PA', 'PHAR', \
								'PHD', 'PSY', 'PT', 'RD', 'RN', 'RPH', 'PHARM'])

	# def _create_codes(self):
		# code_file = 'data/nucc_taxonomy_171.csv'
		# code_f = open(code_file, 'r')
		# code_reader = csv.reader(code_f)
		# code_header = next(code_reader) # ['Code', 'Grouping', 'Classification', 'Specialization', 'Definition', 'Notes']
		# code_dict = {line[0]: line[1:] for line in code_reader}
		# code_f.close()
		# return code_dict


	def parse_to_numeric(self, filename):
		file_in = open(filename, 'r')
		reader = csv.reader(file_in)
		new_filename = filename.split('.')[0] + '_clean.' + filename.split('.')[1]
		file_out = open(new_filename, 'w')
		writer = csv.writer(file_out)
		header = next(reader)
		self._create_col_dict(header)
		self._write_header(writer, header) # Write header with new columns
		self._line_by_line(reader, writer)
		file_in.close()
		file_out.close()
		print('File has been parsed.')
		return None

	def _create_col_dict(self, header):
		self.col_dict = {col: i for i,col in enumerate(header)}
		self.col_dict = OrderedDict(sorted(npi.col_dict.items(), key=lambda t: t[1]))
		# print(col_dict)
		return None

	def _write_header(self, writer, header):
		col_names = np.array(header)[0]
		col_names = np.append(col_names, ['Entity_1', 'Entity_2'])
		col_names = np.append(col_names, ['Gender_M', 'Gender_F'])
		col_names = np.append(col_names, ['Sole_Proprietor_Y', 'Sole_Proprietor_N'])
		col_names = np.append(col_names, ['Organization Subpart_Y', 'Organization Subpart_N'])
		state_names = np.array(['st_' + state for state in self.states])
		col_names = np.append(col_names, state_names)	# Add column for each state
		# col_names = np.append(col_names, self.classifications)
		col_names = np.append(col_names, self.codes)	# Add column for each Taxonomy Code
		col_names = np.append(col_names, self.creds)
		# print(col_names)
		writer.writerow(col_names.tolist())
		return None

	def _line_by_line(self, reader, writer):
		print('Starting to parse...')
		for i,line in enumerate(reader):
			if np.array(line)[1] != '': # only include NPI if Entity Type is not empty
				line = np.array(line)
				# print(len(line))
				# print(line)

				entity = self._find_entity(line)
				line = np.append(line, entity)
				# print(line[self.col_dict['Entity Type Code']], entity)

				gender = self._find_gender(line)
				line = np.append(line, gender)
				# print(line[self.col_dict['Provider Gender Code']], gender)

				sole = self._find_sole(line)
				line = np.append(line, sole)
				# print(line[self.col_dict['Is Sole Proprietor']], sole)

				subpart = self._find_subpart(line)
				line = np.append(line, subpart)
				# print(line[self.col_dict['Is Organization Subpart']], subpart)

				states = self._find_states(line, self.states)
				line = np.append(line, states)
				# print(line[self.col_dict['Provider Business Mailing Address State Name']], states)

				# primary = self._find_primary_classification(line)
				# line = np.append(line, primary)

				specialties = self._find_specialties(line, self.codes)
				line = np.append(line, specialties)
				# keys = ['Healthcare Provider Taxonomy Code_1',
				# 		'Healthcare Provider Taxonomy Code_2',
				# 		'Healthcare Provider Taxonomy Code_3',
				# 		'Healthcare Provider Taxonomy Code_4',
				# 		'Healthcare Provider Taxonomy Code_5',
				# 		'Healthcare Provider Taxonomy Code_6',
				# 		'Healthcare Provider Taxonomy Code_7',
				# 		'Healthcare Provider Taxonomy Code_8',
				# 		'Healthcare Provider Taxonomy Code_9',
				# 		'Healthcare Provider Taxonomy Code_10',
				# 		'Healthcare Provider Taxonomy Code_11',
				# 		'Healthcare Provider Taxonomy Code_12',
				# 		'Healthcare Provider Taxonomy Code_13',
				# 		'Healthcare Provider Taxonomy Code_14',
				# 		'Healthcare Provider Taxonomy Code_15']
				# idx = [self.col_dict[key] for key in keys]
				# print(line[idx], specialties)
				# print(self.codes[(self.codes=='207X00000X').astype(int).argmax()] == '207X00000X')

				credentials = self._find_credentials(line, self.creds)
				line = np.append(line, credentials)
				# print(line[self.col_dict['Provider Credential Text']], credentials)

				line = np.delete(line, np.arange(1,329)) # Delete original non-numeric values
				# print(line)
				writer.writerow(line)
			if i%100000==0 and i!=0:
				print(i)
				print('Time since started:', time.time()-start, 'seconds')
			# if i==0: break
		return None

	def _find_entity(self, line):
		idx = self.col_dict['Entity Type Code']
		if line[idx] == '1': return [1,0]
		elif line[idx] == '2': return [0,1]
		else: return [0,0]

	def _find_gender(self, line):
		idx = self.col_dict['Provider Gender Code']
		if line[idx] == 'M': return [1,0]
		elif line[idx] == 'F': return [0,1]
		else: return [0,0]

	def _find_sole(self, line):
		idx = self.col_dict['Is Sole Proprietor']
		if line[idx] == 'Y': return [1,0]
		elif line[idx] == 'N': return [0,1]
		else: return [0,0]

	def _find_subpart(self, line):
		idx = self.col_dict['Is Organization Subpart']
		if line[idx] == 'Y': return [1,0]
		elif line[idx] == 'N': return [0,1]
		else: return [0,0]

	def _find_states(self, line, states):
		idx = self.col_dict['Provider Business Mailing Address State Name']
		state = line[idx]
		return np.in1d(states, state).astype(int)

	def _find_primary_classification(self, line):
		switches = ['Healthcare Provider Primary Taxonomy Switch_1',
				'Healthcare Provider Primary Taxonomy Switch_2',
				'Healthcare Provider Primary Taxonomy Switch_3',
				'Healthcare Provider Primary Taxonomy Switch_4',
				'Healthcare Provider Primary Taxonomy Switch_5',
				'Healthcare Provider Primary Taxonomy Switch_6',
				'Healthcare Provider Primary Taxonomy Switch_7',
				'Healthcare Provider Primary Taxonomy Switch_8',
				'Healthcare Provider Primary Taxonomy Switch_9',
				'Healthcare Provider Primary Taxonomy Switch_10',
				'Healthcare Provider Primary Taxonomy Switch_11',
				'Healthcare Provider Primary Taxonomy Switch_12',
				'Healthcare Provider Primary Taxonomy Switch_13',
				'Healthcare Provider Primary Taxonomy Switch_14',
				'Healthcare Provider Primary Taxonomy Switch_15']
		switch_idx = [self.col_dict[switch] for switch in switches]
		idx = (np.array(line[switch_idx]) == 'Y').argmax()
		cols = ['Healthcare Provider Taxonomy Code_1',
				'Healthcare Provider Taxonomy Code_2',
				'Healthcare Provider Taxonomy Code_3',
				'Healthcare Provider Taxonomy Code_4',
				'Healthcare Provider Taxonomy Code_5',
				'Healthcare Provider Taxonomy Code_6',
				'Healthcare Provider Taxonomy Code_7',
				'Healthcare Provider Taxonomy Code_8',
				'Healthcare Provider Taxonomy Code_9',
				'Healthcare Provider Taxonomy Code_10',
				'Healthcare Provider Taxonomy Code_11',
				'Healthcare Provider Taxonomy Code_12',
				'Healthcare Provider Taxonomy Code_13',
				'Healthcare Provider Taxonomy Code_14',
				'Healthcare Provider Taxonomy Code_15']
		col = self.col_dict[cols[idx]]
		primary = line[col] # Returns primary code
		# classification = self.df.Classification[self.df.Code==primary].values[0]
		classification = self.df.Classification[self.df.Code==primary].values
		# AttributeError: 'bool' object has no attribute 'astype'
		return (self.classifications == classification).astype(int)

	def _find_specialties(self, line, codes):
		keys = ['Healthcare Provider Taxonomy Code_1',
				'Healthcare Provider Taxonomy Code_2',
				'Healthcare Provider Taxonomy Code_3',
				'Healthcare Provider Taxonomy Code_4',
				'Healthcare Provider Taxonomy Code_5',
				'Healthcare Provider Taxonomy Code_6',
				'Healthcare Provider Taxonomy Code_7',
				'Healthcare Provider Taxonomy Code_8',
				'Healthcare Provider Taxonomy Code_9',
				'Healthcare Provider Taxonomy Code_10',
				'Healthcare Provider Taxonomy Code_11',
				'Healthcare Provider Taxonomy Code_12',
				'Healthcare Provider Taxonomy Code_13',
				'Healthcare Provider Taxonomy Code_14',
				'Healthcare Provider Taxonomy Code_15']
		idx = [self.col_dict[key] for key in keys]
		return np.in1d(codes, line[idx]).astype(int)

	def _find_credentials(self, line, creds):
		idx = self.col_dict['Provider Credential Text']
		x = line[idx]
		x = x.upper()
		x = re.sub("PHARMD", "RPH ", x)
		x = re.sub("\.|\>|\`","",x)
		x = re.sub("\,|\;|\-|\(|\)|\/"," ",x)
		x = re.sub("\s+", " ", x)
		x = re.sub("PHYSICIAN ASSISTANT", "PA", x)
		x = re.sub("NURSE PRACTITIONER", "NP", x)
		x = re.sub("PHYSICAL THERAPIST", "PT", x)
		x = re.sub("(BS IN PHARMACY|BS PHARMACY|DOCTOR OF PHARMACY|PHARMACIST|PHARMD)", " RPH ", x)
		x = re.sub("M D", "MD", x)
		x = re.sub("D C", "DC", x)
		x = re.sub("P C", "PC", x)
		x = re.sub("D P M", "DPM", x)
		x = re.sub("D O", "DO", x)
		x = re.sub("O D", "OD", x)
		x = re.sub("0D", "OD", x)
		x = re.sub("[\d]", "", x) # remove numbers
		x = x.strip()
		# print (np.in1d(creds, x).astype(int))
		return np.array([int(cred in x) for cred in creds])


if __name__ == '__main__':
	start = time.time()
	npi = NPIparser()
	# filename = 'https://s3n.amazonaws.com/gschoolcapstone/npidata_20050523-20170813.csv'
	filename = 'data/npidata_20050523-20170813.csv'
	npi.parse_to_numeric(filename)
	print('Time to parse:', time.time()-start, 'seconds')

	# new_file = filename.split('.')[0] + '_clean.' + filename.split('.')[1]
	# df = pd.read_csv(new_file)

	# Drop columns where only 1 unique value
	# nunique = df.apply(pd.Series.nunique)
	# cols_to_drop = nunique[nunique == 1].index
	# df.drop(cols_to_drop, axis=1, inplace=True)
