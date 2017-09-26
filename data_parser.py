import os, boto3, csv, re
import numpy as np
import pandas as pd
from collections import OrderedDict
import time


# ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
# SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
# client = boto3.client('s3') #low-level functional API
# resource = boto3.resource('s3') #high-level object-oriented API
# obj = client.get_object(Bucket='gschoolcapstone', Key='npidata_20050523-20170813FileHeader.csv')
# f = obj['Body'].read().decode()
# for row in f.split('\n'):
# 	row.split(',')


class NPIparser(object):

	def __init__ (self):
		self.code_dict = self._create_code_dict()
		self.codes = np.array([code for code in self.code_dict.keys()])
		self.states = np.array(['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', \
								'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', \
								'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', \
								'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', \
								'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'])
		self.creds = np.array(['APRN', 'ATC', 'AUD', 'BS', 'CCC', 'DC', 'DDS', 'DO', 'NP', \
								'JD', 'MA', 'MBA', 'MD', 'MS', 'NP', 'OD', 'ORT', 'PA', 'PHAR', \
								'PHD', 'PSY', 'PT', 'RD', 'RN', 'RPH', 'PHARM'])

	def _create_code_dict(self):
		code_file = 'data/nucc_taxonomy_171.csv'
		code_f = open(code_file, 'r')
		code_reader = csv.reader(code_f)
		code_header = next(code_reader) # ['Code', 'Grouping', 'Classification', 'Specialization', 'Definition', 'Notes']
		code_dict = {line[0]: line[1:] for line in code_reader}
		code_f.close()
		return code_dict

	def parse_to_numeric(self, filename):
		file_in = open(filename, 'r')
		reader = csv.reader(file_in)
		file_out = open('data/clean_data.csv', 'w')
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
		col_names = np.array(header)[:2]
		col_names = np.append(col_names, 'Gender')
		col_names = np.append(col_names, 'Is Sole Proprietor')
		col_names = np.append(col_names, 'Is Organization Subpart')
		state_names = np.array(['st_' + state for state in self.states])
		col_names = np.append(col_names, state_names)	# Add column for each state
		col_names = np.append(col_names, self.codes)	# Add column for each Taxonomy Code
		# col_names = np.append(col_names, 'Primary Taxonomy Code')
		col_names = np.append(col_names, self.creds)
		# print(col_names)
		writer.writerow(col_names.tolist())
		return None

	def _line_by_line(self, reader, writer):
		for i,line in enumerate(reader):
			if np.array(line)[1] != '': # only include NPI if Entity Type is not empty
				line = np.array(line)
				# print(len(line))
				# print(line)

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

				line = np.delete(line, np.arange(2,329)) # Delete original non-numeric values
				# print(line)
				writer.writerow(line)
			if i%10000==0 and i!=0: print(i)
			# if i==1000: break
		return None

	def _find_gender(self, line):
		idx = self.col_dict['Provider Gender Code']
		if line[idx] == 'M': return 1
		elif line[idx] == 'F': return -1
		else: return 0

	def _find_sole(self, line):
		idx = self.col_dict['Is Sole Proprietor']
		if line[idx] == 'Y': return 1
		elif line[idx] == 'N': return -1
		else: return 0

	def _find_subpart(self, line):
		idx = self.col_dict['Is Organization Subpart']
		if line[idx] == 'Y': return 1
		elif line[idx] == 'N': return -1
		else: return 0

	def _find_states(self, line, states):
		idx = self.col_dict['Provider Business Mailing Address State Name']
		state = line[idx]
		return np.in1d(states, state).astype(int)

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

	# def _find_primary(self, line):
	# 	cols = np.arange(8,38,2)
	# 	idx = cols[(line[cols] == 'Y').argmax()] - 1
	# 	return line[idx] # Returns code that is primary

	def _find_credentials(self, line, creds):
		idx = self.col_dict['Provider Credential Text']
		x = line[idx]
		# print(x)
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
		# print(x)
		# print([int(cred in x) for cred in creds])
		return np.array([int(cred in x) for cred in creds])
		# print (np.in1d(creds, x).astype(int))

if __name__ == '__main__':

	start = time.time()
	npi = NPIparser()
	npi_file = 'data/npidata_20050523-20170813.csv'
	npi.parse_to_numeric(npi_file)
	print('Time to parse:', time.time()-start, 'seconds')

	df = pd.read_csv('data/clean_data.csv')
