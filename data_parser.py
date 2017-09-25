import os, boto3, csv

ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

# conn = boto.connect_s3(ACCESS_KEY, SECRET_KEY)
# b = conn.get_bucket('gschoolcapstone')
# file_object = b.new_key('npidata_20050523-20170813.csv')
# filename = file_object.get_contents_as_string()

client = boto3.client('s3') #low-level functional API
# resource = boto3.resource('s3') #high-level object-oriented API




if __name__ == '__main__':

	obj = client.get_object(Bucket='gschoolcapstone', Key='npidata_20050523-20170813FileHeader.csv')
	f = obj['Body'].read().decode()
	for row in f.split('\n'):
		row.split(',')


	# filepath_in = 's3n://gschoolcapstone/npidata_20050523-20170813.csv'

	# fid1 = open(filepath_in, 'r')
	# reader = csv.reader(fid1)

	# fid2 = open(filepath_out, 'w')
	# writer = csv.writer(fid2)

	# header = fid1.next
	# print(header)

	# for line in reader:
	# 	reader.read
	# 	writer.write(row)


	# fid1.close()
	# fid2.close()
