#!/usr/bin/python

import sys

# This is our first basic mapper, just aggregate based on node id. This output will be
# used by top_sdb_mapper/reducer to save this data into SimpleDB
def main (argv):
	
	# read each line
	line = sys.stdin.readline()
	
	try:
		while line:
			# strip out trailing characters
			line = line.rstrip()
	
			# look at each word
			words = line.split()

			for word in words:
				# our ids might be separated by commas as well, so do 1 more level of parsing
				ids = word.split(",")
					
				for id in ids:
					# we only care about the ones that start with guid
					if id.startswith("/guid/"):
						# send out our sum
						print "LongValueSum:" + id + "\t" + "1"		

			# get our next line
			line = sys.stdin.readline()
	except "end of file":	
		return None

if __name__ == "__main__":
	main(sys.argv)
