import sys
import re

if len(sys.argv) != 2:
	exit("Must provide filename")

filename = sys.argv[1]
file = open(filename)

globalCompletions = []
globalVisits = []

line = file.readline()

while True:
	#print("Parsing line ", line)
	if line == '':
		break

	match1 = re.search("EXEC", line)

	if match1 != None:
		line = file.readline()
		continue

	completions = []
	visits = []

	for measurement in range(5):
		match2 = re.search("mcts_main", line)

		if match2 == None:
			print(line)
			exit("Unexpected mismatch")

		line = file.readline()
		match3 = re.search("AVG Global completions/msec: (.*)", line)
		
		if match3 == None:
			print(line)
			exit("Unexpected mismatch (completions)")

		completions.append(match3.group(1).strip())

		line = file.readline()
		match4 = re.search("AVG Global visits/msec: (.*)", line)

		if match4 == None:
			print(line)
			exit("Unexpected mismatch (visits)")

		visits.append(match4.group(1).strip())

		line = file.readline()

	globalCompletions.append( ", ".join(completions) )
	globalVisits.append( ", ".join(visits) )

file.close()

for completion in globalCompletions:
	print(completion)

for visits in globalVisits:
	print(visits)
