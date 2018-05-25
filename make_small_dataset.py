import random

# creates a smaller data set by randomly sampling about 5% of the data
# with open('train_triplets.txt', 'r') as f, open('small_train_triplets.txt', 'w') as new:
# 	for line in f:
# 		if random.random() < .0005:
# 			if len(line.split('\t')) == 3:
# 				new.write(line)

with open('train_triplets.txt', 'r') as f, open('small_train_triplets.txt', 'w') as new:
	for line in f:
		if line.split('\t')[1] == 'SOISNSU12AC468C0D8' or line.split('\t')[1] == 'SOCXSUW12A6D4F60A1':
			new.write(line)
