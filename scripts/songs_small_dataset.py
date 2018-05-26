# outputs dataset where number of plays of a song is greater than 2
with open('data/train_triplets.txt', 'r') as f, 
open('small_train_triplets.txt', 'w') as new:
	for line in f:
		if int(line.split('\t')[2]) > 1:
			new.write(line)
