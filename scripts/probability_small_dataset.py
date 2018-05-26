import random

# creates a smaller data set by randomly sampling about 5% of the data
with open('data/train_triplets.txt', 'r') as f, 
open('small_train_triplets.txt', 'w') as new:
	for line in f:
		# write out the line if probability is less than a certain threshold
		if random.random() < .0005:
			new.write(line)

