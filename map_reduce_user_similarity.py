# python map_reduce_user_similarity.py -r emr data/small_train_triplets.txt --instance-type=c1.medium --num-core-instances=2 > results.txt

from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

import math
import itertools

class MRUserSimilarity(MRJob):
	
	def steps(self):
		return [MRStep(mapper=self.mapper_user_plays,
			reducer=self.reducer_normalize_plays),
		MRStep(mapper=self.mapper_song_user_ratings,
			reducer=self.reducer_combine_users),
		MRStep(mapper=self.mapper_user_ratings_list,
			reducer=self.reducer_calculate_similarity),
		MRStep(mapper=self.mapper_sort_results,
			reducer=self.reducer_output_results)]

	def mapper_user_plays(self, _, line):
		userid, songid, plays = line.split('\t')
		yield userid, (songid, plays)

	def reducer_normalize_plays(self, userid, songs):
		songCounts = defaultdict(int)
		total = 0

		for songid, plays in songs:
			total += int(plays)
			songCounts[songid] += int(plays)	

		for songid, totalplays in songCounts.items():
			if math.floor(float(totalplays / total)) < 1:
				yield songid, (userid, float(totalplays / total))

	def mapper_song_user_ratings(self, songid, userplays):
		yield songid, userplays

	def reducer_combine_users(self, songid, userplays):
		combinations = list(itertools.combinations(userplays, 2))
		if combinations:
			for user1, user2 in combinations:
				yield (user1[0], user2[0]), (float(user1[1]), float(user2[1]))
				yield (user2[0], user1[0]), (float(user2[1]), float(user1[1]))

	def mapper_user_ratings_list(self, users, ratings):
		yield users, ratings

	def cosine_similarity(self, vectors):
		xx = 0
		yy = 0
		xy = 0
		counter = 0

		for vector in vectors:
			counter += 1
			xx += float(vector[0]) * float(vector[0])
			yy += float(vector[1]) * float(vector[1])
			xy += float(vector[0]) * float(vector[1])
		
		numerator = float(xy)
		denominator = float((math.sqrt(xx) * math.sqrt(yy)))
		score = 0
		if denominator:
			score = numerator / denominator
		return (score, counter)

	def reducer_calculate_similarity(self, users, ratings):
		similarity, counter = self.cosine_similarity(ratings)
		if counter >= 2:
			yield users, similarity

	def mapper_sort_results(self, users, similarity):
		yield (users[0], similarity), users[1]

	def reducer_output_results(self, user1_data, user2):
		for user in user2:
			yield user1_data[0], (user, user1_data[1])

if __name__ == '__main__':
	MRUserSimilarity.run()