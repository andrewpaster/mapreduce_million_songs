from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import defaultdict

import math
import itertools
import numpy as np


class MRUserSimilarity(MRJob):
	
	def configure_options(self):
	   super(MRUserSimilarity, self).configure_options()
	   self.add_file_option('--tracks', help='directory to unique_tracks.txt')

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
			yield songid, (userid, totalplays / total)

	def mapper_song_user_ratings(self, songid, userplays):
		yield songid, userplays
		# for songid, plays in songs:
		# 	yield songid, (userid, round(plays / total), 0)

	def reducer_combine_users(self, songid, userplays):
		combinations = list(itertools.combinations(userplays, 2))
		for user1, user2 in combinations:
			yield (user1[0], user2[0]), (user1[1], user2[1])
			yield (user2[0], user1[0]), (user2[1], user1[1])

	def mapper_user_ratings_list(self, users, ratings):
		yield users, ratings

	def cosine_similarity(self, vectors):
		xx = 0
		yy = 0
		xy = 0

		for vector in vectors:
			xx += float(vector[0]) * float(vector[0])
			yy += float(vector[1]) * float(vector[1])
			xy += float(vector[0]) * float(vector[1])

		return xy / float((math.sqrt(xx) * math.sqrt(yy)))

	def reducer_calculate_similarity(self, users, ratings):
		similarity = self.cosine_similarity(ratings)
		yield users, similarity

	def mapper_sort_results(self, users, similarity):
		yield (users[0], similarity), users[1]

	def reducer_output_results(self, user1_data, user2):
		for user in user2:
			yield user1_data[0], (user, user1_data[1])

if __name__ == '__main__':
	MRUserSimilarity.run()