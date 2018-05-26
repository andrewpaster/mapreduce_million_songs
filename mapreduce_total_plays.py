from mrjob.job import MRJob
from mrjob.step import MRStep

import random


class MRTotalPlaysPerSong(MRJob):
	
	def configure_args(self):
	   super(MRTotalPlaysPerSong, self).configure_args()
	   self.add_file_arg('--tracks', help='directory to unique_tracks.txt')

	def load_song_dictionary(self):
				
		self.song_dictionary = {}

		f = open('unique_tracks.txt', encoding='utf-8')
		for line in f:
			split_line = line.split("<SEP>")
			self.song_dictionary[split_line[1]] = [split_line[2], split_line[3].rstrip()]

	def steps(self):
		return [MRStep(mapper=self.mapper_get_plays, 
			reducer=self.reducer_sum_plays),
		MRStep(mapper_init=self.load_song_dictionary,
			mapper=self.mapper_sort_results,
			reducer=self.reducer_output_results)]

	def mapper_get_plays(self, _, line):
		userid, songid, plays = line.split('\t')
		yield songid, int(plays)

	def reducer_sum_plays(self, songid, plays):
		yield songid, sum(plays)

	def mapper_sort_results(self, songid, plays):
		yield int(plays), (self.song_dictionary[songid], songid)

	def reducer_output_results(self, plays, songs):
		for song in songs:
			yield song, plays

if __name__ == '__main__':
	MRTotalPlaysPerSong.run()