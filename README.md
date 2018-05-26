# Million Songs Data Set - User Listener Data

# Description

This is an analysis of the [taste profile subset](https://labrosa.ee.columbia.edu/millionsong/tasteprofile) from the [Million Songs Data Set](https://labrosa.ee.columbia.edu/millionsong/). 

MapReduce was used to analyze the similarity between users based on listening habits. The data contains:
* 1,019,318 unique users
* 384,546 unique songs
* 48,373,586 rows

where each row contains:
(userid, songid, play count) in a tab-delimited text file


# Repository files
+ scripts/
    * **probability_small_dataset.py** - uses probability to output a smaller data set for local testing
    * **songs_small_dataset.py** - filters for songs to make a smaller data set for local testing
+ **mapreduce_total_plays.py** - calculates the total plays of each song
+ **map_reduce_user_similarity.py** - calculates the user similarity using cosine similarity; however, this code does not work because there are too many user x user combinations to create vectors of a song. Will change this code in the future to calculate song similarity using cosine similarity

# Running the code

Install the Python mrjob library `pip install mrjob`.

To run this code from the terminal, you need to specify your AWS credentials:
export AWS_ACCESS_KEY_ID = 'your access key id'
export AWS_SECRET_ACCESS_KEY = 'your secret access key' 

See: https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html Access Keys

Use mrjob library to run the code from the terminal using AWS EMR: 
```
python mapreduce_total_plays.py -r emr 
'path to song data file' --num-core-instances=4 
--output-dir='s3-bucket-for-output'  --no-output 
--tracks='path to unique tracks.txt'
```

```
python map_reduce_user_similarity.py -r emr 'path to song data_file'
--instance-type=m5.xlarge --num-core-instances=16 
--output-dir='path to s3 output directory' --no-output
```