from mrjob.job import MRJob
from mrjob.step import MRStep

from math import sqrt
from itertools import combinations


#  to run on an emr node, all you need to do is this:  
#  set your access and secret key as env variables
#  then run python3 MovieSimilarities.py -r emr --items=ml-100k/u.item ml-100k/u.data

class MovieSimilarity(MRJob): 
  
  def configure_options(self): 
    super(MovieSimilarity, self).configure_options()
    self.add_file_option('--items', help='The file that contains names for items')

  def load_movie_names(self): 
    self.movieName = {}
    
    with open('u.item', encoding='ascii', errors='ignore') as f: 
      for line in f: 
        fields = line.split('|')
        self.movieName[int(fields[0])] = fields[1]

  def steps(self): 
    return [
	MRStep(mapper=self.mapper_parse_users, reducer=self.reducer_ratings_by_user), 
        MRStep(mapper=self.mapper_create_item_pairs, reducer=self.reducer_compute_similarity), 
        MRStep(mapper=self.mapper_sort_similarities, mapper_init=self.load_movie_names, 
               reducer=self.reducer_output_similarities)
          ]

  #  Step 1
  def mapper_parse_users(self, key, line): 
    #  This function outouts userID -> (movieId, rating)
    (userID, movieID, rating, timestamp) = line.split('\t')
    yield userID, (movieID, float(rating))

  def reducer_ratings_by_user(self, userID, itemRatings): 
    # group (movieID, rating) pairs by userID
    ratings = list()
    for movieID, rating in itemRatings: 
      ratings.append((movieID, rating))  
    yield userID, ratings


  #  Step 2
  def mapper_create_item_pairs(self, userID, itemRatings): 
    #  Find every pair of movies each user has seen and emit each pair with 
    #  its associated ratings.
    for itemRating1, itemRating2, in combinations(itemRatings, 2): 
      movID_one  = itemRating1[0]
      rating_one = itemRating1[1] 

      movID_two  = itemRating2[0]
      rating_two = itemRating2[1]
  
      if int(movID_one) < int(movID_two): 
        yield (movID_one, movID_two), (rating_one, rating_two)    
      else: 
        yield (movID_two, movID_one), (rating_two, rating_one)

  #  measuring similarity
  def cosine_similarity(self, ratingPairs): 
    #  computes the cosine similarity metric between two rating vectors
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs: 
      sum_xx += ratingX * ratingX 
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if denominator: 
      score = numerator / float(denominator)
    return (score, numPairs)  

  def reducer_compute_similarity(self, moviePair, ratingsPairs): 
    # ratingsPairs in this reducer is a list of tuples rated for that movie combination
    # from multiple users
    score, numPairs = self.cosine_similarity(ratingsPairs)

    #  enforce some minimum quality
    if numPairs > 10  and score > 0.95: 
      yield moviePair, (score, numPairs)

    
  def mapper_sort_similarities(self, moviePair, score_tuple): 
    #  This mapper basically takes its inputs and converts it into 
    #  movie1, score -> movie2, number of times viewed together
    score, num = score_tuple
    movie1, movie2 = moviePair
    yield (self.movieName[int(movie1)], score), (self.movieName[int(movie2)], num)
  
  def reducer_output_similarities(self, movieScore, similarN): 
    #  essentially yield movie -> similar movie, score, number of times viewed together
    movie1, score = movieScore
    for movie, corating in similarN: 
      yield movie1, (movie, score, corating)
    
    
if __name__ == '__main__' :
  MovieSimilarity.run()
