from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularMovie(MRJob): 
    #  this is how you add an extra file so that it gets distributed accross all mapr nodes
    #  notice how we also define a reducer init function in the steps

    def configure_options(self): 
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self): 
        return [
                   MRStep(mapper=self.mapper_get_ratings,
                          reducer_init=self.reducer_init, 
                          reducer=self.reducer_count_arrange), 
                   MRStep(reducer=self.reducer_max_viewed)
               ]

    def mapper_get_ratings(self, _, line): 
        customer, movie, rating, timestamp = line.split("\t")
        yield movie, 1

    def reducer_init(self): 
        self.movieNames = {}
        with open("u.ITEM", encoding = "ISO-8859-1") as f: 
          for line in f: 
            fields = line.split("|")
            self.movieNames[fields[0]] = fields[1]


    def reducer_count_arrange(self, movie, occurence): 
        yield None, (sum(occurence), self.movieNames[movie])

    def reducer_max_viewed(self, key, value_tuple): 
        yield max(value_tuple)

if __name__ == '__main__': 
  MostPopularMovie.run()
