from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperHero(MRJob): 
  
  def configure_options(self): 
    super(MostPopularSuperHero, self).configure_options()
    self.add_file_option('--namefile', help='path to superhero name file')
 
  def steps(self): 
    return [
               MRStep(mapper=self.mapper_parse_lines, 
                      reducer=self.reducer_count_friends), 
               MRStep(mapper=self.mapper_prepare_single_key, 
                      reducer_init=self.reducer_init,
                      reducer=self.reducer_find_most_popular)
           ]

  def mapper_parse_lines(self, _, line): 
      char_ids = line.split(" ")
      yield int(char_ids[0]), int(len(char_ids)-1)

  def reducer_count_friends(self, super_hero, friends): 
      yield super_hero, sum(friends)

  def mapper_prepare_single_key(self, key, value): 
      yield None, (value, key)

  def reducer_init(self): 
      self.superHeroNames = {}
      with open("Marvel-Names.txt", encoding="ISO-8859-1") as f: 
          for line in f: 
              fields = line.split(" ")
              self.superHeroNames[int(fields[0])] = fields[1]

  def reducer_find_most_popular(self, key, value): 
      answer = max(value)
      yield answer[0], self.superHeroNames[answer[1]]

if __name__ == '__main__': 
  MostPopularSuperHero.run()
