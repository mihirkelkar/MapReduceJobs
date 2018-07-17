from mrjob.job import MRJob

class MRMinTemperature(MRJob): 
  def MakeFarhenit(self, tenthOfCelcius): 
    celcius = float(tenthOfCelcius) / 10
    f = celcius * 1.8 + 32
    return f

  def mapper(self, _, line): 
    location, date, ty, data, x, y, z, w = line.split(",")
    if ty=="TMIN": 
      temperature = self.MakeFarhenit(data)
      yield location, temperature

  def reduce(self, location, temps):
    yield min(temps)

if __name__ == "__main__": 
  MRMinTemperature.run() 
