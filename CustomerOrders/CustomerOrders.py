from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerOrders(MRJob):
    def steps(self): 
        return [
                 MRStep(mapper=self.mapper_parse_logs,reducer=self.reducer_count_sums), 
                 MRStep(mapper=self.mapper_consistent_ids, reducer=self.reducer_final)
               ]
    def mapper_parse_logs(self, key, line):
        (customerId, productId, orderAmount) = line.split(',')
        yield customerId, orderAmount

    def reducer_count_sums(self, customerId, orderAmounts):
        yield customerId, sum([float(order) for order in orderAmounts])

    def mapper_consistent_ids(self, customerId, totalOrderAmount): 
        yield "%06f"%int(totalOrderAmount), customerId

    def reducer_final(self, totalOrderAmount, customers): 
        for customerId in customers:  
          yield customerId, totalOrderAmount

if __name__ == '__main__':
    MRCustomerOrders.run()
