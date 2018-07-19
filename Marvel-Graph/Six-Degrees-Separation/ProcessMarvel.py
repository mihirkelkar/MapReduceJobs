import sys

print('Creating the BFS Starting input for character ' + sys.argv[1])

with open("BFS-iteration-0.txt", 'w') as out: 
  with open("Marvel-graph.txt") as f: 
    for line in f: 
      fields = line.split()
      heroID = fields[0]
      numConnections = len(fields) - 1
      connections = fields[-numConnections:]

      color='WHITE'
      distance = 9999

      if heroID == sys.argv[1]: 
        color = 'GRAY'
        distance = 0
  
      if heroID != '': 
        edges = ','.join(connections)
        outstr = '|'.join((heroID, edges, str(distance), color))
        out.write(outstr)
        out.write("\n")
  f.close()
out.close()  
