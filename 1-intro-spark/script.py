# to run this
# in command line => spark-submit script.py

from pyspark import SparkContext
sc =SparkContext()

raw_data = sc.textFile("daily_show.tsv")
raw_data.take(5)

daily_show = raw_data.map(lambda line: line.split('\t'))

print '********Start**********'
print daily_show.take(5)
print '********Finish*********'
# Hit check to see the output


tally = daily_show.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)
print '********Tally Start**********'
print(tally)
print '********Tally Finish**********'

print '********Tally Count**********'
print tally.take(tally.count())
print '********Tally Count finish**********'

def filter_year(line):
    if line[0] == 'YEAR':
        return False
    else:
        return True

filtered_daily_show = daily_show.filter(lambda line: filter_year(line))


# to run the spark => using functional programming approach
filtered_daily_show.filter(lambda line: line[1] != '') \
                   .map(lambda line: (line[1].lower(), 1)) \
                   .reduceByKey(lambda x,y: x+y) \
                   .take(5)