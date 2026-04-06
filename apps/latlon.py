from pyspark import SparkContext
import pandas as pd
from operator import add

# setup the function to add two tuples element-wise
def tuple_add(a, b):
    return (a[0]+b[0], a[1]+b[1])

def main():
    # initialize the spark context
    sc = SparkContext(master="spark://spark-master:7077")

    # download the data from https://s3.amazonaws.com/nycbuspositions/2017/07/2017-07-14-bus-positions.csv.xz
    df = pd.read_csv('/opt/spark-data/2017-07-14-bus-positions.csv')

    # get the route_id, latitude and longitude values from the dataframe and convert it to a list of tuples
    data = df[['route_id', 'latitude', 'longitude']].values
    
    # convert the dataframe to an rdd
    rdd = sc.parallelize(data, 20)

    # cache the rdd to speed up the computations
    rdd.cache()

    # get the latitudes & longitudes
    # sum up the latitudes and longitudes for each route_id
    lat_lon = rdd.map(lambda x: (x[0], (x[1], x[2]))).reduceByKey(tuple_add)

    # count the number of latitudes and longitudes for each route_id
    lengths = rdd.groupBy(lambda x: x[0]).mapValues(lambda x: len(x))

    # join the rdds
    # the result of the join will be (route_id, ((sum_latitude, sum_longitude), count))
    merged = lat_lon.join(lengths)

    # compute the average lat lon values.
    # the result will be (route_id, (avg_latitude, avg_longitude))
    final = merged.map(lambda x: (x[0], (x[1][0][0]/x[1][1], x[1][0][1]/x[1][1])))

    # collect the results and write to a file
    results = final.collect()

    # write the results to a file
    with open("/opt/spark-data/results", "w") as file:
       for result in results:
          # write the route_id and the average lat lon values (centroids) to the file
          file.write(result[0] + " -> " + str(result[1]) + "\n")
       file.close()

if __name__ == '__main__':
  main()


