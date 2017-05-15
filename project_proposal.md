Scala Project Proposal

My project will be a web application that maps tweet activity by keyword. The user will enter a keyword and the output will be a world map with a heat map over the map that will indicate the tweet activity.

My project will use Kafka for retrieving the tweets, Spark streaming for aggregating the tweets, HBase for storing the tweets. My web page will then retrieve the data from HBase and will generate a map using GeoTrellis.

I intend to divide the work into 3 parts: a data extraction pipeline, a web UI and a GeoTrellis application to plot the data.

In order to complete this work I would like it if you taught a bit more about Spark Streaming and about HBase.