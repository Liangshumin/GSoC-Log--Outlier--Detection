# GSoC-Log--Outlier--Detection
This project is based on Drain3 for log outlier detection, we use the Ray framework and Redis database to complete the log detection process,
the specific implementation process in *Drain3/ray_log/transformer.py*.<br>
Modifications to Drain3: use cache to store logs and their corresponding clusters, reducing the repetition of the tree search clustering process.<br>
In transformer.py, the *get_mask* function is used to implement the data processing of the logs to get a structured representation of the logs (mask).
*DrainCluster* implements the clustering process on the mask to get the clustered result.
*RayProducer* stores the clustered information in the Redis database. 
After clustering, if there is a newly appeared cluster or the template of the cluster has changed, this log and its corresponding template need to be stored in the *new_template* stream. 
If there is no change, save the log id and template id to the *old_template* stream.
*RayConsumer* completes the whole process of fetching logs from the Redis database, preprocessing logs, clustering, and adding result to Redis.
