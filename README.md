# IMP-submission
In this document every file in this repository and its relevance to the project is explained.

## Iteration 1
The prerequisite for this iteration is to run a NiFi instance as described in [the NiFi repository](https://github.com/ics-unisg/nassy).

**line_count:** This script aggregets the gazes by line. It needs to be placed in nifi/processors.<br>

**Nifi_Template:** The template for our NiFi implementation. Needs to be loaded via the NiFi Web-UI as described in the repository.<br>

**simulator_lines:** This script replays the prerecorded gaze data to NiFi. The URL needs to be adapted accordingly. The raw data file need to be added to a folder data in the same directory. TODO: URL im script drinlassen?<br>

**frontend:** This is the custom frontend we implemented. TODO: How to run this @Erik?

## Iteration 2
**kafka_simulator:** This script replays the prerecorded gaze data to the respective Kafka topic. The raw data file need to be added to a folder data in the same directory.<br>

**docker-compose:** Spins up all relevant components of our custom architecture.<br>

**spark-job:** Connect to the jupyter frontend of the Spark container. Access Host-IP:8888 and follow the instructions to log in. Afterwards copy this scrip and run it via <em>spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 spark_job.py</em> in a shell inside the container.<br>

**database_scrip:** Writes the results of the line counting to the database. This script can run outside a container but needs to be connected to the corresponding port of the Kafka broker.

## Documentation
**DemoVideo:** Demonstration of the Grafana frontend of our system. On the left the log of the script replaying the data can be seen. Notice the small latency between finishing the replay script and the visualization.

**Presentation:** The slides of the final presentation.

**Report:** Our project report.