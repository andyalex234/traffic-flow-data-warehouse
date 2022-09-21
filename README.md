![images](https://user-images.githubusercontent.com/59474650/191608056-3f1af3c5-4ad3-4df7-9983-29ae2b1b9cff.png)
# traffic-flow-data-warehouse
A scalable data warehouse that will host the vehicle trajectory data extracted by analysing footage taken by swarm drones and static roadside cameras. 
## Business Need

You and your colleagues have joined to create an AI startup that deploys sensors to businesses, collects data from all activities in a business - people’s interaction, traffic flows, smart appliances installed in a company. Your startup helps organizations obtain critical intelligence based on public and private data they collect and organize.

A city traffic department wants to collect traffic data using swarm UAVs (drones) from a number of locations in the city and use the data collected for improving traffic flow in the city and for a number of other undisclosed projects. Your startup is responsible for creating a scalable data warehouse that will host the vehicle trajectory data extracted by analyzing footage taken by swarm drones and static roadside cameras.

The data warehouse should take into account future needs, organize data such that a number of downstream projects query the data efficiently. You should use the Extract Load Transform (ELT) framework using DBT.  Unlike the Extract, Transform, Load (ETL), the ELT framework helps analytic engineers in the city traffic department setup transformation workflows on a need basis.

## Data

In [Downloads – pNEUMA | open-traffic (epfl.ch)](https://open-traffic.epfl.ch/index.php/downloads/#1599047632450-ebe509c8-1330)
 you can find a pNEUMA data: pNEUMA is an open large-scale dataset of naturalistic trajectories of half a million vehicles that have been collected by a one-of-a-kind experiment by a swarm of drones in the congested downtown area of Athens, Greece. Each file for a single (area, date, time) is ~87MB data.

# Tech Stack
![Airflow+dbt](https://user-images.githubusercontent.com/59474650/191607588-f9745b50-6017-48d9-999b-fcfe64465dad.png)
