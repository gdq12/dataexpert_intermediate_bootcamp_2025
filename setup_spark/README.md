# Unit Testing PySpark Course Getting Started

1. instructions to install spark in global environment 

    * install spark and python locally on [windows](https://discord.com/channels/1106357930443407391/1388500306207178824) or [mac](https://discord.com/channels/1106357930443407391/1388501607641124874).

    * pip install required libraries: `pip install -r requirements_unit_test.txt`

    * to verify successful installation: `python -m pytest`

2. instructions to setup docker container instead

    * spin up the docker containers from [docker-compose.yaml](docker-compose.yaml) (via `make up`), since they are interdependent 

    * get the container ID that is running docker image `tabulario/spark-iceberg` via `docker ps -a`

    * enter the running container via terminal: `docker exec -ti containerID sh`

    * copy over needed files to the running docker container: 

        ```{bash}
        docker cp requirements_unit_test.txt containerID:/

        docker cp src/ containerID:/src/
        ```

    * pip install needed libraries: `pip install -r requirements_unit_test.txt`

    * to verify successful installation: `python -m pytest`

# Spark Fundamentals and Advanced Spark Setup

1. To launch the Spark and Iceberg Docker containers, run in terminal: `make up`

2. access a Jupyter notebook at `localhost:8888`

## ❓ Fix for Spark OutOfMemoryError (Java headspace)

Based on this [post](https://discord.com/channels/1106357930443407391/1388501197341720666). 

1. adjust docker resource allocation:

    * settings --> resource 

    * increase memory limit (8GB --> 16GB)

    * increase disk usage limit (to 128GB)

2. update configs in the iceberg container

    * enter running container in docker desktop go to running container: setup_spark>spark-iceberg

    * go to files>opt>spark>conf> spark-defaults.conf

    * rigt click the file to edit it

    * add the following lines to the file at the bottom 

        ```
        spark.serializer                       org.apache.spark.serializer.KryoSerializer
        spark.driver.memory                    8g
        spark.memory.offHeap.enabled           true
        spark.memory.offHeap.size              8g
        ```

    * when working within the notebooks: reduce bucket size to 4 or 8 from 16.