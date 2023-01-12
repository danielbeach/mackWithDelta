## mackWithDelta
testing out the mack Python package for Delta functionality.

To build the `Dockerfile` run `docker build . --tag=mack-delta`

To enter the `Docker` image run `docker run -v .: -it mack-delta /bin/bash`

To run the `main.py` script run `/spark/bin/spark-submit --packages io.delta:delta-core_2.12:0.8.0  main.py`