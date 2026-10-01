# Mount the apps directory and run the simulator using a temporary python container
docker run --rm --network stephackathon_default -v ${PWD}\apps\simulator:/sim -w /sim python:3.12-slim bash -c "pip install confluent_kafka && python -u sim/main.py kafka:9092"
