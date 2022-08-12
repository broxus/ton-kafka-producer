docker buildx build -t kafka .
docker run --mount type=bind,src="$(pwd)/config.yaml",target="/config.yml" --mount type=bind,src="$(pwd)/ton-global.config.json",target="/ton-global.config.json"  -v db:/db -p 0.0.0.0:30310:30310/udp -p 127.0.0.1:30000:12345  --name kafka kafka:latest