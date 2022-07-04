docker-compose -f docker-compose.yml up --build --no-start
docker-compose -f docker-compose.yml stop
docker-compose -f docker-compose.yml rm -f
docker-compose -f docker-compose.yml up -d