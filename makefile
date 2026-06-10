up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

bash:
	docker compose exec airflow bash

sensor:
	echo "ready" > logs/data/data_ready.csv