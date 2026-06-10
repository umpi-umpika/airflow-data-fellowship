if you want to see where is dag store in airflow, you can run this command in terminal 
"""
1. docker exec -it airflow_30-postgres-1 psql -U airflow -d airflow  
2. \dt -> for see all table in database
3. select * from dag; -> for see all dag in databaseq
"""
