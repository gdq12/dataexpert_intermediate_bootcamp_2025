### Step 1️⃣: Setup code

1. Copy into `~/git_repos/dataexpert_intermediate_bootcamp_2025/1_dimensional_data_modeling/1_setup_postgres` from this [directory](https://github.com/DataExpert-io/data-engineer-handbook/tree/main/intermediate-bootcamp/materials/1-dimensional-data-modeling):

   - docker-compose.yml

   - Makefile

   - .en (formally example.env)

### Step 2️⃣: Start PostgreSQL via Docker 

1. Start PostgreSQL & PGAdmin in containers:

   ```{bash}
   make up
   ```

### Step 3️⃣: Connect to PostgreSQL

1. Go to [http://localhost:5050](http://localhost:5050) 

2. Log in using the credentials from your `.env` file  

3. Create a new server:  

   - **Name**: Name of your choice  
   - **Host**: `my-postgres-container`  
   - **Port**: `5432`  
   - **Database**: `postgres`  
   - **Username**: `postgres`  
   - **Password**: `postgres`  
   - ✅ Save Password  

4. Click **Save** —-> connected!

### Step 4️⃣: Verify data loaded

1. in the PGadmin UI, check to see if the tables were loaded --> they werent 

2. enter the running server container:

   ```bash
   docker exec -it my-postgres-container bash
   ```

3. Run the restore manually from inside the container:

   ```bash
   pg_restore -U $POSTGRES_USER -d $POSTGRES_DB /docker-entrypoint-initdb.d/data.dump
   ```

4. check if tables are loaded:

   ```bash
   psql -U postgres -d postgres -c '\dt'
   ```

### Step 5️⃣: Wrap up/done with everything 

1. spin everything back down 

   ```bash
   docker compose stop
   ```