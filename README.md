<p align="center">
	<h1 align="center">
    <p align="center">
    <img src="https://user-images.githubusercontent.com/17098382/118923140-7ac14080-b911-11eb-9a9c-2709ad0a3a0d.png" width="fit-content" alt="Logo">
</p>
  </h1>
  <p align="center">Data Challenge</p>
</p>

# [DEMO!!!](https://trello-clone-liv-saude.herokuapp.com)

## Files

### Database Connection

These files in below are respective to database connection:

- database.py
- database.ini
- config.py

### Dataset

Dataset provided by Loadsmart in format Comma-Separeted Values.

- data.csv

### Document Challenge

Document with specs of this challenge.

- challenge.pdf

### Git Ignore

This file is respective to ignore some files generated in local, e.g. for cache.

- .gitignore

### ETL Files

This files in below threat the data and add to database.

- create_address.py
- create_carrier.py
- create_deliver.py
- create_equipment.py
- create_shipper.py
- create_sourcing_channel.py

### Main

This is the main file of this ETL.

- main.py

## Connection to Database:

This database is hosted by: https://api.elephantsql.com/, a free host using a free database PostgreSQL. Follow in below the configs.

```python
host = "kashin.db.elephantsql.com"
database = "okqvfizk"
user = "okqvfizk"
password = "pbaudbEor8zO8UFJ-rfMbdivEzZmUOWb"
port = 5432
```

## Creating tables on database:

Follow in below the tabled to describe the dataset:

### Table Address

```sql
-- Drop table

-- DROP TABLE public.address;

CREATE TABLE public.address (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	city varchar NOT NULL,
	state varchar(2) NOT NULL,
	CONSTRAINT address_pk PRIMARY KEY (id),
	CONSTRAINT address_un UNIQUE (state)
);

```

### Table Carrier

```sql
-- Drop table

-- DROP TABLE public.carrier;

CREATE TABLE public.carrier (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	channel numeric NOT NULL,
	"name" varchar NULL,
	equipment_id uuid NULL,
	carrier_rating numeric NULL,
	sourcing_channel_id uuid NULL,
	vip_carrier bool NOT NULL DEFAULT false,
	carrier_dropped_us_count numeric NOT NULL DEFAULT 0,
	CONSTRAINT carrier_pk PRIMARY KEY (id),
	CONSTRAINT carrier_un UNIQUE (channel),
	CONSTRAINT carrier_un2 UNIQUE (id),
	CONSTRAINT carrier_fk FOREIGN KEY (equipment_id) REFERENCES equipment(id),
	CONSTRAINT carrier_fk_1 FOREIGN KEY (sourcing_channel_id) REFERENCES sourcing_channel(id)
);
```

### Table Deliver

```sql
-- Drop table

-- DROP TABLE public.deliver;

CREATE TABLE public.deliver (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	loadsmart_id numeric NOT NULL,
	from_id uuid NOT NULL,
	to_id uuid NOT NULL,
	quote_date timestamptz NULL,
	book_date timestamptz NULL,
	source_date timestamptz NULL,
	pickup_date timestamptz NULL,
	delivery_date timestamptz NULL,
	book_price numeric NULL,
	source_price float4 NULL,
	pnl float4 NULL,
	mileage float4 NULL,
	carrier_id uuid NULL,
	shipper_id uuid NOT NULL,
	carrier_on_time_to_pickup bool NOT NULL DEFAULT false,
	carrier_on_time_to_delivery bool NOT NULL DEFAULT false,
	carrier_on_time_overall bool NOT NULL DEFAULT false,
	pickup_appointment_time timestamptz NULL,
	delivery_appointment_time timestamptz NULL,
	has_mobile_app_tracking bool NOT NULL DEFAULT false,
	has_macropoint_tracking bool NOT NULL DEFAULT false,
	has_edi_tracking bool NOT NULL DEFAULT false,
	contracted_load bool NOT NULL DEFAULT false,
	load_booked_autonomously bool NOT NULL DEFAULT false,
	load_sourced_autonomously bool NOT NULL DEFAULT false,
	load_was_cancelled bool NOT NULL DEFAULT false,
	CONSTRAINT deliver_pk PRIMARY KEY (id),
	CONSTRAINT deliver_un2 UNIQUE (loadsmart_id),
	CONSTRAINT deliver_fk FOREIGN KEY (carrier_id) REFERENCES carrier(id),
	CONSTRAINT deliver_fk_1 FOREIGN KEY (shipper_id) REFERENCES shipper(id),
	CONSTRAINT deliver_fk_2 FOREIGN KEY (from_id) REFERENCES address(id),
	CONSTRAINT deliver_fk_3 FOREIGN KEY (to_id) REFERENCES address(id)
);
```

### Table Equipment

```sql
-- Drop table

-- DROP TABLE public.equipment;

CREATE TABLE public.equipment (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	"type" varchar NOT NULL,
	CONSTRAINT equipment_channel_pk PRIMARY KEY (id),
	CONSTRAINT equipment_channel_un2 UNIQUE (type)
);
```

### Table Shipper

```sql
-- Drop table

-- DROP TABLE public.shipper;

CREATE TABLE public.shipper (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	"name" varchar NULL,
	CONSTRAINT shipper_pk PRIMARY KEY (id),
	CONSTRAINT shipper_un1 UNIQUE (id)
);
```

### Table Sourcing Channel

```sql
-- Drop table

-- DROP TABLE public.sourcing_channel;

CREATE TABLE public.sourcing_channel (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	channel numeric NOT NULL,
	CONSTRAINT sourcing_channel_pk PRIMARY KEY (id),
	CONSTRAINT sourcing_channel_un2 UNIQUE (channel)
);
```

## Python Config

```bash
sudo apt install python3-pip
sudo apt-get install -y postgis postgresql-9.3-postgis-2.1
sudo apt-get install libpq-dev python-dev
pip install psycopg2
```

## Python running

```bash
python main.py
```
