# Object oriented Python code to sync data from Cin7Core and Xero APIs to an Azure SQL database.
# Gives options for complete or incremental updates.
# Requires Cin7Core_API.json and Xero_API.json files. The config.json file was not included as it contains the API credentials and database connection string.
# The json config files allow for non-programmers to easily modify which endpoints and fields are synced.
# API calls and database writing are executed asynchronously for speed.
# The database writing function automatically alters the table columns of string fields if the data length exceeds the current column length. This avoids the need for a technical user to identify and alter the columns manually.
# The SQL database tables include primary and foreign key constraints. The config files are ordered in a way to ensure that parent tables are updated before child tables.
# Staging tables are used to allow for upserts.

import requests
import pyodbc
import asyncio
import json
import math
import time
import datetime
import dateutil

class Cin7CoreApi:
    def __init__(self):
        with open("config.json", "r") as f:
            config = json.load(f)

        self.url = config["Cin7CoreApi"]["url"]
        self.headers = config["Cin7CoreApi"]["headers"]
        
        with open("Cin7Core_API.json", "r") as f:
            self.config = json.load(f)

    def params(self, params, start_date=None):
        mod_params = {}
        for k, v in params.items():
            if v == "__DATE__":
                if start_date:
                    mod_params[k] = start_date
            else:
                mod_params[k] = v
        return mod_params

    def get_data(self, data_type, endpoint, page=None, params=None, key=None, id=None, delay=0):   
        if data_type == "page":
            mod_params={"Page": page, "Limit": 1}
        elif data_type == "table":
            mod_params={"Page": page, "Limit": 1000}
        elif data_type == "record":
            mod_params={key: id}

        if params:
            mod_params.update(params)

        while True:
            time.sleep(delay)
            try:
                response = requests.get(self.url + endpoint, headers=self.headers, params=mod_params)
                response.raise_for_status()

                if response.headers.get("Content-Type") != "application/json; charset=utf-8":
                    continue

                if data_type == "page":
                    return math.ceil(response.json().get("Total", 0) / 1000)
                else:
                    return response.json()

            except requests.exceptions.RequestException:
                delay = min(delay + 1, 10)
                continue

    @staticmethod
    def format_date(date):
        if not date:
            return None
        try:
            return datetime.datetime.fromisoformat(date).date()
        except (ValueError, TypeError):
            try:
                return dateutil.parser.parse(date, dayfirst=True).date()
            except (ValueError, TypeError):
                return None

    def field_list(self, item, fields, date_fields, parent=None, parent_id=None, item2=None, fields2=None, item3=None, fields3=None):
        result = []
        parent_count = 0
        for field in fields:
            if parent and field == parent and parent_count == 0:
                value = parent_id
                parent_count = 1
            elif item2 and field in fields2:
                value = item2.get(field)
            elif item3 and field in fields3:
                value = item3.get(field)
            else:
                value = item.get(field)
            if field in date_fields:
                value = self.format_date(value)
            result.append(value)

        if all(v is None for v in result):
            return None
        
        return tuple(result)

    def mod_fields(self, fields, mod_fields):
        return [fields[0]] + [mod_fields.get(field, field) for field in fields[1:]]

class XeroApi:
    def __init__(self):
        with open("config.json", 'r') as f:
            config = json.load(f)

        self.clientId = config["Xero"]["clientId"]
        self.clientSecret = config["Xero"]["clientSecret"]
        self.tokenUrl = config["Xero"]["tokenUrl"]
        self.connUrl = config["Xero"]["connUrl"]
        self.url = config["Xero"]["url"]

        with open("Xero_API.json", "r") as f:
            self.config = json.load(f)

    def access(self, delay=0):
        while True:
            time.sleep(delay)
            try:
                response = requests.post(self.tokenUrl,data={"grant_type": "client_credentials"}, auth=(self.clientId, self.clientSecret))
                response.raise_for_status()
                token = response.json()["access_token"]
                headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
                delay = 0
                break
            except requests.exceptions.RequestException:
                delay = min(delay + 5, 10)
                continue

        while True:
            time.sleep(delay)
            try:
                response = requests.get(self.connUrl, headers=headers)
                response.raise_for_status()
                tenant = response.json()[0]["tenantId"]
                return token, tenant
            except requests.exceptions.RequestException:
                delay = min(delay + 5, 10)
                continue

    def get_data(self, data_type, token, tenant, endpoint, start_date=None, page=None, delay=0):
        headers = {"Authorization": f"Bearer {token}", "Xero-Tenant-Id": tenant, "Accept": "application/json"}
        
        if start_date:
            headers.update({"if-Modified-Since": start_date})
        
        if data_type == "page":
            params = {"page": 1, "pageSize": 1000}
        elif data_type == "paged":
            params = {"page": page, "pageSize": 1000}
        else:
            params = None

        while True:
            time.sleep(delay)
            try:
                response = requests.get(self.url + endpoint, headers=headers, params=params)
                response.raise_for_status()
                if data_type == "page":
                    return response.json()["pagination"]["pageCount"]
                else:
                    return response.json().get(endpoint, [])
            except requests.exceptions.RequestException:
                delay = min(delay + 5, 10)
                continue

    @staticmethod
    def format_date(unixtimestamp):
        if unixtimestamp:
            date = datetime.datetime.fromtimestamp(int(unixtimestamp[6:19]) / 1000).strftime('%Y-%m-%d')
            return date
        return None

    def field_list(self, item, fields, date_fields, parent=None, parent_id=None):
        result = []

        for field in fields:
            if parent is not None and field == parent:
                value = parent_id
            elif "_" in field:
                parts = field.split("_")
                value = item
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        value = None
                        break

            else:
                value = item.get(field)

            if field in date_fields:
                value = self.format_date(value)
            result.append(value)

        if all(v is None for v in result):
            return None
        
        return tuple(result)

class Database:
    def __init__(self):
        with open("config.json", "r") as f:
            config = json.load(f)

        self.db_conn = config["Azure"]

    def gen_query(self, query, params=None):
        connection = pyodbc.connect(self.db_conn, autocommit=False)
        cursor = connection.cursor()
        cursor.fast_executemany = True

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        cursor.connection.commit()

        connection.close()

    @staticmethod
    def alter_query(table, col, length, staging=None):
        length = "MAX" if length > 8000 else math.ceil(length / 100) * 100
        schemas = ["staging", "dbo"] if staging else ["dbo"]

        statements = []
        for schema in schemas:
            statements.append(f"ALTER TABLE {schema}.{table} ALTER COLUMN {col} VARCHAR({length});")

        return statements

    @staticmethod
    def col_length(cursor, table, col):
        query = """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ? AND COLUMN_NAME = ?
        """
        cursor.execute(query, (table, col))
        length = cursor.fetchone()[0]
        return length if length else None

    def upsert(self, table, fields, id_field=None, schema=None):
        placeholders = ", ".join(["?"] * len(fields))
        fields_list = ", ".join(fields)
        set_clause = ", ".join([f"{field}=s.{field}" for field in fields])
        values = ", ".join([f"s.{field}" for field in fields])
        
        if schema:
            return f"INSERT INTO {schema}.{table} ({fields_list}) VALUES ({placeholders});"
        else:
            return f"""
                MERGE dbo.{table} AS t
                USING staging.{table} AS s
                ON t.{id_field} = s.{id_field}
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({fields_list}) VALUES ({values});
            """

    def write_data(self, data, table, fields, i_query, m_query=None, id_field=None, ids=None):
        connection = pyodbc.connect(self.db_conn, autocommit=False)
        cursor = connection.cursor()
        cursor.fast_executemany = True

        if not m_query:
            if id_field:
                cursor.execute(f"DELETE FROM {table} WHERE {id_field} IN ({','.join(['?'] * len(ids))})", ids)
                cursor.connection.commit()
            else:    
                cursor.execute(f"DELETE FROM {table};")
                cursor.connection.commit()

        max_retry=2
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            attempt = 0
            
            while attempt < max_retry: 
                try:
                    cursor.executemany(i_query, batch)
                    cursor.connection.commit()
                    break
                except Exception as e:
                    if attempt == 0 and "truncation" in str(e).lower():
                        max_lengths = {}
                        for row in batch:
                            for idx, value in enumerate(row):
                                if isinstance(value, str) and len(value) > 255:
                                    col = fields[idx]
                                    max_lengths[col] = max(max_lengths.get(col, 0), len(value)) 

                        for col, length in max_lengths.items():
                            current_len = self.col_length(cursor, table, col)
                            if current_len and current_len < length:
                                if m_query:
                                    queries = self.alter_query(table, col, length, staging=True)
                                else:
                                    queries = self.alter_query(table, col, length)
                                for query in queries:
                                    cursor.execute(query)
                                cursor.connection.commit()
   
                    if attempt == 1:
                        print(f"{table} error: {e}") 
                    cursor.connection.rollback()
                    attempt += 1

        if m_query:
            cursor.execute(m_query)
            cursor.connection.commit()
            cursor.execute(f"TRUNCATE TABLE staging.{table};")
            cursor.connection.commit()

        connection.close()

class Process:
    def __init__(self, db: Database, cin7core: Cin7CoreApi=None, xero: XeroApi=None):
        self.db = db
        self.cin7coreapi = cin7core
        self.xeroapi = xero        

    def constraints(self, cfg, mode, api):
        if api == "cin7core":
            table = cfg.get("table", None)
        elif api == "xero":
            table = "xero_" + cfg.get("endpoint", "")
        nested = cfg.get("nested", [])

        if table:
            if mode == "nocheck":
                query = f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL;"
            else:
                query = f"ALTER TABLE {table} CHECK CONSTRAINT ALL;"
            self.db.gen_query(query)
        else:
            for value in nested:
                nested_table = value.get("table", None)
                if nested_table is None:
                    nested_table = table + "_" + value.get("nest", "")

                if mode == "nocheck":
                    query = f"ALTER TABLE {nested_table} NOCHECK CONSTRAINT ALL;"
                else:
                    query = f"ALTER TABLE {nested_table} CHECK CONSTRAINT ALL;"
                self.db.gen_query(query)

    def truncate_staging(self, cfg, api):
        if api == "cin7core":
            table = cfg.get("table", None)
            staging = cfg.get("staging", None)
        elif api == "xero":
            table = "xero_" + cfg.get("endpoint", "")
            staging = True

        if table and staging:
            self.db.gen_query(f"TRUNCATE TABLE staging.{table};")

    def backup_func(self, cfg, api):
        if api == "cin7core":
            table = cfg.get("table", None)
        elif api == "xero":
            table = "xero_" + cfg.get("endpoint", "")
        backup = cfg.get("backup", False)
        nested = cfg.get("nested", [])

        if backup:
            self.db.gen_query(f"TRUNCATE TABLE b_{table};")
            self.db.gen_query(f"INSERT INTO b_{table} SELECT * FROM dbo.{table};")
        else:
            for value in nested:
                if api == "cin7core":
                    table = value.get("table", None)
                elif api == "xero":
                    table = "xero_" + value.get("endpoint", "")
                backup = value.get("backup", False)

                if backup:
                    self.db.gen_query(f"TRUNCATE TABLE b_{table};")
                    self.db.gen_query(f"INSERT INTO b_{table} SELECT * FROM dbo.{table};")

    async def cin7core(self, cfg, days=None):
        endpoint = cfg.get("endpoint")
        list_field = cfg.get("list_field")
        params = cfg.get("params", None)
        params_record = cfg.get("params_id", None)
        table = cfg.get("table", None)
        fields = cfg.get("fields", [])
        date_fields = cfg.get("date_fields", [])
        mod_fields = cfg.get("mod_fields", {})
        staging = cfg.get("staging", None)
        id_field = cfg.get("id_field", None)
        nested = cfg.get("nested", [])
        endpoint_id = cfg.get("endpoint_id", None)
        list_id = cfg.get("list_id", None)
        list_date = cfg.get("list_date", None)
        fom_date = cfg.get("fom_date", None)

        if not id_field:
            id_field = fields[0]    

        if mod_fields:
            db_fields = self.cin7coreapi.mod_fields(fields, mod_fields)
        else:
            db_fields = fields

        if staging:
            i_query = self.db.upsert(table, db_fields, schema="staging")
            m_query = self.db.upsert(table, db_fields, id_field)
        else:
            i_query = self.db.upsert(table, db_fields, schema="dbo")
            m_query = None

        if days is not None:
            if fom_date:
                start_date = (datetime.datetime.now(datetime.timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0) - dateutil.relativedelta.relativedelta(months=1)).replace(tzinfo=None).isoformat()
            else:
                start_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None).isoformat()

        if params:
            if days is not None:
                params = self.cin7coreapi.params(params, start_date)
            else:
                params = self.cin7coreapi.params(params)

        if not nested: # Tables
            pages = await asyncio.to_thread(self.cin7coreapi.get_data, "page", endpoint, page=1, params=params)

            data = []
            for page in range(1, pages + 1):
                response = await asyncio.to_thread(self.cin7coreapi.get_data, "table", endpoint, page, params)

                for item in response.get(list_field, []):
                    data.append(self.cin7coreapi.field_list(item, fields, date_fields))

            if data:
                await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, m_query)

        elif not endpoint_id: # Nested
            pages = await asyncio.to_thread(self.cin7coreapi.get_data, "page", endpoint, page=1, params=params)

            api_data = []
            for page in range(1, pages + 1):
                response = await asyncio.to_thread(self.cin7coreapi.get_data, "table", endpoint, page, params)
                api_data.extend(response.get(list_field, []))

            if api_data:
                data = []
                for item in api_data:
                    data.append(self.cin7coreapi.field_list(item, fields, date_fields))

                await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, m_query)

                if days is not None:
                    ids = []
                    for item in api_data:
                        ids.append(item.get(id_field))

                for value in nested:
                    nest = value.get("nest")
                    table = value.get("table")
                    fields = value.get("fields")
                    mod_fields = value.get("mod_fields", {})
                    date_fields = value.get("date_fields", [])
                    parent = fields[0]

                    if mod_fields:
                        db_fields = self.cin7coreapi.mod_fields(fields, mod_fields)
                    else:
                        db_fields = fields

                    i_query = self.db.upsert(table, db_fields, schema="dbo")
                    m_query = None

                    data = []
                    for item in api_data:
                        parent_id = item.get(id_field)
                        nested_items = item.get(nest, [])

                        for nested_item in nested_items:
                            data.append(self.cin7coreapi.field_list(nested_item, fields, date_fields, parent=parent, parent_id=parent_id))
                    
                    if data:
                        if days is not None:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, id_field=parent, ids=ids)
                        else:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query)        

        else: # Records
            pages = await asyncio.to_thread(self.cin7coreapi.get_data, "page", endpoint_id, page=1, params=params)

            if list_date and days:
                start_list_date = datetime.datetime.fromisoformat(start_date).date()
                
            ids = []
            for page in range(1, pages + 1):
                response = await asyncio.to_thread(self.cin7coreapi.get_data, "table", endpoint_id, page, params)

                for id in response.get(list_field, []):
                    if not list_date or not days:
                        ids.append(id.get(list_id))
                    else:
                        update_date = datetime.datetime.fromisoformat(id.get(list_date)).date()
                        if update_date >= start_list_date:
                            ids.append(id.get(list_id))

            if ids and ids != [None]: 
                api_data = []
                for id in ids:
                    response = await asyncio.to_thread(self.cin7coreapi.get_data, "record", endpoint, params=params_record, key=id_field, id=id)
                    api_data.append(response)

                for value in nested:
                    nest = value.get("nest")
                    nest2 = value.get("nest2", None)
                    nest3 = value.get("nest3", None)
                    table = value.get("table")
                    fields = value.get("fields")
                    mod_fields = value.get("mod_fields", {})
                    nest2_fields = value.get("nest2_fields", [])
                    nest3_fields = value.get("nest3_fields", [])
                    date_fields = value.get("date_fields", [])
                    parent = fields[0]

                    if mod_fields:
                        db_fields = self.cin7coreapi.mod_fields(fields, mod_fields)
                    else:
                        db_fields = fields

                    i_query = self.db.upsert(table, db_fields, schema="dbo")
                    m_query = None

                    data = []
                    for item in api_data:
                        id = item.get(id_field)
                        records = item.get(nest, [])
                        records = records if isinstance(records, list) else [records]

                        for record in records:
                            if nest2 is None:
                                data.append(self.cin7coreapi.field_list(record, fields, date_fields, parent=parent, parent_id=id))
                            else:
                                records2 = record.get(nest2, [])
                                records2 = records2 if isinstance(records2, list) else [records2]

                                for record2 in records2:
                                    if nest3 is None:
                                        data.append(self.cin7coreapi.field_list(record, fields, date_fields, parent=parent, parent_id=id, item2=record2, fields2=nest2_fields))
                                    else:
                                        records3 = record2.get(nest3, [])

                                        for record3 in records3:
                                            data.append(self.cin7coreapi.field_list(record, fields, date_fields, parent=parent, parent_id=id, item2=record2, fields2=nest2_fields, item3=record3, fields3=nest3_fields))

                    if data:
                        if days is not None:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, id_field=parent, ids=ids)
                        else:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query)

    async def xero(self, cfg, token, tenant, days=None, start_date=None):
        endpoint = cfg.get("endpoint")
        paged = cfg.get("paged", False)
        fields = cfg.get("fields")
        date_fields = cfg.get("date_fields", [])
        nested = cfg.get("nested", [])
        table = f"xero_{endpoint}"
        id_field = fields[0]

        i_query = self.db.upsert(table, fields, schema="staging")
        m_query = self.db.upsert(table, fields, id_field)

        if not paged:
            api_data = []
            response = await asyncio.to_thread(self.xeroapi.get_data, "table", token, tenant, endpoint, start_date)
            api_data.extend(response)

        else:
            pages = await asyncio.to_thread(self.xeroapi.get_data, "page", token, tenant, endpoint, start_date)

            api_data = []
            for page in range(1, pages + 1):
                response = await asyncio.to_thread(self.xeroapi.get_data, "paged", token, tenant, endpoint, start_date, page=page)
                api_data.extend(response)

        if api_data:
            data = []
            for item in api_data:
                data.append(self.xeroapi.field_list(item, fields, date_fields))

            if data:
                await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, m_query)

            if nested:
                if days is not None:
                    ids = []
                    for item in api_data:
                        ids.append(item.get(id_field))

                for value in nested:
                    nest = value.get("nest")
                    fields = value.get("fields")
                    date_fields = value.get("date_fields", [])
                    table = f"xero_{endpoint}_{nest}"
                    parent = fields[0]

                    i_query = self.db.upsert(table, fields, schema="dbo")

                    data = []
                    for item in api_data:
                        parent_id = item.get(id_field)
                        nested_items = item.get(nest, [])

                        for nested_item in nested_items:
                            data.append(self.xeroapi.field_list(nested_item, fields, date_fields, parent=parent, parent_id=parent_id))
                    
                    if data:
                        if days is not None:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query, id_field=parent, ids=ids)
                        else:
                            await asyncio.to_thread(self.db.write_data, data, table, fields, i_query)

class Update:
    def __init__(self, process: Process):
        self.process = process

    def backup(self):
        for cfg in self.process.cin7coreapi.config:
            self.process.backup_func(cfg, "cin7core")

        for cfg in self.process.xeroapi.config:
            self.process.backup_func(cfg, "xero")

        print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Backup completed")

    async def update_cin7core(self, days=None):
        config = self.process.cin7coreapi.config

        for cfg in config:
            self.process.truncate_staging(cfg, "cin7core")
            self.process.constraints(cfg, "nocheck", "cin7core")
            
        tasks = [asyncio.create_task(self.process.cin7core(cfg, days)) for cfg in config]
        await asyncio.gather(*tasks)

        for cfg in config:
            self.process.constraints(cfg, "check", "cin7core")

    async def update_xero(self, days=None):
        config = self.process.xeroapi.config
        token, tenant = self.process.xeroapi.access()

        if days is not None:
            start_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None).isoformat()
        else:
            start_date = None

        for cfg in config:
            self.process.constraints(cfg, "nocheck", "xero")
            self.process.truncate_staging(cfg, "xero")

        self.process.db.gen_query("TRUNCATE TABLE xeroAccess; INSERT INTO xeroAccess (Token, Tenant_ID) VALUES (?, ?);", (token, tenant))

        tasks = [asyncio.create_task(self.process.xero(cfg, token, tenant, days, start_date)) for cfg in config]
        await asyncio.gather(*tasks)

        for cfg in config:
            self.process.constraints(cfg, "check", "xero")

    async def update_all(self, days=None):
        start_time = time.time()
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Updating database..")
        
        await asyncio.gather(
            self.update_cin7core(days),
            self.update_xero(days)
        )

        print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Data update completed in {time.strftime('%Hh %Mm %Ss', time.gmtime(time.time() - start_time))}")

if __name__ == "__main__":

    update = Update(Process(db=Database(), cin7core=Cin7CoreApi(), xero=XeroApi()))

    while True:
        try: 
            mode = input("Choose: Full Update (F) or Regular Updates (R) or Single Update (S): ").upper().strip()
            
            if mode == "R":
                backup_count = 0
                while True:
                    try:
                        days = int(input("Enter number of days to update (0 = today): "))
                        break
                    except ValueError:
                        continue
                
                while True:
                    try:
                        interval = int(input("Enter interval between updates in minutes: ")) * 60
                        break
                    except ValueError:
                        continue

                while True:
                    if datetime.datetime.now().weekday() < 5 and 6 <= datetime.datetime.now().hour < 18:
                        try:
                            asyncio.run(update.update_all(days))
                            backup_count = 0
                        except Exception as e:
                            print(f"Error: {e}")    
                        time.sleep(interval)
                    elif (datetime.datetime.now().hour >= 18 or datetime.datetime.now().hour < 6) and backup_count == 0:
                        try:
                            update.backup()
                            backup_count += 1
                        except Exception as e:
                            print(f"Error: {e}")    
                    else:
                        time.sleep(interval)

            elif mode == "S" or mode == "F":

                if mode == "S":
                    days = 0
                else:
                    days = None

                while True:
                    try:
                        api = input("Choose: Cin7Core (C) or Xero (X) or Both (B): ").upper().strip()
                        if api == "C" or api == "X":
                            start_time = time.time()
                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Updating database..")
                            if api == "C":
                                asyncio.run(update.update_cin7core(days))
                            else:
                                asyncio.run(update.update_xero(days))
                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} - Data update completed in {time.strftime('%Hh %Mm %Ss', time.gmtime(time.time() - start_time))}")
                        elif api == "B":
                            asyncio.run(update.update_all(days))
                        else:
                            raise ValueError
                        break
                    except ValueError:
                        continue

            else:
                raise ValueError
            
        except ValueError:
            continue