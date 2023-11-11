import pg8000
import pandas as pd
import requests
from tqdm import tqdm

# Configure your PostgreSQL connection string - Generic credentials, use your own credentials from your database
db_config = {
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres'
}

# Your Google API Key
google_api_key = 'your_api_key_google_maps' #https://developers.google.com/maps/documentation/javascript/get-api-key

def get_geocode(address):
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={google_api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
    return None, None

# Create a connection to the database
conn = pg8000.connect(**db_config)
cursor = conn.cursor()

# Fetch the address data from the PostgreSQL table
query = """
SELECT id, tipo_logradouro, logradouro, numero, complemento, cep, municipio, uf
FROM estabelecimentos_pvh
"""
cursor.execute(query)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)

# Update each row with latitude and longitude in batches
batch_size = 100
for start in tqdm(range(0, len(df), batch_size), desc="Processing batches"):
    end = start + batch_size
    batch = df.iloc[start:end]
    for index, row in batch.iterrows():
        # here i'm using the name of the columns from my table on my database, please verify your table header
        lat, lng = get_geocode(f"{row['tipo_logradouro']} {row['logradouro']} {row['numero']} {row['complemento']} {row['cep']} {row['municipio']} {row['uf']}")
        if lat is not None and lng is not None:
            update_query = f"""
            UPDATE estabelecimentos_pvh
            SET latitude = '{lat}', longitude = '{lng}'
            WHERE id = {row['id']}
            """
            cursor.execute(update_query)
    conn.commit()  # Commit after each batch

# Close the cursor and connection
cursor.close()
conn.close()
