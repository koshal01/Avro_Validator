# Import the Pandas library as pd
import pandas as pd
import json

# Read the Parquet File as DataFrame
data = pd.read_parquet("C:/Users/agrawalk/Documents/Code/Avro_validator/dt1/performance/data/part-00001-1c24a030-c0d7-4f5a-af1d-26508d4f13a6.c000.snappy.parquet")

#converting into json
df = data.to_json(orient="records")
df = json.loads(df)

#creating a json file
with open('performance.json', 'w') as f:
    json.dump(df, f, indent=4)

# Display the data
# print(data)