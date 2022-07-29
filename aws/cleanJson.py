import json

filename = "C:/Users/agrawalk/Documents/Code/Avro_validator/dt2/systems2/data/systemsDt2.json"
with open(filename,'r') as fobj:
    data = fobj.read()

data = json.loads(data)

for record in data:
    a = record.values()
    b = record.keys()
    for val in zip(b,a):
        if isinstance(val[1], str):
            record[val[0]] = val[1].strip()

with open(filename, 'w') as f:
    json.dump(data, f, indent=4)

