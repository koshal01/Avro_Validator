import json
import math
import fastavro
from fastavro import writer, reader
from datetime import datetime
from dateutil.relativedelta import relativedelta

import sys
sys.path.insert(0, "../../")

from logger import logger
from errorClass import NotFoundError, MapError

#### GLOBAL VARIABLES ####
i = 0                 # for specifiying current json record
mapFiles_Index = {}   # mapping input with output

def readFile(filename):
    """Read file from remote path.
    Args:
        filename(str): filename to read.
    Returns:
        The contents of the file.
    Raises:
        IOError: Unable to read file
        ValueError: Empty FIle
    """

    data = None
    try:
        with open(filename,'r') as fobj:
            data = fobj.read()
    except (IOError) as err:
        raise IOError(err)

    if not data:
        raise ValueError(f'No data available in {filename}')

    data = json.loads(data)

    return data

def dataTypeCheck(data, Type, field):
    """Checks the data type of output fields.
    Args:
        data(): value whose data type is to be checked
        Type(str): data type the data should be of
        field(str): curent output field for which it is being checked
    Raises:
        TypeError: Data Type mismatch
    """

    msg = Type + "Type"
    if(Type == "null"):      Type = "None"
    elif(Type == "boolean"): Type = "bool"
    elif(Type == "long"):    Type = "int"
    elif(Type == "double"):  Type = "float"
    elif(Type == "string"):  Type = "str"

    if not isinstance(data, eval(Type)):
        raise TypeError(f"The value [{data}] for field [{field}] should be [{msg}]")

def mappingWithInputJson(filename, input_field, output_field):
    """Mapping current record from output file in input file.
    Args:
        filename(str): input filename to read.
        input_map_field(str): key from input file which will be used to map with output file
        output_map_field(str): key from output file
    Returns:
        The list of index of input value at which it got mapped to input file from current record of output file(List)
        Sends the status of whether any index was found or not(bool)
    """

    # reading input file
    input_json = readFile(filename)

    index = 0
    idx = []

    # searching the current output record in input record
    for input in input_json:
        isFound = True
        for input_map in range(len(input_field)):
            if(input[input_field[input_map]] != output_json[i][output_field[input_map]]):
                isFound = False
        
        if(isFound):
            idx.append(index)
        index = index + 1

    isFound = True if len(idx) > 0 else False

    return isFound, idx

def mapFilesDictionary(validator):
    input_filepaths = validator.get("input_filepath")

    if not input_filepaths:
        logger.error(f'Validator Error: The field [input_filepath] was not found in validator of AVRO schema')
        raise KeyError('input_filepath')

    primary_key = output_json[i][validator["output_field"][0][0]]
    mapFiles_Index.update({"primary_key": primary_key })

    for file in range(len(input_filepaths)):
        filename = input_filepaths[file].split('/')
        input_field = validator.get("input_field")[file]
        output_field = validator.get("output_field")[file]

        isFound, idxList = mappingWithInputJson(input_filepaths[file], input_field, output_field)
        primary_key = output_json[i][output_field[0]] #output_map_value

        if(not isFound):
            raise MapError(primary_key, primary_key, output_field[0])

        mapFiles_Index.update({filename[-1]: idxList})

def inputList(validator):
    """Generated input value list using the input field and input filepath provided through validator
    Args:
        validator(dict): metadata provided in avro schema
    Returns:
        Input value list
    Raises:
        NotFoundError: If unable to map input file with output file using provided fields
    """

    input_no = validator["input_filepath"]
    it = 0
    input_value_list = []

    while it < len(input_no):
        input_file = validator["input_filepath"][it]
        filename = input_file.split('/')
        input_field = validator["input_field"][it]

        # reading input file
        input_json = readFile(input_file)

        idxList = mapFiles_Index.get(filename[-1])
        for j in range(len(input_field)):
            input_list = []
            for index in idxList:
                input_data = f"input_json[{index}]"
                ids = input_field[j].split("-")
                for part in ids:
                    input_data += f'''["{part}"]'''
                input_list.append(eval(input_data))
            input_value_list.append(input_list)

        it = it + 1

    return input_value_list

def directMapping(validator, input_value_list, idx, cnt):
    """Extract specified value from input_value_list.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        specified value from input_value_list.
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"
    val = input_value_list[idx][0]

    return val, msg

def isNull(validator, input_value_list, idx, cnt):
    """Checking the specified the input value is null or not
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified
    Returns:
        sends the null_val_cmp[idx] if input value is null 
        Else sends status of "moreValidation"
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"
    null_val_cmp = validator["null_val_cmp"]

    for j in range(idx, idx+cnt):
        for k in range(len(input_value_list[j])):
            if not input_value_list[j][k]:
                return null_val_cmp[idx], msg

    return "moreValidation", msg

def add(validator, input_value_list, idx, cnt):
    """Performs addition operation. 
        If any number is to be added then is specified at that index under field number in validator of avro schema. 
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified
    Returns:
        Added value.
        Message to log into file if any error.
    """

    msg = 'ValueError(f"For field - [{output_field}] Added value does not match - [{data}], [{val}]")'

    val = 0
    for j in range(idx, idx+cnt):
        for k in range(len(input_value_list[j])):
            val += input_value_list[j][k]

    if(validator.get("number")):
        if(validator["number"][idx] != ""):
            val += validator["number"][idx]

    input_value_list.append([val])

    return val, msg

def subtract(validator, input_value_list, idx, cnt):
    """Performs subtraction operation
        If any number is to be subtracted then is specified at that index under field number in validator of avro schema. 
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified
    Returns:
        Subtracted value.
        Message to log into file if any error.
    """
    msg = 'ValueError(f"For field - [{output_field}] Subtracted value does not match - [{data}], [{val}]")'
    
    val = 0
    isAdd = True    
    for j in range(idx, idx+cnt):
        for k in range(len(input_value_list[j])):
            if(isAdd): 
                isAdd = False
                val = input_value_list[j][k]
            else: 
                val -= input_value_list[j][k]

    if(validator.get("number")):
        if(validator["number"][idx] != ""):
            val -= validator["number"][idx]

    input_value_list.append([val])

    return val, msg

def multiply(validator, input_value_list, idx, cnt):
    """Performs multiplication operation.
        If any number is to be multiplied then is specified at that index under field number in validator of avro schema. 
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified
    Returns:
        Multiplied value.
        Message to log into file if any error.
    """

    msg = 'ValueError(f"For field - [{output_field}] Multiply value does not match - [{data}], [{val}]")'

    val = 1
    for j in range(idx, idx+cnt):
        for k in range(len(input_value_list[j])):
            val *= input_value_list[j][k]

    if(validator.get("number")):
        if(validator["number"][idx] != ""):
            val *= validator["number"][idx]

    input_value_list.append([val])

    return val, msg

def divide(validator, input_value_list, idx, cnt):
    """Performs division operation.
        If any number is to be divided then is specified at that index under field number in validator of avro schema. 
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified
    Returns:
        Divided value.
        Message to log into file if any error.
    """
    msg = 'ValueError(f"For field - [{output_field}] Divide value does not match - [{data}], [{val}]")'

    val = input_value_list[idx][0]
    firstTime = True
    for j in range(idx, idx+cnt):
        for k in range(len(input_value_list[j])):
            if input_value_list[j][k] == 0: 
                raise ZeroDivisionError("Division by zero")
                
            if firstTime:
                if k+1 < len(input_value_list[j]):
                    if input_value_list[j][k+1] == 0: 
                        raise ZeroDivisionError("Division by zero")
                    val /= input_value_list[j][k+1]
                firstTime = False
            else:
                val /= input_value_list[j][k]

    if(validator.get("number")):
        if(validator["number"][idx] != ""):
            val /= validator["number"][idx]

    input_value_list.append([val])

    return format(val, '.2f'), msg

def compare(validator, input_value_list, idx, cnt):
    """Compares the input_value from the input_value_list[idx] with the value specified using comparison 
        specified at the index under field compare_val & relational_operator reps. in validator of avro schema.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        If cond satisfies True -> output_cmp_with[0] else False -> output_cmp_with[1]. 
        If we want to do logical Comparison or intermediate check then it sends the result obtained after comparison
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"

    relational_operator = validator["relational_operator"][idx]
    compare_val = validator["compare_val"]

    if(compare_val[idx] == "current_date"):
        compare_val[idx] = datetime.now()

    condMatch = False
    if(relational_operator == "greaterThan"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] >= compare_val[idx]):
                condMatch = True
            else:
                condMatch = False
                break
    if(relational_operator == "lessThan"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] <= compare_val[idx]):
                condMatch = True
            else:
                condMatch = False
                break      
    elif(relational_operator == "equalTo"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] == compare_val[idx]):
                condMatch = True
            else:
                condMatch = False
                break
    elif(relational_operator == "isNotEqualTo"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] != compare_val[idx]):
                condMatch = True
            else:
                condMatch = False
                break

    if isinstance(compare_val[idx], datetime):
        compare_val[idx] = "current_date"

    input_value_list.append([condMatch])

    return condMatch, msg

def countCompare(validator, input_value_list, idx, cnt):
    """Compares the input_value from the input_value_list[idx] with the value specified using comparison 
        specified at the index under field compare_val & relational_operator reps. in validator of avro schema.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        If cond satisfies True -> output_cmp_with[0] else False -> output_cmp_with[1]. 
        If we want to do logical Comparison or intermediate check then it sends the result obtained after comparison
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"

    relational_operator = validator["relational_operator"][idx]
    compare_val = validator["compare_val"]
    countCompare_val = validator["countCompare_val"]

    condMatch = 0
    if(relational_operator == "greaterThan"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] >= compare_val[idx]):
                condMatch = condMatch + 1
    if(relational_operator == "lessThan"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] <= compare_val[idx]):
                condMatch = condMatch + 1
    elif(relational_operator == "equalTo"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] == compare_val[idx]):
                condMatch = condMatch + 1
    elif(relational_operator == "isNotEqualTo"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] != compare_val[idx]):
                condMatch = condMatch + 1
    elif(relational_operator == "isNotEqualTo"):
        for k in range(len(input_value_list[idx])):
            if(input_value_list[idx][k] != compare_val[idx]):
                condMatch = condMatch + 1

    input_value_list.append([condMatch])

    val = True if(condMatch >= countCompare_val[idx]) else False

    return val, msg

def logicalCompare(validator, input_value_list, idx, cnt):
    """ Compares the input_value_list from the idx using comparison 
        specified at the index under field logical_operator in validator of avro schema.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        If cond satisfies True -> output_cmp_with[0] else False -> output_cmp_with[1]. 
        If we want to do intermediate check then it sends the result obtained after comparison
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"

    logical_operator = validator["logical_operator"]

    for operator in logical_operator:
        if operator == "and":
            isVal = True if((input_value_list[idx][0]) and (input_value_list[idx+1][0])) else False
            input_value_list.insert(idx+2, [isVal])
        elif operator == "or":
            isVal = True if((input_value_list[idx][0]) or (input_value_list[idx+1][0])) else False
            input_value_list.insert(idx+2, [isVal])
        else:
            raise TypeError(f"Provide correct logical operator [{operator}]")
        idx = idx + 2

    val = input_value_list[-1][0]

    return val, msg

def epochConverter(validator, input_value_list, idx, cnt):
    """ Converts the input value date into epoch time i.e no of ms from 1st Jan, 1970.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        Number of ms from 1st Jan, 1970(long)
        Message to log into file if any error.
    """
    msg = 'TypeError(f"Value MisMatch: [{primary_key}] The value [{data}] is not equivalent in millisecond of given input datetime")'

    dt_string = input_value_list[idx][0]
    dt_string = dt_string[:19]
    format = "%Y-%m-%d %H:%M:%S"

    dt_object = datetime.strptime(dt_string, format)    # input datetime
    epoch_time = datetime(1970, 1, 1, 0, 0, 0)       # epoch datetime

    delta = (dt_object - epoch_time).total_seconds() * 1000
    val = int(delta)
    input_value_list.append([val])

    return val, msg 

def addMonth(validator, input_value_list, idx, cnt):
    """ Adds the noOfMonth taken from idx+1 of input_value_list to input value date.
    Args:
        validator(dict): metadata provided in avro schema
        input_value_list(list): all the input data 
        idx(int): from which input_field this function should be used
        cnt(int): number of input_fields after the index specified --> (no use)
    Returns:
        Date after adding months.
        Message to log into file if any error.
    """

    msg = "NotFoundError(primary_key, data, output_field)"

    dt_string = input_value_list[idx][0]
    format = "%Y-%m-%d %H:%M:%S"
    dt_string = dt_string[:19]
    dt_object = datetime.strptime(dt_string, format)    # input datetime

    noMonth = input_value_list[idx+1][0]
    depreciation_date = dt_object + relativedelta(months=noMonth)

    input_value_list.append([depreciation_date])

    return depreciation_date, msg


def idValueMatch(data, schema):
    # for accessing the validator
    global j

    # maps the index of differnet input file for current output record 
    if(j == 0):
        validator = output_schema["fields"][j]["validator"]
        mapFilesDictionary(validator)
        j += 1

    output_field = schema["logicalType"]
    dataType = schema["type"]

    #checking the dataType of current output field
    dataTypeCheck(data, dataType, output_field) 

    validator = output_schema["fields"][j]["validator"]
    aggregators = validator["aggregator"]
    primary_key = mapFiles_Index.get("primary_key")

    # if we want some of the output field not to be tested
    if aggregators[0][0] == "passing":
        j = j + 1
        return data

    if aggregators[0][0] == "directCompare":
        values = validator["compare_val"]
        msg = "NotFoundError(primary_key, data, output_field)"

        if data not in values:
            raise eval(msg)

        j = j + 1
        return data

    # storing all the required values from differnet files in a list
    input_value_list = inputList(validator)

    for aggregator in aggregators:
        val, msg = eval(aggregator[0])(validator, input_value_list, aggregator[1]-1, aggregator[2])

        if aggregator[0] == "isNull" and val != "moreValidation":
            break

        if(validator.get("intermediate_cmp_with")):
            val_cmp_with = validator["intermediate_cmp_with"][aggregator[1]-1]
            if not val_cmp_with == "" and not val:
                val = val_cmp_with
                break

    if validator.get("output_cmp_with"):
        output_cmp_with = validator["output_cmp_with"]
        val = output_cmp_with[0] if(val) else output_cmp_with[1]

    if(dataType == "double" or dataType == "float"):
        val = round(val, 2)
        if not math.isclose(val, data, rel_tol=1e-2):
            logger.warning(eval(msg))
    elif(val != data):
        logger.warning(eval(msg))

    j = j + 1
    return data

def decode(data, *args):
    return data

fastavro.write.LOGICAL_WRITERS["string-system_id"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-system_id"] = decode

fastavro.write.LOGICAL_WRITERS["string-customer_id"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-customer_id"] = decode

fastavro.write.LOGICAL_WRITERS["string-name"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-name"] = decode

fastavro.write.LOGICAL_WRITERS["string-currency"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-currency"] = decode

fastavro.write.LOGICAL_WRITERS["string-city"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-city"] = decode

fastavro.write.LOGICAL_WRITERS["string-state"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-state"] = decode

fastavro.write.LOGICAL_WRITERS["string-country"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-country"] = decode

fastavro.write.LOGICAL_WRITERS["int-months_to_depreciate"] = idValueMatch
fastavro.read.LOGICAL_READERS["int-months_to_depreciate"] = decode

fastavro.write.LOGICAL_WRITERS["string-depreciation_start_date"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-depreciation_start_date"] = decode

fastavro.write.LOGICAL_WRITERS["boolean-is_depreciated"] = idValueMatch
fastavro.read.LOGICAL_READERS["boolean-is_depreciated"] = decode

fastavro.write.LOGICAL_WRITERS["int-postal_code"] = idValueMatch
fastavro.read.LOGICAL_READERS["int-postal_code"] = decode

fastavro.write.LOGICAL_WRITERS["string-loc_lattitude"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-loc_lattitude"] = decode

fastavro.write.LOGICAL_WRITERS["string-loc_longitude"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-loc_longitude"] = decode

fastavro.write.LOGICAL_WRITERS["string-device_type"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-device_type"] = decode

fastavro.write.LOGICAL_WRITERS["double-capacity_used"] = idValueMatch
fastavro.read.LOGICAL_READERS["double-capacity_used"] = decode

fastavro.write.LOGICAL_WRITERS["double-capacity_total"] = idValueMatch
fastavro.read.LOGICAL_READERS["double-capacity_total"] = decode

fastavro.write.LOGICAL_WRITERS["double-capacity_free"] = idValueMatch
fastavro.read.LOGICAL_READERS["double-capacity_free"] = decode

fastavro.write.LOGICAL_WRITERS["string-domain_name"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-domain_name"] = decode

fastavro.write.LOGICAL_WRITERS["string-current_version"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-current_version"] = decode

fastavro.write.LOGICAL_WRITERS["double-cost"] = idValueMatch
fastavro.read.LOGICAL_READERS["double-cost"] = decode

fastavro.write.LOGICAL_WRITERS["double-cost_per_gb"] = idValueMatch
fastavro.read.LOGICAL_READERS["double-cost_per_gb"] = decode

fastavro.write.LOGICAL_WRITERS["long-update_time"] = idValueMatch
fastavro.read.LOGICAL_READERS["long-update_time"] = decode

fastavro.write.LOGICAL_WRITERS["string-partitionKey"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-partitionKey"] = decode

fastavro.write.LOGICAL_WRITERS["string-collection_id"] = idValueMatch
fastavro.read.LOGICAL_READERS["string-collection_id"] = decode

# loading avro schema
with open("output.avsc", "r") as f:
    output_sc = f.read()

output_schema = fastavro.parse_schema(json.loads(output_sc))

# reading json data
filename = "data/output.json"
output_json = readFile(filename)

#creating an avro file
with open("output.avro", "wb") as f:
    pass

#writing into avro file
for record in output_json:
    try:
        j = 0 #for acessing validator from each field in current json
        writer(open("output.avro", "wb"), output_schema, [record]) 
    except (ValueError, TypeError, NotFoundError, MapError, KeyError, ZeroDivisionError, IOError) as err:
        logger.error(err)
    i = i + 1

# reading from avro file
# for user in reader(open("output.avro", "rb")):
#     logger.info("The record %s is correct", user)