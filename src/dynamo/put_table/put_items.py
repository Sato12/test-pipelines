import boto3
import sys
import time
import traceback
import json

if len(sys.argv) not in [4, 5]:
    print("Usage: python3 putItems.py tbl_name path_items pk sk")
    sys.exit(1)

dynamodb = boto3.client('dynamodb', region_name='us-east-1')
table_name = sys.argv[1]
pathItems = sys.argv[2]
pk = sys.argv[3]
sk = sys.argv[4] if len(sys.argv) == 5 else None

f = open(pathItems)
items = json.load(f)

print(f"Put items for {table_name} from -> '{pathItems}'")

def mapPkSk(item):
    itemPk = item[pk]["S"]
    itemSk = item[sk]["S"] if sk is not None else str(sk)
    return f"PK->'{itemPk}' SK->'{itemSk}'"

def compareItems(item1, item2):
    if set(item1.keys()) != set(item2.keys()):
        return False

    for key in item1.keys():
        val1 = item1[key]
        val2 = item2[key]

        if isinstance(val1, dict) and isinstance(val2, dict):
            if not compareItems(val1, val2):
                return False
        else:
            if val1 != val2:
                return False

    return True

actualItems = dynamodb.scan(TableName=table_name)['Items']
remainingItemsPksSks = list(map(lambda x: mapPkSk(x), actualItems))

uploaded = 0
toUpdate = 0
toRemove = 0
for item in items:
    try:
        itemPkSk = mapPkSk(item)
        isUpdatedItem = True
        if itemPkSk in remainingItemsPksSks:
            remainingItemsPksSks.remove(itemPkSk)
            fullActualItem = list(filter(lambda x: x[pk]["S"] == item[pk]["S"] and (sk is None or ( x[sk]["S"] ==item[sk]["S"])), actualItems))[0]
            isUpdatedItem = not compareItems(fullActualItem, item)
        print(f"Update -> {isUpdatedItem} -- {itemPkSk}")
        
        if isUpdatedItem:
            toUpdate += 1
            response = dynamodb.put_item(
                TableName=table_name,
                Item=item
            )
            uploaded += 1
            time.sleep(0.5)
    except:
        print("Invalid Put Item", item)
        traceback.print_exc()
    

toRemove = len(remainingItemsPksSks)
for item in remainingItemsPksSks:
    itemSplit = item.split("'")
    key = {}
    key[pk] = {"S": itemSplit[1]}
    if sk is not None: key[sk] = {"S": itemSplit[3]}
    print("This key will be removed ->", key)
    dynamodb.delete_item(TableName=table_name, Key=key)
    time.sleep(0.5)

print(f"Process completed.")
print(f"Total Items -> {len(items)}")
print(f"Items to Update -> {toUpdate}")
print(f"Items Uploaded -> {uploaded}")
print(f"Items Deleted -> {toRemove}")

if toUpdate != uploaded:
    raise Exception("There was some error on Uploading items")
