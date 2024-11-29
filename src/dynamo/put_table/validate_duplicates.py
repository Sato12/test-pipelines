import sys
import json

if len(sys.argv) not in [3, 4]:
    print("Usage: python3 ValidateDuplications.py path_items pk sk")
    sys.exit(1)

pathItems = sys.argv[1]
pk = sys.argv[2]
sk = sys.argv[3] if len(sys.argv) == 4 else None

f = open("../../../../" + pathItems)
items = json.load(f)

print(f"Validating items for pk -> {pk}, sk -> {sk}, from -> '{pathItems}'")

repeatedPksSks = []
seenPksSks = []

for item in items:
    itemPk = item[pk]["S"]
    itemSk = item[sk]["S"] if sk is not None else str(sk)
    itemPkSk = f"PK->'{itemPk}' SK->'{itemSk}'"
    if itemPkSk in (seenPksSks + repeatedPksSks):
        repeatedPksSks.append(itemPkSk)
        print("Repeated -->", itemPkSk)
    else:
        seenPksSks.append(itemPkSk)
        
if len(repeatedPksSks) > 0:
    raise Exception(f"There are Repeated Keys on {pathItems}")

print(f"Process completed.")
print(f"Total Items -> {len(items)}")
