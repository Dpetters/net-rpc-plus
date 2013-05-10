import json
import pprint
import sys


def getRequestDiag(request):
  diag =  "\t";
  diag += "\"" + request["SourceAddress"] + "\""
  diag += " -> "
  diag += "\"" + request["DestinationAddress"] + "\""
  diag += "[label = \"" + request["ServiceMethod"]  + "\"]"
  diag += "[rightnote = \"" + pprint.pformat(request["Args"]) + "\"]" 
  diag += ";\n"
  return diag

def getResponseDiag(request):
  diag = "\t";
  diag += "\"" + request["SourceAddress"] + "\""
  diag += " <- "
  diag += "\"" + request["DestinationAddress"] + "\""
  diag += "[leftnote = \"" + pprint.pformat(request["Reply"]) + "\"]" 
  diag += ";\n"
  return diag

# get the json data
json_data=open(sys.argv[1])
data = json.load(json_data)

# write to file
diag_file = open("log.diag", 'w')
diag_file.write("seqdiag {\n")

for request in data:
  diag_file.write(getRequestDiag(request))
  diag_file.write(getResponseDiag(request))
  diag_file.write("\n")

diag_file.write("}\n")
