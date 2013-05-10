from subprocess import call

import json
import os
import pprint


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

def main(args):
  logFiles = []
  for (dirpath, dirname, filenames) in os.walk(args[0]):
    logFiles.extend(filenames)
    break

  requests = []

  for filename in logFiles:
    # get the json data
    json_data=open(args[0] + filename)
    requests.extend(json.load(json_data))
   
  requests = sorted(requests, key=lambda x: x["StartTime"])

  # write to file
  diag_file = open("log.diag", 'w')
  diag_file.write("seqdiag {\n")

  for request in requests:
    diag_file.write(getRequestDiag(request))
    diag_file.write(getResponseDiag(request))
    diag_file.write("\n")

  diag_file.write("}\n")

  diag_file.close()

  call(["seqdiag", "log.diag"])


if __name__ == '__main__':
  import sys
  main(sys.argv[1:])
