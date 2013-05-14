from subprocess import call

import json
import os
import pprint

def getErr(request):
  if request["Status"]:
    if request["Status"].has_key("Err"):
      return request["Status"]["Err"]
  return None

def getTime(request):
  return request["StartTime"]

def getRequestDiag(request):
  diag =  "\t";
  diag += "\"" + request["SourceAddress"] + "\""
  diag += " -> "
  diag += "\"" + request["DestinationAddress"] + "\""
  diag += "[label = \"" + request["ServiceMethod"]
  err = getErr(request)
  if err:
    diag += "(Err=" + str(err) + ")"
  diag += "\""
  if err:
    diag += ", color = red, failed"
  diag += "]"
  diag += "[rightnote = \"" + pprint.pformat(request["Args"]) + "\"]" 
  diag += ";\n"
  return diag

def getResponseDiag(request):
  diag = ""
  if not getErr(request):
    diag += "\t";
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
    if filename.split(".")[-1] == "json":
      # get the json data
      json_data=open(args[0] + filename)
      requests.extend(json.load(json_data))

  requests = sorted(requests, key=getTime)

  # write to file
  diag_file = open("log.diag", 'w')
  diag_file.write("seqdiag {\n")
  diag_file.write("\tedge_length = 300;")
  diag_file.write("\tdefault_fontsize = 14;") 
  diag_file.write("\tdefault_note_color = lightblue;")
  diag_file.write("\tactivation = none;")

  for request in requests:
    diag_file.write(getRequestDiag(request))
    diag_file.write(getResponseDiag(request))
    diag_file.write("\n")

  diag_file.write("}\n")

  diag_file.close()

  call(["seqdiag", "-f", "ProximaNova-Semibold.otf", "log.diag"])


if __name__ == '__main__':
  import sys
  main(sys.argv[1:])
