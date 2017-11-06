
#	For DJ: python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\CheapSQL-master\server.py"



from flask import Flask,render_template,url_for,redirect,jsonify,request

import sys
import shlex

#	Change the long DIR to your local setting
# sys.path.append('D:/Djinn/Midgard/Geffen/Masters/CMSC 227/Project Code/CheapSQL-master/scripts')
#import SQLValidator as parser
import CheapSQLlib as SQL


app = Flask(__name__)

#	Global variables
#	Column position definitions
#	alias,loc,col_name,data_type,length,mask,isNullable,defaultValue,isUnique
_SCHEMA_ALIAS			= 0
_SCHEMA_TABLE           = 1
_SCHEMA_COLUMN          = 2
_SCHEMA_DATA_TYPE       = 3
_SCHEMA_LENGTH          = 4
_SCHEMA_MASK            = 5
_SCHEMA_ISNULLABLE      = 6
_SCHEMA_DEFAULTVALUE    = 7
_SCHEMA_ISUNIQUE        = 8

_TABLE_ALIAS			= 0
_TABLE_TABLE			= 1
_TABLE_SCHEMA			= 2



@app.route('/')




def index():
	
	return render_template("index.html")

@app.route('/query',methods=['GET','POST'])
def query():
	
	errorFlag = False
	resultFlag = False
	error = ""
	resp = {}
	
	query = request.form.get("query")
	
	isValidQuery,checkType,outputList,error = SQL.isValidSQL(query)
	
	# If valid SQL, proceed with the query
	
	if isValidQuery:
	#if True:
		
		if checkType == "INSERT":
			# Execute INSERT
			resultFlag,error = SQL.executeInsert(outputList[0],outputList[1])
			
			if resultFlag:
				resp["data"] = ["",""]
				resp["numrows"] = 1
				resp["valid_query"] = "True"
			else:
				errorFlag = True
			
		elif checkType == "SELECT":
			# Execute SELECT
			selected_columns = list()
			result_bulk = list()	#list of BPlusTree()
			resultFlag,selected_columns,result_bulk,error = SQL.executeSelect(outputList[0],outputList[1],outputList[2])
			
			if resultFlag:
			
				resp["query"] = request.form.get("query")
				
				columns_list = list()
				for count in range(0,len(selected_columns)):
					columns_list.append(selected_columns[count][_SCHEMA_COLUMN])
					
				resp["columns"] = columns_list
					
				data = []
				
				for bulk_count in range(0,len(result_bulk)):
					row = list()
					for col_count in range(0,len(columns_list)):
						brane_key = selected_columns[col_count][_SCHEMA_ALIAS].upper() + "." + selected_columns[col_count][_SCHEMA_COLUMN].upper()
						row.append(result_bulk[bulk_count][brane_key])
					data.append(row)
				
				resp["data"] = data
				resp["numrows"] = len(result_bulk)
				resp["valid_query"] = "True"
			else:
				errorFlag = True
			
		else:
			print "[server] [ERROR] Unrecognized query type"
			error += "\n[ERROR isValidSQL] Unexpected query termination"
			errorFlag = True

	# If not, return the error.
	else:
		errorFlag = True
		print "[server] [ERROR] Invalid Query"
	
	if errorFlag:
		print "[server] Invalid Query"
		error = error.replace("\n","<br />")
		resp["error"] = error
	return jsonify(resp)


if __name__ == '__main__':
	print "\t---------------------------------------"
	print "\tStarting CheapSQL Server v0.0.1"
	print "\t(c) 20017"
	print "\t---------------------------------------"
	
	print "[main] "
	
	app.run(debug=True,host='0.0.0.0')