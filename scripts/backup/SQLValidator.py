
#	------------------------------------------------------
#python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\CheapSQL-master\SQLValidator.py"
#python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\CheapSQL-master\validatesql_backup.py"
#	------------------------------------------------------

import shlex
import sys
import re
from pythonds.basic.stack import Stack
from pythonds.trees.binaryTree import BinaryTree
import btree
import cPickle as pickle

#	------------------------------------------------------
#	Global variables
# _install_dir = "\\"
#_install_dir = "D:\\My Documents\\AY 2017-2018 1S\\CMSC 227\\CheapSQL\\"
_install_dir = "D:\\Djinn\\Midgard\\Geffen\\Masters\\CMSC 227\\Project Code\\CheapSQL-master\\"
_schema_ext = ".csf"
_row_delimiter = ";"
_rowfile_extension = ".crf"
_bt_order = 1024

#	------------------------------------------------------

def newSplit(value):
	lexer = shlex.shlex(value)
	lexer.quotes += '"'
	lexer.whitespace_split = True
	lexer.commenters = ''
	lexer.wordchars += '\''
	return list(lexer)

def newSplit2(value):
	lexer = shlex.shlex(value,posix=True)
	lexer.quotes = '"'
	lexer.wordchars += '\''
	return list(lexer)
	
#	------------------------------------------------------

def loadTables(loc):
	global error
	return_list = list()
	if loc == "ALL_TABLES":
		
		return_list.append(("STUDENT","STUDENT","STUDENT_SCHEMA"))
		return_list.append(("STUDENTHISTORY","STUDENTHISTORY","STUDENTHISTORY_SCHEMA"))
		return_list.append(("COURSE", "COURSE", "COURSE_SCHEMA"))
		return_list.append(("COURSEOFFERING","COURSEOFFERING","COURSEOFFERING_SCHEMA"))
		return_list.append(("STUDCOURSE","STUDCOURSE","STUDCOURSE_SCHEMA"))
		
	elif loc == "STUDENT":
		return_list.append(("STUDENT","STUDENT","STUDENT_SCHEMA"))
	elif loc == "STUDENTHISTORY":
		return_list.append(("STUDENTHISTORY","STUDENTHISTORY","STUDENTHISTORY_SCHEMA"))
	elif loc == "COURSE":
		return_list.append(("COURSE", "COURSE", "COURSE_SCHEMA"))
	elif loc == "COURSEOFFERING":
		return_list.append(("COURSEOFFERING","COURSEOFFERING","COURSEOFFERING_SCHEMA"))
	elif loc == "STUDCOURSE":
		return_list.append(("STUDCOURSE","STUDCOURSE","STUDCOURSE_SCHEMA"))
	return return_list

#	------------------------------------------------------

def loadSchema(loc, withFlags, return_list):
	global error
	read_flag = False
	if loc == "ALL_TABLES":
		
		if not loadSchema("STUDENT",withFlags,return_list):
			error +=   "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("STUDENTHISTORY",withFlags,return_list):
			error +=   "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False		
		if not loadSchema("COURSE",withFlags,return_list):
			error +=   "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("COURSEOFFERING",withFlags,return_list):
			error +=   "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("STUDCOURSE",withFlags,return_list):
			error +=   "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False		
	else:
		loc_file = loc + _schema_ext
		#schema_file  = open(_install_dir + "scripts\\schema\\" + loc_file,"r")
		#lexer_list = shlex.split(schema_file.read())
		
		#error +=   "\nRECURSE: " + loc
		
		with open(_install_dir + "scripts\\schema\\" + loc_file) as schema_file:
			file_lines = schema_file.readlines()
		
		#	Raw column information (one row)
		for i in range(0, len(file_lines) ):	
			read_flag = True
			state = 0
			col_name = ""
			data_type = ""
			length = 0	
			mask = ""
			isNullable = False
			
			column_info = file_lines[i].upper()
			#error +=   "\n COLUMN_INFO : "
			#error +=   column_info
			if column_info[0] != "#":
			
				column_info_list = shlex.split(column_info)
			
				#	Parse column information
				for j in range(0, len(column_info_list) ):
					
					if state == 0:
						col_name = column_info_list[j]
						state = 1
					elif state in [1,3,5,7]:
						state += 1
					elif state == 2:
						data_type = column_info_list[j].upper()
						state += 1
					elif state == 4:
						if column_info_list[j] == "*":
							length = 0
						else:
							length = int(column_info_list[j])
						state += 1
					elif state == 6:
						mask = column_info_list[j]
						state += 1
					elif state == 8:
						if column_info_list[j].upper == "FALSE":
							isNullable = False
						elif column_info_list[j].upper == "TRUE":
							isNullable = True
						state += 1
				if state != 9:
					error +=   "\n[ERROR] Corrupt schema file : " + _install_dir + "scripts\\schema\\" + loc_file
					error +=   "\n        Line : " + str(i)
					return False
					
				if withFlags:
					return_list.append((loc,col_name,data_type,length,mask,isNullable,False))
				else:
					return_list.append((loc,col_name,data_type,length,mask,isNullable))
		if read_flag == False:
			error +=   "\n[ERROR loadSchema] Possible corrupt schema file : " + _install_dir + "scripts\\schema\\" + loc_file
			return False
		else:
			return True
		#error +=   "\n RETURN LIST : "
		#error +=   return_list
		#error +=   "---------------------------------"

#	------------------------------------------------------
	
def isValidAlias(candidate, selected_tables):
	#check for special characters and shit
	global error
	for table in selected_tables:
		if candidate.upper() == table[0].upper():
			error +=   "\n[ERROR isValidAlias] Duplicate alias : " + candidate
			return False
	return True

#	------------------------------------------------------

def isValidDate(date_string,mask_string):
	global error
	re_mask_string = ""
	if mask_string == "YYYY-MM-DD":
		re_mask_string = "[0-9]{4}-(0[1-9]|1[1-2])-(0[1-9]|[1-2][0-9]|[3][0-1])"
	
	mask_checker = re.compile(re_mask_string)
	if mask_checker.match(str(date_string)) is None:
		error +=   "\n[ERROR - isValidDate] Invalid date"
		return False
	else:
		return True
		
#	------------------------------------------------------

def isValidTerm(someTerm):
	global error
	return True

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidTables(tables_string, selected_tables):
	global error
	#load tables
	table_list = loadTables("ALL_TABLES")
	
	#----------------------------------------------------

	isValid = False
	state = 0
	
	lexer = shlex.shlex(tables_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	print "\n [isValidTables]\n"
	print lexer_list
	
	for i in range(0, len(lexer_list) ):
	#error +=   lexer_list[i]
		
		#	States:
		#	0	Initial (checks)
		#	1	AS
		#	2	Check for alias validity
		#	3	Check for comma ( -> 0)
		
		if state == 0:
			
			table_alias = lexer_list[i].upper()
			result = next((i for i, v in enumerate(table_list) if v[0].upper() == table_alias), -1)
			
			if result != -1 :
				selected_tables.append(table_list[result])
				state = 1
			else:
				error +=   "\n[ERROR] Expected a valid target table. Read the goddamn schema man, what the hell is " + str(lexer_list[i].upper()) + "?"
				return False
		
		#Check for comma or AS
		elif state == 1:
			if lexer_list[i] == ",":
				
				state = 0
			elif lexer_list[i].upper() == "AS":
				state = 2
			else:
				error +=   "\n[ERROR] Expected ',' or AS keyword, but instead we got this shit : " + str(lexer_list[i].upper())
				return False
		
		#Check for alias validity
		elif state == 2:
			candidate_alias = lexer_list[i].upper()
			#check if alias is an existing alias or table name
			if isValidAlias(candidate_alias, selected_tables) :
			
				temp_tuple = selected_tables[-1]
				
				temp_t0 = candidate_alias
				temp_t1 = temp_tuple[1]
				temp_t2 = temp_tuple[2]

				temp_tuple = (temp_t0,temp_t1,temp_t2)

				selected_tables[-1] = temp_tuple
				
				state = 3
			else :
				return False
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else :
				error +=   "\n[ERROR] Expected ','"
				return False
		else :
			error +=   "\n[ERROR] Unknown state : " + str(state)
			return False
	
	if (state == 3) or (state == 1) :
		return True
	
	return False

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidColumns(columns_string, selected_tables, selected_columns):
	global error
	#load tables
	column_list = list()
	
	#	(alias, table, schema_loc)
	
	for t1 in selected_tables:
		loadSchema(t1[1], False, column_list)
	
	#----------------------------------------------------
	
	isValid = False
	state = 0
	
	lexer = shlex.shlex(columns_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	#lexer_list = shlex.split(columns_string)
	
	
	#print "\n[isValidColumns][lexer_list]:"
	#print lexer_list
	#print "\n[isValidColumns][selected_tables]:"
	#print selected_tables
	
	#	State
	#	0	Initial, check for alias OR *
	#	1	Check for .
	#	2	Check for column
	#	3	Check for ,
	#	4	Final state if * 
	
	for i in range(0, len(lexer_list) ):
	#error +=   lexer_list[i]
	
		if state == 0:
			if lexer_list[i] == "*":
				state = 4
			else:
				table_alias = lexer_list[i].upper()
				result = next((i for i, v in enumerate(selected_tables) if v[0].upper() == table_alias), -1)
				
				if result != -1 :
					source_table_index = result
					state = 1
				else:
					error +=   "\n[ERROR] Expected a valid target table name/alias. ["+table_alias+"]"
					return False
		elif state == 1:
			if lexer_list[i] == ".":
				state = 2
			else:
				error +=   "\n[ERROR] Expected '.' ... it's not that hard man."
				return False
		elif state == 2:
			column_name = lexer_list[i].upper()
			result = next((i for i, v in enumerate(column_list) if ((v[0].upper() == selected_tables[source_table_index][1]) and (v[1].upper() == column_name))), -1)
			if result != -1 :
				selected_columns.append(column_list[result])
				state = 3
			else:
				error +=   "\n[ERROR] Expected a valid column name : " + str(column_name)
				return False
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else:
				error +=   "\n[ERROR] Expected ','"
				return False
		elif state == 4:
			error +=   "\n[ERROR isValidColumns] Expected keyword: FROM"
			return False
		else:
			error +=   "\n[ERROR] Unknown state : " + str(state)
			
	if state in [3,4] :
		return True
	else:
		return False

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidConditions(conditions_string, selected_tables):
	global error
	#load tables
	column_list = list()
	loadSchema("ALL_TABLES", False, column_list)
	
	#----------------------------------------------------
	global error
	
	isValid = False
	state = 0
	pStack = Stack()
	eTree = BinaryTree("!")
	pStack.push(eTree)
	currentTree = eTree
	ctr = 0
	operatorList = ["=", "AND", "OR"]
	
	lexer = shlex.shlex(conditions_string, posix=True)
	#lexer.whitespace += ';'
	lexer_list = list(lexer)
	
	print "\n[isValidConditions][lexer_list]:"
	print lexer_list
	#print "\n[isValidColumns][selected_tables]:"
	#print selected_tables
	
	#lexer_list = shlex.split(conditions_string)
	
	for i in range(0, len(lexer_list) ):
		ctr += 1
		#-----------
		
		term = lexer_list[i].upper()
		
		if term == "(":
			currentTree.insertLeft("")
			pStack.push(currentTree)
			currentTree = currentTree.getLeftChild()
		elif (term not in operatorList) and (term not in ["(",")",";"]):
		
			# Check if term is a valid column or value
			if state == 0:
				result = next((i for i, v in enumerate(selected_tables) if v[0].upper() == term), -1)
				if result != -1 :
					source_table_index = result
					state = 1
				else:
					if isValidTerm(term) :
						rootVal = term
						state = 99
					else:
						error +=   "\n[ERROR] Expected a valid number or column name"
						return False
			elif state == 1:
				if term == ".":
					state = 2
				else:
					error +=   "\n[ERROR] Expected '.'"
					return False
			elif state == 2:
				result = next((i for i, v in enumerate(column_list) if ((v[0].upper() == selected_tables[source_table_index][1]) and (v[1].upper() == term))), -1)
				
				if result != -1 :
					rootVal = column_list[result][0] + "." + column_list[result][1]
					state = 99
				else:
					error +=   "\n[ERROR] Expected a valid column name : " + str(term)
					return False
			if state == 99:
				if not pStack.isEmpty():
					state = 0
					currentTree.setRootVal(rootVal)
					parent = pStack.pop()
					currentTree = parent
				else:
					error +=   "\n[ERROR] Unbalanced operations"
					return False
			
		elif term in operatorList:
			currentTree.setRootVal(term)
			currentTree.insertRight("")
			pStack.push(currentTree)
			currentTree = currentTree.getRightChild()
		elif term == ")":
			if not pStack.isEmpty():
				currentTree = pStack.pop()
			else:
				error +=   "\n[ERROR] Unbalanced operations"
				return False
		#elif term == ";":
		#	i = len(lexer_list) + 1
		else:
			raise ValueError
	
	if ctr > 0:
		if not pStack.isEmpty():
			error += "\n[ERROR] Unbalanced grouping"
			return False
		else:
			return True
	else:
		return True
#	------------------------------------------------------
#	INSERT Checking
def isValidSchemaString(schema_string,target_columns):
	global error
	#load tables
	table_list = loadTables("ALL_TABLES")
	#target_schema = loadSchema("ALL_TABLES", True)
	#----------------------------------------------------

	isValid = False
	state = 0
	
	lexer = shlex.shlex(schema_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	#print "\n [isValidSchemaString][lexer_list] : "
	#print lexer_list
	#lexer_list = shlex.split(schema_string)
	ctr = 0
	
	for i in range(0, len(lexer_list) ):
	
		term = lexer_list[i].upper()
		if state == 0:
			result = next((i for i, v in enumerate(table_list) if v[0].upper() == term), -1)
			if result != -1 :
				target_schema = list()
				loadSchema(term, True, target_schema)
				target_table = term
				state = 1
			else:
				error +=   "\n[ERROR] Invalid table name : " + term
				return False
		#begin parenthesis
		elif state == 1:
			if term == "(":
				state = 2
			else:
				error +=   "\n[ERROR] Expected '('"
				return False
		#column name
		elif state == 2:
			result = next((i for i, v in enumerate(target_schema) if (v[0].upper() == target_table) and (v[1].upper() == term)), -1)
			if result != -1 :
				if target_schema[result][2] == True:
					error +=   "\n[ERROR] Duplicate column : " + term
					return False
				else:
					#flag the column as selected
					#error +=   "\nRESULT : " + str(result)
					temp_t0 = target_schema[result][0]
					temp_t1 = target_schema[result][1]
					temp_t2 = target_schema[result][2]
					temp_t3 = target_schema[result][3]
					temp_t4 = target_schema[result][4]
					temp_t5 = True
					target_schema[result] = (temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,temp_t5)
					#error +=   "\n -----"
					#error +=   target_schema
					#add the column into target_columns
					
					target_columns.append((temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,ctr))
					
					ctr += 1
					
					state = 3
			else:
				error +=   "\n[ERROR] Invalid column : " + term
				return False
		#comma
		#end parenthesis
		elif state == 3:
			if term == ",":
				state = 2
			elif term == ")":
				isValid = True
			else:
				error +=   "\n[ERROR] Expected ')' [" + term + "]"
				return False

		
		else:
			return False
					
		
	
	return isValid
	
#	------------------------------------------------------
#	INSERT Checking
def isValidValuesString(values_string,target_columns,values_list):
	global error
	
	lexer_list = newSplit2(values_string)
	print "\n[isValidValuesString][lexer_list][newSplit2] : "
	print lexer_list
	
	state = 0
	column_count = 0
	counter = 0
	tempDate = ""
	tempString = ""
	valid_value = ""
	
	#	States
	#	0: Checks for (
	#	1: Checks for the data
	#	2: Checks for , or )
	
	for i in range(0, len(lexer_list) ):
	
		if state == 0:
			if lexer_list[i] == "(":
				state = 1
			
		elif state == 1:
			term = lexer_list[i]
			#check with the datatype
			current_column = target_columns[column_count]
			#	0	Table name
			#	1	Column name
			#	2	Data type
			#	3	Length
			#	4	Mask
			#	5	IsNullable
			
			#----STRING
			if current_column[2] == "STRING":
				
				if (term != ",") and (term != ")"):
					tempString += term
					
					#	Length check
					if len(tempString) > current_column[3]:
						error +=   "\n[ERROR] Length exceeded " + str(current_column[3])
						return False
					elif (len(tempString) > 0) and (tempString.upper() != "NULL"):
						
						if current_column[4] != "*":
							mask_checker = re.compile(current_column[4])
							if mask_checker.match(str(tempString)) is None:
								error +=   "\n[ERROR] Invalid value; did not match input mask : " + str(current_column[4]) + " : " + str(tempString)
								return False
							else:
								# VALID!!!!!!! [with Mask]
								valid_value = tempString								
						else:
							# VALID!!!!!!! [without Mask]
							valid_value = tempString
					else:
						if not current_column[5]:
							error +=   "\n[ERROR] Column " + current_column[0] + "is not nullable"
							return False
						# VALID!!!!!!!
						valid_value = tempString
						print "\n yy: " + str(valid_value)
						if term == ",":
							state = 1
							valid_value = tempString
							tempString = ""
						elif term == ")":
							state = 3
							valid_value = tempString
			
			#----DATE
			elif current_column[2] == "DATE":
				
				if (term != ",") and (term != ")"):
					tempDate += term

					# Perform the actual checking
					#	Length check
					if len(tempDate) > current_column[3]:
						error +=   "\n[ERROR] Length exceeded " + str(current_column[3])
						return False
					elif len(tempDate) > 0:
						if current_column[4] != "*":
							mask_string = current_column[4]
							current_column[4]
							if not isValidDate(tempDate,mask_string):
								error +=   "\n[ERROR] Unsupported date format: " + str(tempDate)
								return False
							else:
								# VALID!!!!!!!
								valid_value = tempDate
					else:
						if not current_column[5]:
							error +=   "\n[ERROR] Column " + current_column[0] + "is not nullable"
							return False
						# VALID!!!!!!!
						valid_value = tempDate						
						if term == ",":
							state = 1
							valid_value = tempDate
							#values_list.append(valid_value)
							tempDate = ""
						elif term == ")":
							state = 3
							
						

			#----INT
			elif current_column[2] == "INTEGER":
				if (not term.isdigit()) and (term.upper() != "NULL") and (term != ""):
					error +=   "\n[ERROR] Type mismatch. Expected " + current_column[2]
					return False
				else:
					valid_value = term
					#values_list.append(valid_value)
			column_count += 1
			
			
			state = 2
		elif state == 2:
			values_list.append(valid_value)
			if lexer_list[i] == ",":
				state = 1 
				tempDate = ""
				tempString = ""
				valid_value = ""
			elif lexer_list[i] == ")":
				state = 3
	
	if state == 3:
		if column_count != len(target_columns):
			error += "\n[ERROR][isValidValuesString] Value count and target column count mismatch : ("+str(column_count)+"/"+str(len(target_columns))+")"
		else:
			return True

	else:
		error += "\n[ERROR][isValidValuesString]["+str(state)+"] Expected ')'"
		return False

#	------------------------------------------------------
#	INSERT Execute

def executeInsert(target_columns,values_list):
	global error
	
	target_schema = list()
	loadSchema(target_columns[0][0], True, target_schema)
	
	index_bt = list()
	
	outputString = ""
	row_filename = ""
	
	delimitFlag = False
	print values_list
	
	#	Generate the output string and filename
	for i in range(0, len(target_schema) ):
		
		new_bt = btree.BPlusTree(_bt_order)
		
		new_bt = pickle.load(open(_install_dir + "scripts\\indexes\\" + str(target_schema[i][0]).lower() + "_" + str(target_schema[i][1]).upper() + "_index_bt.pkl", 'rb'))
		#print "\n[executeInsert] Load new_bt SUCCESS:" + "scripts\\indexes\\" + str(target_columns[i][0]).lower() + "_" + str(target_columns[i][1]).upper() + "_index_bt.pkl"
		
		index_bt.append(new_bt)
		#print "\n[executeInsert] Append to index_bt list SUCCESS"
		
		for j in range(0, len(target_columns)):
			#print "\n Compare: " + str(target_schema[i][1]) + " / " + str(target_columns[j][1])
			if target_schema[i][1] == target_columns[j][1]:
				print str(i) + "/" + str(j)
				
				if i == 0:
					row_filename = _install_dir + "scripts\\data\\" + str(target_schema[0][0]) + "_" + str(values_list[j]) + _rowfile_extension
				
				if delimitFlag == False:
					outputString = str(values_list[j])
					delimitFlag = True
				else:
					outputString += _row_delimiter + str(values_list[j])
				
				#	Add a new index bt node with a blank filename_list
				if index_bt[i][values_list[j]] is None:
					filename_list = list()
					filename_list.append(row_filename)
					index_bt[i].insert(str(values_list[j]), filename_list)
					print "\n SOMECHECK: " + str(values_list[j]) + "//" + str(values_list[i])
					print index_bt[i][values_list[j]]
				else:
					print "\n[executeInsert] Existing index bt node found"
					print values_list[j]
					print index_bt[i][values_list[j]]
					
					
				pickle.dump(index_bt[i], open(_install_dir + "scripts\\indexes\\" + str(target_schema[i][0]).lower() + "_" + str(target_schema[i][1]).upper() + "_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
				
				print "\n[executeInsert] Save new_bt SUCCESS:" + str(target_schema[i][0]).lower() + "_" + str(target_schema[i][1]).upper() + "_index_bt.pkl"
	print "\n Final output string:"
	print outputString
	
	f = open(row_filename, 'w')
	f.write(outputString)
			
		
#	------------------------------------------------------
#	QUERY Checking
def isValidSQL(input_sql):

	global error
	error = ""

	isValid = False
	state = 0
	warningFlag = 0
	columns_string = ""
	tables_string = ""
	conditions_string = ""
	schema_string = ""
	values_string = ""
	skipColumnCheck = False
	skipConditionsCheck = False
	
	#lexer = shlex.shlex(input_sql, posix=False)
	#lexer.whitespace += '\x00'
	#lexer_list = list(lexer)
	
	#lexer_list = shlex.split(input_sql.encode('utf8'))
	lexer_list = newSplit(input_sql.encode('utf8'))
	
	for x in lexer_list:
		x.rstrip('\x00')
		
	#print "\n [isValidSQL][input_sql] : \n"
	#print input_sql
	#print "\n [isValidSQL][lexer_list] : \n"
	#print lexer_list
	
	#----- Start statement selection -----
	
	if lexer_list[0].upper() == "SELECT":
		checkType = "SELECT"
		# start loop 
		for i in range(0, len(lexer_list) ):
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					error += "\n[ERROR isValidSQL] Unexpected query termination"
					return False,error
				elif lexer_list[i].upper() == "FROM":
					state = 1
					
				elif lexer_list[i].upper() != "SELECT":
					columns_string += " "
					columns_string += lexer_list[i]
				
			#	Tables
			elif state == 1:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() == "WHERE":
					state = 2
				elif lexer_list[i].upper() != "FROM":
					tables_string += " "
					tables_string += lexer_list[i]
					
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() != "WHERE":
					conditions_string += " "
					conditions_string += lexer_list[i]
			elif state == 3:
				if warningFlag == 0:
					error +=   "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				error +=   "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,error
			
		# end loop
		if state == 2:
			error +=   "\n [WARNING isValidSQL] Expected ';'"
			state = 3
			
	elif lexer_list[0].upper() == "DELETE":
		checkType = "DELETE"
		# start loop 
		for i in range(0, len(lexer_list) ):
			#error +=   lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					error +=   "\n[ERROR isValidSQL] Unexpected query termination"
					return False,error
				elif lexer_list[i].upper() == "FROM":
					state = 1
					print "change state ..."
					skipColumnCheck = True
				elif lexer_list[i].upper() != "DELETE":
					error +=   "\n[ERROR isValidSQL] Expected keyword : FROM, " + lexer_list[i].upper() 
					return False,error
			#	Tables
			elif state == 1:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() == "WHERE":
					state = 2
				elif lexer_list[i].upper() != "FROM":
					tables_string += " "
					tables_string += lexer_list[i]
				else:
					error +=   "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,error
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() != "WHERE":
					conditions_string += " "
					conditions_string += lexer_list[i]
				else:
					error +=   "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,error
			elif state == 3:
				if warningFlag > 0:
					error +=   "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				error +=   "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,error
		# end loop

		if state == 2:
			error +=   "\n [WARNING isValidSQL] Expected ';'"
			state = 3
			
	elif lexer_list[0].upper() == "INSERT":
		
		checkType = "INSERT"
		
		for i in range(0, len(lexer_list) ):
			#error += lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i].upper() == "INTO":
					state = 1
			#	Tables
			elif state == 1:
				if lexer_list[i] == ";":
					error +=   "\n[ERROR isValidSQL] Unexpected query termination"
					return False
				elif lexer_list[i].upper() == "VALUES":
					state = 2
				elif lexer_list[i].upper() != "INTO":
					schema_string += " "
					schema_string += lexer_list[i]
				else:
					error +=   "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,error
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				if lexer_list[i].upper() != "VALUES":
					values_string += " "
					values_string += lexer_list[i]
				else:
					error +=   "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,error
			else:
				error +=   "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,error
		# end loop
		
		if state == 2:
			#error +=   "\n [WARNING isValidSQL] Expected ';'"
			state = 3
	else:
		error += "\n[ERROR isValidSQL] Unknown command : " + str(lexer_list[0].upper())
		#error += "\n" + str(lexer_list

		
		return False,error
	#----- End statement selection -----
	
	if state != 3:
		error +=   "\n[ERROR isValidSQL] Unexpected query structure : " + str(state)
		return False,error
	
	values_list = list()
	if checkType == "INSERT":
		#Check INSERT
		target_columns = list()
		if isValidSchemaString(schema_string,target_columns):
			if isValidValuesString(values_string,target_columns,values_list):
				
				# Execute INSERT
				executeInsert(target_columns,values_list)
				
				return True,error
			else:
				#error +=   "\n[ERROR isValidSQL] isValidValuesString failed"
				#error +=   "\n values_string : "
				#error +=   "\n" + values_string
				return False,error
		else:
			#error +=   "\n[ERROR isValidSQL] isValidSchemaString failed"
			#error +=   "\n schema_string : "
			#error +=   "\n" + schema_string
			return False,error
	else:
		selected_tables = list()
		selected_cloumns = list()

		#Check SELECT
		#Check DELETE
		if isValidTables(tables_string, selected_tables):
			if skipColumnCheck or isValidColumns(columns_string, selected_tables, selected_cloumns):
				if skipConditionsCheck or isValidConditions(conditions_string, selected_tables):
					return True,error
				else:
					#error +=   "\n[ERROR isValidSQL] isValidConditions failed"
					#error +=   "\n conditions_string : "
					#error +=   "\n" + conditions_string
					return False,error
			else:
				#error +=   "\n[ERROR isValidSQL] isValidColumns failed"
				#error +=   "\n columns_string : "
				#error +=   "\n" + columns_string
				return False,error
		else:
			#error +=   "\n[ERROR isValidSQL ] isValidTables failed"
			#error +=   "\n tables_string : "
			#error +=   "\n" + tables_string
			return False,error
	return False,error
	
#	------------------------------------------------------
#	Start of Main Program


input_sql_file = open(_install_dir + "test_input.sql","r")
input_sql = input_sql_file.read()
global error

returnValue,error = isValidSQL(input_sql)

new_bt = btree.BPlusTree(_bt_order)

new_bt = pickle.load(open(_install_dir + "scripts\\indexes\\student_STUDNO_index_bt.pkl", 'rb'))
print "\n\n STUDNO"
print new_bt.items()

new_bt = pickle.load(open(_install_dir + "scripts\\indexes\\student_DEGREE_index_bt.pkl", 'rb'))
print "\n\n DEGREE"
print new_bt.items()

new_bt = pickle.load(open(_install_dir + "scripts\\indexes\\student_birthday_index_bt.pkl", 'rb'))
print "\n\n BIRTHDAY"
print new_bt.items()


#bt = btree.BPlusTree(1024)
#pickle.dump(bt, open(_install_dir + "course_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "courseoffering_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "studcourse_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_STUDNO_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_STUDENTNAME_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_BIRTHDAY_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_DEGREE_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_MAJOR_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "scripts\\indexes\\student_UNITSEARNED_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)


#pickle.dump(bt, open(_install_dir + "studenthistory_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
#pickle.dump(bt, open(_install_dir + "student_index_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

#sys.setrecursionlimit(1024 * 100)
#print sys.getrecursionlimit()
#
#bt = btree.BPlusTree(1024)
#loaded_bt = btree.BPlusTree(1024)

#l = range(3000000)

#for item in l:
#	bt.insert(item, str(item) + "RANDOM SHIET HAHAHAHAH")


#print "\n\n Test: bt.keys ::"
#print bt.keys
#
#
#print "\n\n Test: bt[key]"
#print bt[333]

#pickle.dump(bt, open(_install_dir + "sample_bt.pkl", 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

#print "\nStart loading"
#loaded_bt = pickle.load(open(_install_dir + "sample_bt.pkl", 'rb'))
#print "\nEnd loading"
#
#print "\n\n Test: loaded_bt[key]"
#print loaded_bt[299999]

#print "\n\n returnValue : "
#print returnValue

#if not returnValue:
#	print error
#else:
#	print "AAAAAAA!!! YAAAY!"


#	End of Main Program
#	------------------------------------------------------
