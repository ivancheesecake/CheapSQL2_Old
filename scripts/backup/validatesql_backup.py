
#	------------------------------------------------------
#python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\CheapSQL-master\validatesql.py"
#	------------------------------------------------------

import shlex
import sys
import re
from pythonds.basic.stack import Stack
from pythonds.trees.binaryTree import BinaryTree


#	------------------------------------------------------
#	Global variables
#_install_dir = "D:\\My Documents\\AY 2017-2018 1S\\CMSC 227\\CheapSQL\\"
_install_dir = "D:\\Djinn\\Midgard\\Geffen\\Masters\\CMSC 227\\Project Code\\CheapSQL-master\\"
_schema_ext = ".csf"

#	------------------------------------------------------

def loadTables(loc):
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
	
	read_flag = False
	if loc == "ALL_TABLES":
		
		if not loadSchema("STUDENT",withFlags,return_list):
			print "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("STUDENTHISTORY",withFlags,return_list):
			print "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False		
		if not loadSchema("COURSE",withFlags,return_list):
			print "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("COURSEOFFERING",withFlags,return_list):
			print "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False
		if not loadSchema("STUDCOURSE",withFlags,return_list):
			print "\n[ERROR loadSchema] Loading 'STUDENT' schema failed"
			return False		
	else:
		loc_file = loc + _schema_ext
		#schema_file  = open(_install_dir + "scripts\\schema\\" + loc_file,"r")
		#lexer_list = shlex.split(schema_file.read())
		
		#print "\nRECURSE: " + loc
		
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
			#print "\n COLUMN_INFO : "
			#print column_info
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
					print "\n[ERROR] Corrupt schema file : " + _install_dir + "scripts\\schema\\" + loc_file
					print "\n        Line : " + str(i)
					return False
					
				if withFlags:
					return_list.append((loc,col_name,data_type,length,mask,isNullable,False))
				else:
					return_list.append((loc,col_name,data_type,length,mask,isNullable))
		if read_flag == False:
			print "\n[ERROR loadSchema] Possible corrupt schema file : " + _install_dir + "scripts\\schema\\" + loc_file
			return False
		else:
			return True
		#print "\n RETURN LIST : "
		#print return_list
		#print "---------------------------------"

#	------------------------------------------------------
	
def isValidAlias(candidate, selected_tables):
	#check for special characters and shit
	
	for table in selected_tables:
		if candidate.upper() == table[0].upper():
			print "\n[ERROR isValidAlias] Duplicate alias : " + candidate
			return False
	return True

#	------------------------------------------------------

def isValidDate(date_string,mask_string):
	
	re_mask_string = ""
	if mask_string == "YYYY-MM-DD":
		re_mask_string = "[0-9]{4}-(0[1-9]|1[1-2])-(0[1-9]|[1-2][0-9]|[3][0-1])"
	
	mask_checker = re.compile(re_mask_string)
	if mask_checker.match(str(date_string)) is None:
		print "\n[ERROR - isValidDate] Invalid date"
		return False
	else:
		return True
		
#	------------------------------------------------------

def isValidTerm(someTerm):

	return True

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidTables(tables_string, selected_tables):

	#load tables
	table_list = loadTables("ALL_TABLES")
	
	#----------------------------------------------------

	isValid = False
	state = 0
	
	lexer = shlex.shlex(tables_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	for i in range(0, len(lexer_list) ):
	#print lexer_list[i]
		
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
				print "\n[ERROR] Expected a valid target table. Read the goddamn schema man, what the hell is " + str(lexer_list[i].upper()) + "?"
				return False
		
		#Check for comma or AS
		elif state == 1:
			if lexer_list[i] == ",":
				
				state = 0
			elif lexer_list[i].upper() == "AS":
				state = 2
			else:
				print "\n[ERROR] Expected ',' or AS keyword, but instead we got this shit : " + str(lexer_list[i].upper())
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
				print "\n[ERROR] Expected ','"
				return False
		else :
			print "\n[ERROR] Unknown state : " + str(state)
			return False
	
	if (state == 3) or (state == 1) :
		return True
	
	return False

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidColumns(columns_string, selected_tables, selected_columns):

	#load tables
	column_list = list()
	
	#	(alias, table, schema_loc)
	
	for t1 in selected_tables:
		loadSchema(t1[1], False, column_list)
	
	#----------------------------------------------------
	
	isValid = False
	state = 0
	
	lexer_list = shlex.split(columns_string)
	
	#	State
	#	0	Initial, check for alias OR *
	#	1	Check for .
	#	2	Check for column
	#	3	Check for ,
	#	4	Final state if * 
	
	for i in range(0, len(lexer_list) ):
	#print lexer_list[i]
	
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
					print "\n[ERROR] Expected a valid target table name/alias."
					return False
		elif state == 1:
			if lexer_list[i] == ".":
				state = 2
			else:
				print "\n[ERROR] Expected '.' ... it's not that hard man."
				return False
		elif state == 2:
			column_name = lexer_list[i].upper()
			result = next((i for i, v in enumerate(column_list) if ((v[0].upper() == selected_tables[source_table_index][1]) and (v[1].upper() == column_name))), -1)
			if result != -1 :
				selected_columns.append(column_list[result])
				state = 3
			else:
				print "\n[ERROR] Expected a valid column name : " + str(column_name)
				return False
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else:
				print "\n[ERROR] Expected ','"
				return False
		elif state == 4:
			print "\n[ERROR isValidColumns] Expected keyword: FROM"
			return False
		else:
			print "\n[ERROR] Unknown state : " + str(state)
			
	if state in [3,4] :
		return True
	else:
		return False

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidConditions(conditions_string, selected_tables):

	#load tables
	column_list = list()
	loadSchema("ALL_TABLES", False, column_list)
	
	#----------------------------------------------------
	
	isValid = False
	state = 0
	pStack = Stack()
	eTree = BinaryTree("!")
	pStack.push(eTree)
	currentTree = eTree
	
	operatorList = ["=", "AND", "OR"]
	
	#lexer = shlex.shlex(conditions_string, posix=True)
	#lexer.whitespace += ';'
	#lexer_list = list(lexer)
	
	lexer_list = shlex.split(conditions_string)
	for i in range(0, len(lexer_list) ):
	
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
						print "\n[ERROR isValidConditions] Expected a valid number or column name"
						return False
			elif state == 1:
				if term == ".":
					state = 2
				else:
					print "\n[ERROR] Expected '.'"
					return False
			elif state == 2:
				result = next((i for i, v in enumerate(column_list) if ((v[0].upper() == selected_tables[source_table_index][1]) and (v[1].upper() == term))), -1)
				
				if result != -1 :
					rootVal = column_list[result][0] + "." + column_list[result][1]
					state = 99
				else:
					print "\n[ERROR isValidConditions] Expected a valid column name : " + str(term)
					return False
			if state == 99:
				if not pStack.isEmpty():
					state = 0
					currentTree.setRootVal(rootVal)
					parent = pStack.pop()
					currentTree = parent
				else:
					print "\n[ERROR isValidConditions] Unbalanced operations"
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
				print "\n[ERROR isValidConditions] Unbalanced operations"
				return False
		#elif term == ";":
		#	i = len(lexer_list) + 1
		else:
			raise ValueError
	print "fff"
	if not pStack.isEmpty():
		print "\n[ERROR isValidConditions] Unbalanced grouping"
		return False
	else:
		print "asfasdf"
		return True

#	------------------------------------------------------
#	INSERT Checking
def isValidSchemaString(schema_string,target_columns):
	
	#load tables
	table_list = loadTables("ALL_TABLES")
	#target_schema = loadSchema("ALL_TABLES", True)
	#----------------------------------------------------

	isValid = False
	state = 0
	
	#lexer = shlex.shlex(schema_string, posix=True)
	#lexer.whitespace += ','
	#lexer_list = list(lexer)
	
	lexer_list = shlex.split(schema_string)
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
				print "\n[ERROR] Invalid table name : " + term
				return False
		#begin parenthesis
		elif state == 1:
			if term == "(":
				state = 2
			else:
				print "\n[ERROR] Expected '('"
				return False
		#column name
		elif state == 2:
			result = next((i for i, v in enumerate(target_schema) if (v[0].upper() == target_table) and (v[1].upper() == term)), -1)
			if result != -1 :
				if target_schema[result][2] == True:
					print "\n[ERROR] Duplicate column : " + term
					return False
				else:
					#flag the column as selected
					#print "\nRESULT : " + str(result)
					temp_t0 = target_schema[result][0]
					temp_t1 = target_schema[result][1]
					temp_t2 = target_schema[result][2]
					temp_t3 = target_schema[result][3]
					temp_t4 = target_schema[result][4]
					temp_t5 = True
					target_schema[result] = (temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,temp_t5)
					#print "\n -----"
					#print target_schema
					#add the column into target_columns
					
					target_columns.append((temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,ctr))
					
					ctr += 1
					
					state = 3
			else:
				print "\n[ERROR] Invalid column : " + term
				return False
		#comma
		#end parenthesis
		elif state == 3:
			if term == ",":
				state = 2
			elif term == ")":
				isValid = True
			else:
				print "\n[ERROR] Expected ')'"
				return False

		
		else:
			return False
					
		
	
	return isValid
	
#	------------------------------------------------------
#	INSERT Checking
def isValidValuesString(values_string,target_columns):
	
	lexer_list = shlex.split(values_string)
	
	state = 0
	column_count = 0
	
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
				#	Length check
				if len(term) > current_column[3]:
					print "\n[ERROR] Length exceeded " + str(current_column[3])
					return False
				elif len(term) > 0:
					
					if current_column[4] != "*":
						mask_checker = re.compile(current_column[4])
						if mask_checker.match(str(term)) is None:
							print "\n[ERROR] Invalid value; did not match input mask : " + str(current_column[4]) + " : " + str(term)
							return False
						#VALID
					#else:
						# *
				else:
					if not current_column[5]:
						print "\n[ERROR] Column " + current_column[0] + "is not nullable"
						return False
					#VALID
					
			#----DATE
			elif current_column[2] == "DATE":
				#	Length check
				if len(term) > current_column[3]:
					print "\n[ERROR] Length exceeded " + str(current_column[3])
					return False
				elif len(term) > 0:
					if current_column[4] != "*":
						mask_string = current_column[4]
						current_column[4]
						if not isValidDate(term,mask_string):
							print "\n[ERROR] Unsupported date format: " + str(term)
							return False
						#VALID
					#else:
						# *
				else:
					if not current_column[5]:
						print "\n[ERROR] Column " + current_column[0] + "is not nullable"
						return False
					#VALID
			#----INT
			elif current_column[2] == "INTEGER":
				if not term.isdigit():
					print "\n[ERROR] Type mismatch. Expected " + current_column[2]
					return False
				
			column_count += 1
			state = 2
		elif state == 2:
			if lexer_list[i] == ",":
				state = 1 
			elif lexer_list[i] == ")":
				state = 3
	
	if state == 3:
		return True
	else:
		print "\n[ERROR] Expected ')'"
		return False

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
	
	lexer = shlex.shlex(input_sql, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	#lexer_list = shlex.split(input_sql)
	
	
	print lexer_list[0].upper()
	#----- Start statement selection -----

	if lexer_list[0].upper() == "SELECT":
		checkType = "SELECT"
		# start loop 
		for i in range(0, len(lexer_list) ):
			#print lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					print "\n[ERROR isValidSQL] Unexpected query termination"
					return False
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
					print "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				print "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False
			
		# end loop
		if state == 2:
			print "\n [WARNING isValidSQL] Expected ';'"
			state = 3
			
	elif lexer_list[0].upper() == "DELETE":
		checkType = "DELETE"
		# start loop 
		for i in range(0, len(lexer_list) ):
			#print lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					print "\n[ERROR isValidSQL] Unexpected query termination"
					return False
				elif lexer_list[i].upper() == "FROM":
					state = 1
					skipColumnCheck = True
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
					print "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() != "WHERE":
					conditions_string += " "
					conditions_string += lexer_list[i]
				else:
					print "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False
			elif state == 3:
				if warningFlag > 0:
					print "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				print "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False
		# end loop

		if state == 2:
			print "\n [WARNING isValidSQL] Expected ';'"
			state = 3
			
	elif lexer_list[0].upper() == "INSERT":
		
		checkType = "INSERT"
		
		for i in range(0, len(lexer_list) ):
			#print lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i].upper() == "INTO":
					state = 1
			#	Tables
			elif state == 1:
				if lexer_list[i] == ";":
					print "\n[ERROR isValidSQL] Unexpected query termination"
					return False
				elif lexer_list[i].upper() == "VALUES":
					state = 2
				elif lexer_list[i].upper() != "INTO":
					schema_string += " "
					schema_string += lexer_list[i]
				else:
					print "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				if lexer_list[i].upper() != "VALUES":
					values_string += " "
					values_string += lexer_list[i]
				else:
					print "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False
			else:
				print "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False
		# end loop
		
		if state == 2:
			print "\n [WARNING isValidSQL] Expected ';'"
			state = 3
	else:
		print "\n[ERROR isValidSQL] Unknown command : " + lexer_list[0].upper()
		return False
	#----- End statement selection -----
	
	if state != 3:
		print "\n[ERROR isValidSQL] Unexpected query structure : " + str(state)
		return False
	
	
	if checkType == "INSERT":
		#Check INSERT
		target_columns = list()
		if isValidSchemaString(schema_string,target_columns):
			if isValidValuesString(values_string,target_columns):
				return True
			else:
				#print "\n[ERROR isValidSQL] isValidValuesString failed"
				#print "\n values_string : "
				#print "\n" + values_string
				return False
		else:
			#print "\n[ERROR isValidSQL] isValidSchemaString failed"
			#print "\n schema_string : "
			#print "\n" + schema_string
			return False
	else:
		selected_tables = list()
		selected_cloumns = list()

		#Check SELECT
		#Check DELETE
		if isValidTables(tables_string, selected_tables):
			if skipColumnCheck or isValidColumns(columns_string, selected_tables, selected_cloumns):
				if skipConditionsCheck or isValidConditions(conditions_string, selected_tables):
					return True
				else:
					#print "\n[ERROR isValidSQL] isValidConditions failed"
					#print "\n conditions_string : "
					#print "\n" + conditions_string
					return False
			else:
				#print "\n[ERROR isValidSQL] isValidColumns failed"
				#print "\n columns_string : "
				#print "\n" + columns_string
				return False
		else:
			#print "\n[ERROR isValidSQL ] isValidTables failed"
			#print "\n tables_string : "
			#print "\n" + tables_string
			return False
	return False
	
#	------------------------------------------------------
#	Start of Main Program

input_sql_file = open(_install_dir + "test_input.sql","r")
input_sql = input_sql_file.read()

if isValidSQL(input_sql):
	print "\n Is VALID"
	

#	End of Main Program
#	------------------------------------------------------
