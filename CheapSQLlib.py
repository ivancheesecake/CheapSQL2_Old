
#	------------------------------------------------------
#python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\CheapSQL-master\CheapSQLlib.py"

#	------------------------------------------------------

import shlex
import sys
import re
import string
from pythonds.basic.stack import Stack
from pythonds.trees.binaryTree import BinaryTree
import btree
import cPickle as pickle
import copy

#	------------------------------------------------------
#	Global variables
# _install_dir = "\\"
#_install_dir = "D:\\My Documents\\AY 2017-2018 1S\\CMSC 227\\CheapSQL\\"
_install_dir = "D:\\Djinn\\Midgard\\Geffen\\Masters\\CMSC 227\\Project Code\\CheapSQL-master\\"
_schema_ext = ".csf"
_row_delimiter = ";"
_rowfile_extension = ".crf"
_bt_order = 1024

#	Associative array globals
_BRANE_ORDER	= 16


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



#	------------------------------------------------------

def postordereval(row_brane, tree):
	#opers = {'+':operator.add, '-':operator.sub, '*':operator.mul, '/':operator.truediv}
	res1 = None
	res2 = None
	if tree:
		res1 = postordereval(row_brane, tree.getLeftChild())  #// \label{peleft}
		res2 = postordereval(row_brane, tree.getRightChild()) #// \label{peright}

		oper = tree.getRootVal()
		if oper == "AND":
			return res1 and res2
		elif oper == "OR":
			return res1 or res2
		elif oper == "=":
			return row_brane[res1] == res2
		else:
			if tree.getRootVal() == "@":
				return True
			else:
				return tree.getRootVal()

#	------------------------------------------------------


def commaSpace(haystack):

	#Pre-process the commas
	needle = ","
	replacement = " , "
	parts = haystack.split('"')

	for i in range(0,len(parts),2):
	   parts[i] = parts[i].replace(needle,replacement)

	return '"'.join(parts)
	
def newSplit(value):
	lexer = shlex.shlex(value)
	lexer.quotes += '"'
	lexer.whitespace_split = True
	lexer.commenters = ''
	lexer.wordchars += '\''
	
	return list(lexer)

def semicolonSplit(value):
	lexer = shlex.shlex(value,posix=True)
	lexer.quotes = ""
	lexer.whitespace_split = True
	lexer.whitespace = ";"
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

def loadSchema(alias, loc, withFlags, return_list):
	global error
	read_flag = False
	if loc == "ALL_TABLES":
		
		errorFlag, error = loadSchema("STUDENT","STUDENT",withFlags,return_list)
		if errorFlag:
			return False,error
		errorFlag, error = loadSchema("STUDENTHISTORY","STUDENTHISTORY",withFlags,return_list)
		if errorFlag:
			return False,error
		errorFlag, error = loadSchema("COURSE","COURSE",withFlags,return_list)
		if errorFlag:
			return False,error
		errorFlag, error = loadSchema("COURSEOFFERING","COURSEOFFERING",withFlags,return_list)
		if errorFlag:
			return False,error
		errorFlag, error = loadSchema("STUDCOURSE","STUDCOURSE",withFlags,return_list)
		if errorFlag:
			return False,error
	else:
		loc_file = loc + _schema_ext
		#schema_file = open(_install_dir + "scripts\\schema\\" + loc_file,"r")
		#lexer_list = shlex.split(schema_file.read())
		
		#error += "\nRECURSE: " + loc
		
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
			isUnique = False
			
			column_info = file_lines[i].upper()
			#error += "\n COLUMN_INFO : "
			#error += column_info
			if column_info[0] != "#":
			
				column_info_list = shlex.split(column_info)
				#print "[loadSchema] column_info_list"
				#print column_info_list
				#	Parse column information
				for j in range(0, len(column_info_list) ):

					#	commas
					if state in [1,3,5,7,9,11]:
						if column_info_list[j] == ",":
							state += 1				
						else:
							error += "\n[ERROR] Corrupt schema file (type 1) : " + _install_dir + "scripts\\schema\\" + loc_file 
					#	columnName
					elif state == 0:
						col_name = column_info_list[j]
						state += 1
					#	dataType
					elif state == 2:
						data_type = column_info_list[j].upper()
						state += 1
					#	length
					elif state == 4:
						if column_info_list[j] == "*":
							length = 0
						else:
							length = int(column_info_list[j])
						state += 1
					#	mask
					elif state == 6:
						mask = column_info_list[j]
						state += 1
					#	isNullable
					elif state == 8:
						if column_info_list[j].upper() == "FALSE":
							isNullable = False
						elif column_info_list[j].upper() == "TRUE":
							isNullable = True
						state += 1
					#	defaultValue
					elif state == 10:
						defaultValue = column_info_list[j]
						state += 1
					#	isUnique
					elif state == 12:
						if column_info_list[j].upper() == "FALSE":
							isUnique = False
						elif column_info_list[j].upper() == "TRUE":
							isUnique = True
						state += 1

				if state != 13:
					print "[loadSchema][ERROR] state : " + str(state)
					print "[loadSchema][ERROR] Corrupt schema file (type 2) : " + _install_dir + "scripts\\schema\\" + loc_file
					error += "\n[ERROR] Corrupt schema file (type 2) : " + _install_dir + "scripts\\schema\\" + loc_file
					error += "\n Line : " + str(i)
					return False,error
					
				if withFlags:
					return_list.append((alias,loc,col_name,data_type,length,mask,isNullable,defaultValue,isUnique,False))
				else:
					return_list.append((alias,loc,col_name,data_type,length,mask,isNullable,defaultValue,isUnique))
		if read_flag == False:
			error += "\n[ERROR loadSchema] Possible corrupt schema file : " + _install_dir + "scripts\\schema\\" + loc_file
			return False,error
		else:
			return True,error

#	------------------------------------------------------
	
def isValidAlias(candidate, selected_tables):
	#check for special characters and shit
	global error
	for table in selected_tables:
		if candidate.upper() == table[0].upper():
			error += "\n[ERROR isValidAlias] Duplicate alias : " + candidate
			return False,error
	return True,error

#	------------------------------------------------------

def isValidDate(date_string,mask_string):
	global error
	re_mask_string = ""
	if mask_string == "YYYY-MM-DD":
		re_mask_string = "[0-9]{4}-(0[1-9]|1[1-2])-(0[1-9]|[1-2][0-9]|[3][0-1])"
	
	mask_checker = re.compile(re_mask_string)
	if mask_checker.match(str(date_string.strip('"'))) is None:
		error += "\n[ERROR - isValidDate] Invalid date sdfgsd"
		print error
		return False,error
	else:
		return True,error
		
#	------------------------------------------------------

def isValidTerm(someTerm):
	global error
	return True,error

#	------------------------------------------------------

def getSearchAllKey(some_table):
	
	cur_table = some_table.upper()
	if cur_table == "STUDENT":
		return "STUDNO"
	elif cur_table == "STUDENTHISTORY":
		return "STUDNO"
	elif cur_table == "COURSE":
		return "CNO"
	elif cur_table == "COURSEOFFERING":
		return "CNO"
	elif cur_table == "STUDCOURSE":
		return "STUDNO"
	
#	------------------------------------------------------

def getColumnPosition(selected_column):

	schema_list = list()
	loadSchema(selected_column[1],selected_column[1],False,schema_list)
	for i in range(0,len(schema_list)):
		if selected_column[_SCHEMA_COLUMN] == schema_list[i][_SCHEMA_COLUMN]:
			return i
	return -1

#	------------------------------------------------------

def addToBulk(bulk, selected_table, selected_columns, conditionTree, applyCondition):
	'''
	addToBulk(
		bulk 				list(BPlustree(_BRANE_ORDER))	,
		selected_tables		list(list())	,
		selected_columns	list()
	)
	'''
	
	global error
	_COLUMN_NAME = 0
	_COLUMN_POSITION = 1
	
	#print "[addToBulk] selected_table"
	#print selected_table
	#print "[addToBulk] selected_columns"
	#print selected_columns
	#print "[addToBulk] conditionTree.postorder()"
	#print conditionTree.postorder()
	print "[addToBulk] conditionTree.inorder()"
	conditionTree.inorder()
	print "-------------------------------------"
	
	#Definitions
	cur_table_name = selected_table[_TABLE_TABLE]
	cur_table_alias = selected_table[_TABLE_ALIAS]
	cur_schema = list()
	cur_column_name = getSearchAllKey(selected_table[_TABLE_TABLE])
	cur_index_bt = btree.BPlusTree(_bt_order)
	
	
	#print "cur_table_alias : " + cur_table_alias
	#print "cur_table_name : " + cur_table_name
	
	successFlag, error = loadSchema(cur_table_alias,cur_table_name, False, cur_schema)
	if not successFlag:
		return False,bulk,error 
	
	
	#print "cur_table_name : " + cur_table_name
	#print "cur_table_alias : " + cur_table_alias
	#print "cur_column_name : " + cur_column_name
	
	#Pickle load
	cur_index_bt = pickle.load(open(_install_dir + "scripts\\indexes\\" + cur_table_name.lower() + "_" + cur_column_name.upper() + "_index_bt.pkl", 'rb'))
	
	#Get the column positions (in the schema) of the selected columns
	selected_rows_pos = list()
	for i in range(0,len(selected_columns)):
		col_pos = getColumnPosition(selected_columns[i])
		
		if col_pos == -1:
			error += "[addToBulk][ERROR] Column position for '"+str(selected_columns[i][_SCHEMA_COLUMN])+"' on table '"+str(selected_columns[i][_SCHEMA_TABLE])+"' as '"+str(selected_columns[i][_SCHEMA_ALIAS])+"'  not found"
			print error
			return False,bulk,error
		else:
			selected_rows_pos.append((selected_columns[i][_SCHEMA_COLUMN],col_pos))
	
	#Get all rows from the index file
	file_list = cur_index_bt.values()
	
	#print "file_list : " + str(len(file_list))
	#for cnt in range(0,len(file_list)):
	#	print file_list[cnt]
	
	#print "bulk : " + str(len(bulk))
	#for cnt in range(0,len(bulk)):
	#	print bulk[cnt]
		
	#bulk related shit
	new_bulk = list()
	test_count = 0
	test_count2 = 0
	
	if len(bulk) == 0:
		emptyBulk_flag = True
		b_start = -1
	else:
		emptyBulk_flag = False
		b_start = 0
	
	for b in range(b_start,len(bulk)):

		#test_count2 += 1
		#print "--- bulk : " + str(b)
		test_count3 = 0
		#print "file_list : " + str(len(file_list))
		#for cnt in range(0,len(file_list)):
		#	print file_list[cnt]
		
		for i in range(0,len(file_list)):
			for j in range(0,len(file_list[i])):
				#	Read the file
				
				cur_row_file = open(file_list[i][j], 'r')
				cur_row_string = cur_row_file.read()
				cur_row_list = semicolonSplit(cur_row_string)
				
				#print "cur_row_list : "
				#for cnt in range(0,len(cur_row_list)):
				#	print cur_row_list[cnt]
				
				#Iterate through selected rows
				
				if not emptyBulk_flag:
					brane = btree.BPlusTree(_BRANE_ORDER)
					brane = copy.deepcopy(bulk[b])
				else:
					brane = btree.BPlusTree(_BRANE_ORDER)
				
				#	Find the right position
				#for r in range(0,len(selected_rows_pos)):
				for col_cnt in range(0,len(cur_row_list)):
					
					for pos_cnt in range(0,len(selected_rows_pos)):
						if selected_rows_pos[pos_cnt][_COLUMN_NAME] == cur_schema[col_cnt][_SCHEMA_COLUMN]:
							display_flag = True
						else:
							display_flag = False
						
					cur_bulk_key = selected_table[_TABLE_ALIAS].upper() + "." + cur_schema[col_cnt][_SCHEMA_COLUMN].upper()
					cur_bulk_value = cur_row_list[col_cnt].strip('"')
					brane.insert(cur_bulk_key,cur_bulk_value)
					#print "[addToBulk] Successfully inserted ("+cur_bulk_key+","+cur_bulk_value+")"
				
				#	Only apply the conditions on the last join
				print "applyCondition : " + str(applyCondition)
				if applyCondition:
					eval_result = postordereval(brane, conditionTree)
					#print "[addToBulk] eval_result : " + str(eval_result)
					if eval_result is True:
						new_bulk.append(brane)
				else:
					new_bulk.append(brane)
				
	#bulk = new_bulk
	return True,new_bulk,error
				
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
	
	#print " [isValidTables]\n"
	#print lexer_list
	
	for i in range(0, len(lexer_list) ):
	#error += lexer_list[i]
		
		#	States:
		#	0	Initial (checks)
		#	1	AS
		#	2	Check for alias validity
		#	3	Check for comma ( -> 0)
		
		if state == 0:
			
			table_alias = lexer_list[i].upper()
			result = next((i for i, v in enumerate(table_list) if v[_TABLE_ALIAS].upper() == table_alias), -1)
			
			if result != -1 :
				selected_tables.append(table_list[result])
				state = 1
			else:
				error += "\n[ERROR] Expected a valid target table. Read the goddamn schema man, what the hell is " + str(lexer_list[i].upper()) + "?"
				return False,error
		
		#Check for comma or AS
		elif state == 1:
			if lexer_list[i] == ",":
				
				state = 0
			elif lexer_list[i].upper() == "AS":
				state = 2
			else:
				error += "\n[ERROR] Expected ',' or AS keyword, but instead we got this shit : " + str(lexer_list[i].upper())
				return False,error
		
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
				return False,error
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else :
				error += "\n[ERROR] Expected ','"
				return False,error
		else :
			error += "\n[ERROR] Unknown state : " + str(state)
			return False,error
	
	if (state == 3) or (state == 1) :
		return True,error
	
	return False,error

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidColumns(columns_string, selected_tables, selected_columns):
	global error
	#load tables
	column_list = list()
	
	#	(alias, table, schema_loc)
	
	for t1 in selected_tables:
		loadSchema(t1[_SCHEMA_ALIAS], t1[_SCHEMA_TABLE], False, column_list)
	
	#----------------------------------------------------
	
	isValid = False
	state = 0
	
	lexer = shlex.shlex(columns_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	#lexer_list = shlex.split(columns_string)
	
	
	#print "[isValidColumns][lexer_list]:"
	#print lexer_list
	#print "[isValidColumns][selected_tables]:"
	#print selected_tables
	
	#	State
	#	0	Initial, check for alias OR *
	#	1	Check for .
	#	2	Check for column
	#	3	Check for ,
	#	4	Final state if * 
	
	for i in range(0, len(lexer_list) ):
	#error += lexer_list[i]
	
		if state == 0:
			if lexer_list[i] == "*":
				selected_columns.extend(column_list)
				state = 4
			else:
				table_alias = lexer_list[i].upper()
				result = next((i for i, v in enumerate(selected_tables) if v[_TABLE_ALIAS].upper() == table_alias), -1)
				
				if result != -1 :
					source_table_index = result
					state = 1
				else:
					error += "\n[ERROR] Expected a valid target table name/alias. ["+table_alias+"]"
					return False,error
		elif state == 1:
			if lexer_list[i] == ".":
				state = 2
			else:
				error += "\n[ERROR] Expected '.' ... it's not that hard man."
				return False,error
		elif state == 2:
			column_name = lexer_list[i].upper()
			
			#print "AAAAAAAAAAAAAAAAAAAAAAAA"
			#print selected_tables
			#print "AAAAAAAAAAAAAAAAAAAAAAAA"
			#print column_list
			
			result = next((i for i, v in enumerate(column_list) if ((v[_SCHEMA_ALIAS].upper() == selected_tables[source_table_index][_TABLE_ALIAS].upper()) and (v[_SCHEMA_COLUMN].upper() == column_name.upper()))), -1)
			if result != -1 :
				selected_columns.append(column_list[result])
				state = 3
			else:
				error += "\n[ERROR] Expected a valid column name : " + str(column_name)
				return False,error
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else:
				error += "\n[ERROR] Expected ','"
				return False,error
		elif state == 4:
			error += "\n[ERROR isValidColumns] Expected keyword: FROM"
			return False,error
		else:
			error += "\n[ERROR] Unknown state : " + str(state)
			
	if state in [3,4] :
		return True,error
	else:
		return False,error

#	------------------------------------------------------
#	SELECT/DELETE Checking
def isValidConditions(conditions_string, selected_tables):
	global error
	#load tables
	column_list = list()
	
	for ctr in range(0,len(selected_tables)):
		loadSchema(selected_tables[ctr][_SCHEMA_ALIAS],selected_tables[ctr][_SCHEMA_TABLE], False, column_list)
		
	#print "[isValidConditions] column_list : "
	#print column_list

	#print "[isValidConditions] conditions_string : "
	#print conditions_string
	
	#----------------------------------------------------
	global error
	
	isValid = False
	state = 0
	pStack = Stack()
	eTree = BinaryTree("@")
	pStack.push(eTree)
	currentTree = eTree
	
	ctr = 0
	operatorList = ["=", "AND", "OR"]
	
	lexer = shlex.shlex(conditions_string, posix=True)
	#lexer.whitespace += ';'
	lexer_list = list(lexer)
	
	#print "[isValidConditions][lexer_list]:"
	#print lexer_list
	#print "[isValidColumns][selected_tables]:"
	#print selected_tables
	
	#lexer_list = shlex.split(conditions_string)
	
	some_list = newSplit(conditions_string)
	
	#print "lexer_list"
	#print lexer_list
	#print "some_list"
	#print some_list
	
	for i in range(0, len(lexer_list) ):
		ctr += 1
		#-----------
		
		term = lexer_list[i].upper()
		origTerm = lexer_list[i]
		
		if term == "(":
			currentTree.insertLeft("")
			pStack.push(currentTree)
			currentTree = currentTree.getLeftChild()
		elif (term not in operatorList) and (term not in ["(",")",";"]):
		
			# Check if term is a valid column or value
			if state == 0:
				#print "[isValidConditions] term : " + term
				#print "[isValidConditions] selected_tables : "
				#print selected_tables
				
				result = next((i for i, v in enumerate(selected_tables) if v[_TABLE_ALIAS].upper() == term), -1)
				if result != -1 :
					source_table_index = result
					state = 1
				else:
					if isValidTerm(term) :
						rootVal = origTerm
						state = 99
					else:
						error += "\n[ERROR] Expected a valid number or column name"
						print error
						return False,eTree,error
			elif state == 1:
				if term == ".":
					state = 2
				else:
					error += "\n[ERROR] Expected '.'"
					print error
					return False,eTree,error
			elif state == 2:
				result = next((i for i, v in enumerate(column_list) if ((v[_SCHEMA_ALIAS].upper() == selected_tables[source_table_index][_TABLE_ALIAS].upper()) and (v[_SCHEMA_COLUMN].upper() == term.upper()))), -1)
				
				if result != -1 :
					rootVal = column_list[result][_SCHEMA_ALIAS] + "." + column_list[result][_SCHEMA_COLUMN]
					state = 99
				else:
					error += "\n[ERROR] Expected a valid column name : " + str(term)
					print error
					return False,eTree,error
			if state == 99:
				if not pStack.isEmpty():
					state = 0
					currentTree.setRootVal(rootVal)
					parent = pStack.pop()
					currentTree = parent
				else:
					error += "\n[ERROR] Unbalanced operations"
					print error
					return False,eTree,error
			
		elif term in operatorList:
			currentTree.setRootVal(term)
			currentTree.insertRight("")
			pStack.push(currentTree)
			currentTree = currentTree.getRightChild()
		elif term == ")":
			if not pStack.isEmpty():
				currentTree = pStack.pop()
			else:
				error += "\n[ERROR] Unbalanced operations"
				print error
				return False,eTree,error
		#elif term == ";":
		#	i = len(lexer_list) + 1
		else:
			#print "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
			raise ValueError

	
	
	#print "[isValidConditions] currentTree.inorder()"
	#currentTree.inorder()
	#print "[isValidConditions] --------------->>"
	#print "[isValidConditions] eTree.inorder()"
	#eTree.inorder()
	#print "[isValidConditions] --------------->>"
	
	
	
	if ctr > 0:
		if not pStack.isEmpty():
			error += "\n[ERROR] Unbalanced grouping"
			return False,eTree,error
		else:
			return True,eTree,error
	else:
		return True,eTree,error
#	------------------------------------------------------
#	INSERT Checking
def isValidSchemaString(schema_string,target_columns):
	global error
	#load tables
	table_list = loadTables("ALL_TABLES")
	#----------------------------------------------------

	isValid = False
	state = 0
	
	lexer = shlex.shlex(schema_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	#print " [isValidSchemaString][lexer_list] : "
	#print lexer_list
	#for i in range(0,len(lexer_list)):
	#	print lexer_list[i]
	#lexer_list = shlex.split(schema_string)
	ctr = 0
	
	for i in range(0, len(lexer_list) ):
		term = lexer_list[i].upper()
		if state == 0:
			result = next((i for i, v in enumerate(table_list) if v[_TABLE_ALIAS].upper() == term), -1)
			if result != -1 :
				target_schema = list()
				loadSchema(term, term, True, target_schema)
				target_table = term
				state = 1
			else:
				error += "\n[isValidSchemaString][ERROR] Invalid table name : " + term
				return False,error
		#begin parenthesis
		elif state == 1:
			if term == "(":
				state = 2
			else:
				error += "\n[isValidSchemaString][ERROR] Expected '('"
				return False,error
		#column name
		elif state == 2:
			result = next((i for i, v in enumerate(target_schema) if (v[_SCHEMA_ALIAS].upper() == target_table) and (v[_SCHEMA_COLUMN].upper() == term)), -1)
			if result != -1 :
				if target_schema[result][2] == True:
					error += "\n[isValidSchemaString][ERROR] Duplicate column : " + term
					return False,error
				else:
					#flag the column as selected
					#error += "\nRESULT : " + str(result)
					temp_t0 = target_schema[result][_SCHEMA_ALIAS]			#	Alias
					temp_t1 = target_schema[result][_SCHEMA_TABLE]  		#	Table name
					temp_t2 = target_schema[result][_SCHEMA_COLUMN]  		#	Column name
					temp_t3 = target_schema[result][_SCHEMA_DATA_TYPE]  	#	DataType
					temp_t4 = target_schema[result][_SCHEMA_LENGTH]  		#	Length
					temp_t5 = target_schema[result][_SCHEMA_MASK]  			#	Mask
					temp_t6 = target_schema[result][_SCHEMA_ISNULLABLE] 	#	isNullable
					temp_t7 = target_schema[result][_SCHEMA_DEFAULTVALUE]	#	defaultValue
					temp_t8 = target_schema[result][_SCHEMA_ISUNIQUE]		#	isUnique
					temp_t9 = True
					
					#target_schema[result] = (temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,temp_t5)
					#error += "\n -----"
					#error += target_schema
					#add the column into target_columns
					
					target_columns.append((temp_t0,temp_t1,temp_t2,temp_t3,temp_t4,temp_t5,temp_t6,temp_t7,temp_t8,temp_t9,ctr))
					
					ctr += 1
					state = 3
			else:
				error += "\n[isValidSchemaString][ERROR] Invalid column : " + term
				return False,error
		#comma
		#end parenthesis
		elif state == 3:
			if term == ",":
				state = 2
			elif term == ")":
				isValid = True
			else:
				error += "\n[ERROR] Expected ')' [" + term + "]"
				return False,error
		else:
			return False,error
					
	return isValid,error
	
#	------------------------------------------------------
#	INSERT Checking
def isValidValuesString(values_string,target_columns,values_list):
	global error
	
	lexer_list = newSplit(values_string)
	#print "[isValidValuesString][lexer_list][newSplit] : "
	#print lexer_list
	#for i in range(0,len(lexer_list)):
	#	print lexer_list[i]
	#	
	#print "[isValidValuesString][target_columns] : "
	#for i in range(0,len(target_columns)):
	#	print target_columns[i]
	
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
			#	0	Alias
			#	1	Table name
			#	2	Column name
			#	3	Data type
			#	4	Length
			#	5	Mask
			#	6	IsNullable
			#	7	DefaultValue
			#	8	IsUnique
			#print "[isValidValuesString] current_column"
			#print current_column
			#----STRING
			if current_column[_SCHEMA_DATA_TYPE] == "STRING":
				
				if (term != ",") and (term != ")"):
					tempString += term
					
					#	Length check
					if len(tempString) > current_column[_SCHEMA_DATA_TYPE]:
						error += "\n[ERROR] Length exceeded " + str(current_column[_SCHEMA_DATA_TYPE])
						return False,error
					elif (len(tempString) > 0) and (tempString.upper() != "NULL"):
						
						if current_column[_SCHEMA_MASK] != "*":
							
							mask_checker = re.compile(current_column[_SCHEMA_MASK])
							#string.replace(tempString,"\"","")
							tempString = tempString.strip('"')	

							
							if mask_checker.match(str(tempString)) is None:
								error += "\n[ERROR] Invalid value; did not match input mask : " + str(current_column[_SCHEMA_MASK]) + " : " + str(tempString)
								return False,error
							else:
								# VALID!!!!!!! [with Mask]
								#print "[isValidValuesString] String " + str(tempString)
								valid_value = tempString								
						else:
							# VALID!!!!!!! [without Mask]
							#print "[isValidValuesString] String " + str(tempString)
							valid_value = tempString
					else:
						# isNullable checking
						if not current_column[_SCHEMA_ISNULLABLE]:
							error += "\n[ERROR][String] Column " + current_column[_SCHEMA_COLUMN] + "is not nullable"
							return False,error
						# defaultValue
						else:
							tempString = current_column[_SCHEMA_DEFAULTVALUE]
							
						if current_column[_SCHEMA_ISUNIQUE]:
							#Check if unique
							cur_target_table = current_column[_SCHEMA_TABLE]
							cur_target_column = current_column[_SCHEMA_COLUMN]
							
							cur_col_index_bt = btree.BPlusTree(_bt_order)
							cur_col_index_bt = pickle.load(open(_install_dir + "scripts\\indexes\\" + cur_target_table + "_" + cur_target_column + "_index_bt.pkl", 'rb'))
							
							if cur_col_index_bt[tempString] is not None:
								error += "\n[ERROR] Value '" + tempString + "' for column '"+cur_target_column+"' already exists in table '"+cur_target_table+"'"
								print error
						
						# VALID!!!!!!!
						#print "[isValidValuesString] String " + str(tempString)
						valid_value = tempString
						tempString = ""
						#print " yy: " + str(valid_value)
						if term == ",":
							state = 1
							tempString = ""
						elif term == ")":
							state = 3
							
			#----DATE
			elif current_column[_SCHEMA_DATA_TYPE] == "DATE":
				
				if (term != ",") and (term != ")"):
					tempDate += term
					#print "[isValidValuesString] Start tempDate : " + str(tempDate)
					# Perform the actual checking
					#	Length check
					if len(tempDate) > current_column[_SCHEMA_DATA_TYPE]:
						print "[isValidValuesString][ERROR] Length exceeded " + str(current_column[_SCHEMA_DATA_TYPE])
						error += "\n[ERROR] Length exceeded " + str(current_column[_SCHEMA_DATA_TYPE])
						return False,error
					elif len(tempDate) > 0:
						if current_column[_SCHEMA_MASK] != "*":
							mask_string = current_column[_SCHEMA_MASK]
							#current_column[_SCHEMA_MASK]
							successFlag,error = isValidDate(tempDate,mask_string)
							if not successFlag:
								return False,error
							else:
								# VALID!!!!!!!
								#print "[isValidValuesString] Date " + str(tempDate)
								valid_value = tempDate
					else:
						# isNullable check
						if not current_column[_SCHEMA_ISNULLABLE]:
							error += "\n[ERROR][Date] Column " + current_column[_SCHEMA_COLUMN] + "is not nullable"
							return False,error
						# defaultValue
						else:
							tempDate = current_column[_SCHEMA_DEFAULTVALUE]
							
						# VALID!!!!!!!
						#print "[isValidValuesString] Date " + str(tempDate)
						valid_value = tempDate
						tempDate = ""
						if term == ",":
							state = 1
						elif term == ")":
							state = 3
							
						

			#----INT
			elif current_column[_SCHEMA_DATA_TYPE] == "INTEGER":
				tempInt = term
				if (tempInt.upper() == "NULL") or (tempInt != ""):
					if not current_column[_SCHEMA_ISNULLABLE]:
						print "[ERROR][Integer] Column " + current_column[_SCHEMA_COLUMN] + " is not nullable"
						print "[ERROR][Integer] isNullable : " + str(current_column[_SCHEMA_ISNULLABLE])
						error += "\n[ERROR][Integer] Column " + current_column[_SCHEMA_COLUMN] + "is not nullable"
						return False,error
					else:
						tempInt = current_column[_SCHEMA_DEFAULTVALUE]
						
				if (not tempInt.isdigit()):
					error += "\n[ERROR] Type mismatch. Expected " + current_column[_SCHEMA_DATA_TYPE]
					return False,error
				else:
					#print "[isValidValuesString] Integer " + str(tempInt)
					valid_value = tempInt
					tempInt = ""
					#values_list.append(valid_value)
					
			column_count += 1
			#print "[isValidValuesString]" + str(column_count) + ":" + str(term)
			
			
			
			state = 2
		elif state == 2:
			#print "[isValidValuesString] Appending : " + str(valid_value)
			values_list.append(valid_value)
			if lexer_list[i] == ",":
				state = 1 
				tempDate = ""
				tempString = ""
				tempInt = ""
				valid_value = ""
			elif lexer_list[i] == ")":
				state = 3
	
	if state == 3:
		if column_count != len(target_columns):
			error += "\n[ERROR][isValidValuesString] Value count and target column count mismatch : ("+str(column_count)+"/"+str(len(target_columns))+")"
		else:
			return True,error

	else:
		error += "\n[ERROR][isValidValuesString]["+str(state)+"] Expected ')'"
		return False,error

#	------------------------------------------------------
#	INSERT Execute
def executeInsert(target_columns,values_list):
	global error
	
	target_schema = list()
	loadSchema(target_columns[0][_SCHEMA_ALIAS],target_columns[0][_SCHEMA_TABLE], True, target_schema)
	
	#print "[executeInsert] target_schema"
	#print target_schema
	#print "-----------------"
	#for i in range(0,len(target_schema)):
	#	print target_schema[i]
	
	#print "[executeInsert] values_list"
	#print values_list
	#print "-----------------"
	#for i in range(0,len(values_list)):
	#	print values_list[i]
		
	index_bt = list()
	
	outputString = ""
	row_filename = ""
	
	delimitFlag = False
	filename_flag = False
	
	
	#	Generate the output string and filename
	for col_cnt in range(0, len(target_schema) ):
		
		cur_target_table = str(target_schema[col_cnt][_SCHEMA_ALIAS]).lower()
		cur_target_column = str(target_schema[col_cnt][_SCHEMA_COLUMN]).upper()
		
		new_bt = btree.BPlusTree(_bt_order)
		new_bt_filename = _install_dir + "scripts\\indexes\\" + cur_target_table + "_" + cur_target_column + "_index_bt.pkl"
		new_bt = pickle.load(open(new_bt_filename, 'rb'))
		
		index_bt.append((new_bt,new_bt_filename))
		#print "[executeInsert] Append to index_bt list SUCCESS"
		
		for j in range(0, len(target_columns)):
			if cur_target_column == target_columns[j][_SCHEMA_COLUMN]:
				#print str(i) + "/" + str(j)
				
				if col_cnt == 0:
					row_filename = _install_dir + "scripts\\data\\" + cur_target_table.upper() + "_" + str(values_list[j]) + _rowfile_extension
					filename_flag = True
				
				if delimitFlag == False:
					outputString = str(values_list[j])
					delimitFlag = True
				else:
					outputString += _row_delimiter + str(values_list[j])
				
				#	Add a new index bt node with a blank filename_list
				#print "================================="
				#print col_cnt
				#print _SCHEMA_ISUNIQUE
				#print target_schema[col_cnt][_SCHEMA_ISUNIQUE]
				#print "================================="
				if (target_schema[col_cnt][_SCHEMA_ISUNIQUE]) and (index_bt[col_cnt][0][values_list[j]] is not None):
					error += "\n[ERROR][executeInsert] Duplicate key detected : " + str(values_list[j])
					print error
					return False,error
				else:
					filename_list = list()
					filename_list.append(row_filename)
					index_bt[col_cnt][0].insert(str(values_list[j]), filename_list)
					
				
	if filename_flag:
	
		for bt_cnt in range(0,len(index_bt)):
			pickle.dump(index_bt[bt_cnt][0], open(index_bt[bt_cnt][1], 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
	
		f = open(row_filename, 'w')
		f.write(outputString)
		
		print "[executeInsert] Inserted 1 row"
		
	else:
		#print "[executeInsert] FAAAAAAAAAAIL BITCH"
		error += "\n[ERROR][executeInsert] No filename string defined"
	return True,error
			
#	------------------------------------------------------
#	SELECT Execute
def executeSelect(selected_tables,selected_columns,conditionTree):
	global error
	
	#print "[executeSelect] Start"
	#print "[executeSelect] selected_tables"
	#for i in range(0,len(selected_tables)):
	#	print selected_tables[i]
	#
	#print "[executeSelect] selected_columns"
	#for i in range(0,len(selected_columns)):
	#	print selected_columns[i]
	
	result_bt = btree.BPlusTree(_bt_order)
	result_list = list()
	index_count = 0
	index_list = list()
	
	#	Implementing an associative array as a BPlusTree
	#	- The elements of the bulk ARE BPlusTrees, not the bulk itself
	result_bulk = list()
	'''
		addToBulk(
			bulk 				list(BPlustree(_BRANE_ORDER))	,
			selected_tables		list(list())	,
			selected_columns	list()
		)
	'''
	for table_cnt in range(0,len(selected_tables)):
		if table_cnt == len(selected_tables) - 1 :
			applyCondition = True
		else:
			applyCondition = False
			
		successFlag,result_bulk,error = addToBulk(result_bulk,selected_tables[table_cnt],selected_columns, conditionTree, applyCondition)
		if not successFlag:
			return False,selected_columns,None,error
	
	#result_list = result_bulk.values()
	
	
	return True,selected_columns,result_bulk,error
	
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
	
	checkType = None
	outputList = list()
	
	tier1_list = semicolonSplit(input_sql.encode('utf8'))
	lexer_list = newSplit(commaSpace(tier1_list[0]))
	
		
	#print " [isValidSQL][input_sql] :"
	#print input_sql
	#print " [isValidSQL][tier1_list] :"
	#print tier1_list
	print " [isValidSQL][lexer_list] :"
	print lexer_list
	
	#----- Start statement selection -----
	
	if lexer_list[0].upper() == "SELECT":
		
		checkType = "SELECT"
		# start loop 
		for i in range(0, len(lexer_list) ):
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					error += "\n[ERROR isValidSQL] Unexpected query termination"
					return False,checkType,outputList,error
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
					#print "[isValidSQL]* lexer_list["+str(i)+"]" + str(lexer_list[i])
					#state = 3
					conditions_string += " "
					conditions_string += str(lexer_list[i])
				else:
					print "[isValidSQL] lexer_list["+str(i)+"]" + str(lexer_list[i])
					
			elif state == 3:
				if warningFlag == 0:
					error += "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				error += "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,checkType,outputList,error
			
		# end loop
			
	elif lexer_list[0].upper() == "DELETE":
		checkType = "DELETE"
		# start loop 
		for i in range(0, len(lexer_list) ):
			#error += lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i] == ";":
					error += "\n[ERROR isValidSQL] Unexpected query termination"
					return False,checkType,outputList,error
				elif lexer_list[i].upper() == "FROM":
					state = 1
					#print "change state ..."
					skipColumnCheck = True
				elif lexer_list[i].upper() != "DELETE":
					error += "\n[ERROR isValidSQL] Expected keyword : FROM, " + lexer_list[i].upper() 
					return False,checkType,outputList,error
			#	Tables
			elif state == 1:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() == "WHERE":
					state = 2
				elif lexer_list[i].upper() not in ["SELECT","FROM","WHERE"]:
					tables_string += " "
					tables_string += lexer_list[i]
				else:
					error += "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,checkType,outputList,error
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				elif lexer_list[i].upper() not in ["SELECT","FROM","WHERE"]:
					conditions_string += " "
					conditions_string += lexer_list[i]
				else:
					error += "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,checkType,outputList,error
			elif state == 3:
				if warningFlag > 0:
					error += "\n[WARNING isValidSQL] Statements after ';' delimiter will be ignored"
					warningFlag += 1
			else:
				error += "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,checkType,outputList,error
		# end loop

		if state == 2:
			error += "\n [WARNING isValidSQL] Expected ';'"
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
					error += "\n[ERROR isValidSQL] Unexpected query termination"
					return False,checkType,outputList,error
				elif lexer_list[i].upper() == "VALUES":
					state = 2
				elif lexer_list[i].upper() != "INTO":
					schema_string += " "
					schema_string += lexer_list[i]
				else:
					error += "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,checkType,outputList,error
			elif state == 2:
				if lexer_list[i] == ";":
					state = 3
				if lexer_list[i].upper() != "VALUES":
					values_string += " "
					values_string += lexer_list[i]
				else:
					error += "\n[ERROR isValidSQL] Duplicate keyword : " + lexer_list[i].upper() 
					return False,checkType,outputList,error
			else:
				error += "\n[ERROR isValidSQL] Unknown state : " + str(state)
				return False,checkType,outputList,error
		# end loop
		
		if state == 2:
			#error += "\n [WARNING isValidSQL] Expected ';'"
			state = 3
	else:
		error += "\n[ERROR isValidSQL] Unknown command : " + str(lexer_list[0].upper())
		#error += "\n" + str(lexer_list

		
		return False,checkType,outputList,error
	#----- End statement selection -----
	if state not in [1,2,3]:
		error += "\n[ERROR isValidSQL] Unexpected query structure : " + str(state)
		return False,checkType,outputList,error
	
	if checkType == "INSERT":
	
		values_list = list()
		target_columns = list()
	
		#Check INSERT
		successFlag,error = isValidSchemaString(schema_string,target_columns)
		if successFlag:
			successFlag,error = isValidValuesString(values_string,target_columns,values_list)
			if successFlag:
				
				outputList.append(target_columns)
				outputList.append(values_list)
				
				return True,checkType,outputList,error
			else:
				print "INVALID DAAAAAAAATE"
				return False,checkType,outputList,error
		else:
			return False,checkType,outputList,error
	else:
		selected_tables = list()
		selected_columns = list()

		#Check SELECT
		#Check DELETE
		successFlag, error = isValidTables(tables_string, selected_tables)
		if successFlag:
		
			if not skipColumnCheck:
				successFlag, error = isValidColumns(columns_string, selected_tables, selected_columns)
			else:
				successFlag = True
				
			if successFlag:
			
				if not skipConditionsCheck:
					successFlag,conditionTree,error = isValidConditions(conditions_string, selected_tables)
				else:
					successFlag = True
			
				if successFlag:
					
					outputList.append(selected_tables)
					outputList.append(selected_columns)
					outputList.append(conditionTree)
					return True,checkType,outputList,error
				else:
					return False,checkType,outputList,error
			else:
				return False,checkType,outputList,error
		else:
			return False,checkType,outputList,error
	return False,checkType,outputList,error
	
#	------------------------------------------------------
#	Start of Main Program




#	End of Main Program
#	------------------------------------------------------
