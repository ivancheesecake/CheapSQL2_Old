
#	------------------------------------------------------
#python "D:\Djinn\Midgard\Geffen\Masters\CMSC 227\Project Code\SQL_parser_01.py"
#	------------------------------------------------------

import shlex
import sys


#	------------------------------------------------------
#	Global variables
_install_dir = "D:\\Djinn\\Midgard\\Geffen\\Masters\\CMSC 227\\Project Code\\"

#	------------------------------------------------------

def loadTables(loc):
	if loc == "ALL_TABLES":
		return_list = [("STUDENT","STUDENT","STUDENT_SCHEMA"),("STUDENTHISTORY","STUDENTHISTORY","STUDENTHISTORY_SCHEMA"),("COURSE", "COURSE", "COURSE_SCHEMA"),("COURSEOFFERING","COURSEOFFERING","COURSEOFFERING_SCHEMA"),("STUDCOURSE","STUDCOURSE","STUDCOURSE_SCHEMA")]

	return return_list

def loadSchema(loc):
	return_list = list()
	if loc == "ALL_TABLES":
		return_list.append(("STUDENT","STUDNO"))
		return_list.append(("STUDENT","STUDENTNAME"))
		return_list.append(("STUDENT","BIRTHDAY"))
		return_list.append(("STUDENT","DEGREE"))
		return_list.append(("STUDENT","MAJOR"))
		return_list.append(("STUDENT","UNITSEARNED"))
		return_list.append(("STUDENTHISTORY","STUDNO"))
		return_list.append(("STUDENTHISTORY","DESCRIPTION"))
		return_list.append(("STUDENTHISTORY","ACTION"))
		return_list.append(("STUDENTHISTORY","DATEFILED"))
		return_list.append(("STUDENTHISTORY","DATERESOLVED"))
	return return_list
	
def isValidAlias(candidate):
	#check for special characters and shit
	return True


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
	
		if state == 0:
			table_alias = lexer_list[i].upper()
			result = next((i for i, v in enumerate(table_list) if v[0].upper() == table_alias), -1)
			
			if result != -1 :
				selected_tables.append(table_list[result])
				state = 1
			else:
				print "\n [ERROR] Expected a valid target table. Read the goddamn schema man, what the hell is " + str(lexer_list[i].upper()) + "?"
				return False
		
		#Check for comma or AS
		elif state == 1:
			if lexer_list[i] == ",":
				
				state = 0
			elif lexer_list[i].upper() == "AS":
				state = 2
			else:
				print "\n [ERROR] Expected ',' or AS keyword, but instead we got this shit : " + str(lexer_list[i].upper())
				return False
		
		#Check for the actual table
		elif state == 2:
			candidate_alias = lexer_list[i].upper()
			#check if alias is an existing alias or table name
			if isValidAlias(candidate_alias) :
			
				temp_tuple = selected_tables[-1]
				
				temp_t0 = candidate_alias
				temp_t1 = temp_tuple[1]
				temp_t2 = temp_tuple[2]

				temp_tuple = (temp_t0,temp_t1,temp_t2)
				
				selected_tables[-1] = temp_tuple
				
				state = 3
			else :
				print "\n [ERROR] Invalid alias : " + candidate_alias
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
			else :
				print "\n [ERROR] Expected ','"
				return False
		else :
			print "\n [ERROR] Unknown state : " + str(state)
			return False
	
	if (state == 3) or (state == 1) :
		return True
	
	return False


#	------------------------------------------------------

def isValidColumns(columns_string, selected_tables, selected_columns):

	#load tables
	column_list = loadSchema("ALL_TABLES")
	
	#----------------------------------------------------
	
	isValid = False
	state = 0
	
	lexer = shlex.shlex(columns_string, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)
	
	for i in range(0, len(lexer_list) ):
	#print lexer_list[i]
	
		if state == 0:
			table_alias = lexer_list[i].upper()
			result = next((i for i, v in enumerate(selected_tables) if v[0].upper() == table_alias), -1)
			
			if result != -1 :
				source_table_index = result
				state = 1
			else:
				print "\n [ERROR] Expected a valid target table name/alias."
				return False
		elif state == 1:
			if lexer_list[i] == ".":
				state = 2
			else:
				print "\n [ERROR] Expected '.' ... it's not that hard man."
				return False
		elif state == 2:
			column_name = lexer_list[i].upper()
			result = next((i for i, v in enumerate(column_list) if ((v[0].upper() == selected_tables[source_table_index][1]) and (v[1].upper() == column_name))), -1)
			if result != -1 :
				selected_columns.append(column_list[result])
				state = 3
			else:
				print "\n [ERROR] Expected a valid column name : " + str(column_name)
				return False
		elif state == 3:
			if lexer_list[i] == ",":
				state = 0
		else:
			print "\n [ERROR] Unknown state : " + str(state)
			
	if state == 3 :
		return True
	
	return False

#	------------------------------------------------------

def isValidConditions(conditions_string):
	return True

#	------------------------------------------------------

def isValidSQL(input_sql):

	isValid = False
	state = 0
	columns_string = ""
	tables_string = ""
	conditions_string = ""
	

	lexer = shlex.shlex(input_sql, posix=True)
	#lexer.whitespace += ','
	lexer_list = list(lexer)

	#----- Start statement selection -----

	if lexer_list[0].upper() == "SELECT":
		# start loop 
		for i in range(0, len(lexer_list) ):
			#print lexer_list[i]
			
			#	Columns
			if state == 0:
				if lexer_list[i].upper() == "FROM":
					state = 1
					
				elif lexer_list[i].upper() != "SELECT":
					columns_string += " "
					columns_string += lexer_list[i]
				
			#	Tables
			elif state == 1:
				if lexer_list[i].upper() == "WHERE":
					state = 2
				elif lexer_list[i].upper() != "FROM":
					tables_string += " "
					tables_string += lexer_list[i]
					
			elif state == 2:
				if lexer_list[i].upper() != "WHERE":
					conditions_string += " "
					conditions_string += lexer_list[i]
			
		# end loop

	elif lexer_list[0].upper() == "INSERT":
		print "\n Under Construction"
		
	elif lexer_list[0].upper() == "DELETE":
		print "\n Under Construction"

	#----- End statement selection -----
		
	"""
	print "\n"
	print "COLUMNS:\n"
	print columns_string

	print "\n"
	print "TABLES:\n"
	print tables_string

	print "\n"
	print "WHERE:\n"
	print conditions_string
	"""
	
	selected_tables = list()
	selected_cloumns = list()
	
	if isValidTables(tables_string, selected_tables):
		if isValidColumns(columns_string, selected_tables, selected_cloumns):
			if isValidConditions(conditions_string):
				return True
			else:
				print "\n [ERROR] isValidConditions failed"
				print "\n conditions_string : "
				print "\n" + conditions_string
		else:
			print "\n [ERROR] isValidColumns failed"
			print "\n columns_string : "
			print "\n" + columns_string
	else:
		print "\n [ERROR] isValidTables failed"
		print "\n tables_string : "
		print "\n" + tables_string
	
	return False
	
	

#	------------------------------------------------------
#	Main Program




# input_sql_file  = open(_install_dir + "input.sql","r")
# input_sql = input_sql_file.read()

# if isValidSQL(input_sql):
# 	print "\n Is VALID"



#	------------------------------------------------------