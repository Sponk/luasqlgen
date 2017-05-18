return {
	name = "test", -- Name of the Database
	structdef = { -- Defines new struct members
		function(name, fields)
			return "" -- Return a method as Lua string
		end
	},
	
	defines = "", -- Additional defines prepended to the C++ code
	tables = { -- The actual table definitions
		table = { 
			field1 = "string", -- <fieldname> = <fieldtype>
			field2 = "string",
			field3 = "string"
		},
		table2 = {
			reference = "table", -- Fieldtype can be other structure in the database
			field = "int"
		}
	}
}
