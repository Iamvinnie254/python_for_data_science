y = 13

print("Memory address of y:", id(y))

# Example: Using type() to determine data type

int_variable = 42
string_variable = "Hello, World!"
float_variable = 3.14

print(type(int_variable))
print(type(string_variable))
print(type(float_variable))

# Type casting
# Example: Specifying data type
integer_variable = int(3.14)
float_variable = float("42.42")
string_variable = str(123)

# Print the variable and its data type. 
# Use colon (:) as separator 
print(integer_variable,":", type(int_variable))
print(float_variable, ":", type(float_variable))
print(string_variable, ":", type(string_variable))

###strings
# Example: positive indexing
my_string = "Hello, World!"

# Accessing characters using positive indexing
first_char = my_string[0]
second_char = my_string[1]
last_char = my_string[12]

print("Positive indexing:")
print("First character:", first_char)
print("Second character:", second_char)
print("Last character:", last_char)
# Example: negative indexing
# Accessing characters using negative indexing
last_char_negative = my_string[-1]
second_last_char = my_string[-2]
first_char_negative = my_string[-13]

print("\nNegative indexing:")
print("Last character:", last_char_negative)
print("Second last character:", second_last_char)
print("First character:", first_char_negative)