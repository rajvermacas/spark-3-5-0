#!/usr/bin/env python3
"""
Evaluation script to compare the behavior of vars(), dir(), and __dict__ in Python
when used in different contexts:
1. Without parameters
2. With a class
3. With an object instance
"""

import pprint

def print_section(title):
    """Print a section header for better readability"""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80 + "\n")

# Define a sample class with different attributes and methods
class SampleClass:
    class_var = "I am a class variable"
    
    def __init__(self):
        self.instance_var = "I am an instance variable"
        self._private_var = "I am a private instance variable"
        
    def method(self):
        return "I am a method"
        
    @classmethod
    def class_method(cls):
        return "I am a class method"
        
    @staticmethod
    def static_method():
        return "I am a static method"
        
    @property
    def prop(self):
        return "I am a property"

# Create an instance
sample_obj = SampleClass()

# Add a dynamic attribute to the instance
sample_obj.dynamic_attr = "I am a dynamically added attribute"

# SECTION 1: Without parameters
print_section("1. WITHOUT PARAMETERS")

print("vars() without parameters:")
try:
    result = vars()
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\ndir() without parameters:")
try:
    result = dir()
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\n__dict__ without parameters:")
try:
    result = __dict__
    print(f"Type: {type(result)}")
    print("Sample of keys:", list(result.keys())[:5])  # Show just a few keys to avoid overwhelming output
except Exception as e:
    print(f"Error: {e}")

# SECTION 2: With a class
print_section("2. WITH A CLASS")

print("vars(SampleClass):")
try:
    result = vars(SampleClass)
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\ndir(SampleClass):")
try:
    result = dir(SampleClass)
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\nSampleClass.__dict__:")
try:
    result = SampleClass.__dict__
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")

# SECTION 3: With an object instance
print_section("3. WITH AN OBJECT INSTANCE")

print("vars(sample_obj):")
try:
    result = vars(sample_obj)
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\ndir(sample_obj):")
try:
    result = dir(sample_obj)
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")
    
print("\nsample_obj.__dict__:")
try:
    result = sample_obj.__dict__
    pprint.pprint(result)
except Exception as e:
    print(f"Error: {e}")

# SECTION 4: Summary of differences
print_section("4. SUMMARY OF DIFFERENCES")

print("""
vars():
- Without parameters: Returns local symbol table (variables in current scope)
- With class: Returns class.__dict__ (namespace dictionary of the class)
- With object: Returns object.__dict__ (instance attributes dictionary)

dir():
- Without parameters: Returns names in current scope
- With class: Returns all valid attributes for the class (including inherited)
- With object: Returns all valid attributes for the object (including methods, properties, inherited)

__dict__:
- Without parameters: Module's namespace dictionary
- With class: Class namespace dictionary (class attributes, methods)
- With object: Instance namespace dictionary (instance attributes only)
""")
