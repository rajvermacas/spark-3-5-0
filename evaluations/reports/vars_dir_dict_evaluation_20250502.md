# Evaluation Report: vars(), dir(), and __dict__ in Python

## Evaluation Goal
Evaluate the differences between `vars()`, `dir()`, and `__dict__` in Python when used in three different contexts:
1. Without any parameters
2. With a class
3. With an object instance

## Execution Details
- **Subject evaluated**: Python built-in functions and attributes: `vars()`, `dir()`, and `__dict__`
- **Script filename**: `evaluations/scripts/evaluate_vars_dir_dict.py`
- **Command used**: `python3 /root/projects/Spark_3_5_0/CascadeProjects/windsurf-project/spark-3-5-0/evaluations/scripts/evaluate_vars_dir_dict.py`

## Raw Output
```
================================================================================
============================ 1. WITHOUT PARAMETERS =============================
================================================================================

vars() without parameters:
{'SampleClass': <class '__main__.SampleClass'>,
 '__annotations__': {},
 '__builtins__': <module 'builtins' (built-in)>,
 '__cached__': None,
 '__doc__': '\n'
            'Evaluation script to compare the behavior of vars(), dir(), and '
            '__dict__ in Python\n'
            'when used in different contexts:\n'
            '1. Without parameters\n'
            '2. With a class\n'
            '3. With an object instance\n',
 '__file__': '/root/projects/Spark_3_5_0/CascadeProjects/windsurf-project/spark-3-5-0/evaluations/scripts/evaluate_vars_dir_dict.py',
 '__loader__': <_frozen_importlib_external.SourceFileLoader object at 0x7fa8d0693e30>,
 '__name__': '__main__',
 '__package__': None,
 '__spec__': None,
 'pprint': <module 'pprint' from '/usr/lib/python3.12/pprint.py'>,
 'print_section': <function print_section at 0x7fa8d0464f40>,
 'result': <Recursion on dict with id=140363027847936>,
 'sample_obj': <__main__.SampleClass object at 0x7fa8d047e0f0>}

dir() without parameters:
['SampleClass',
 '__annotations__',
 '__builtins__',
 '__cached__',
 '__doc__',
 '__file__',
 '__loader__',
 '__name__',
 '__package__',
 '__spec__',
 'pprint',
 'print_section',
 'result',
 'sample_obj']

__dict__ without parameters:
Error: name '__dict__' is not defined

================================================================================
=============================== 2. WITH A CLASS ================================
================================================================================

vars(SampleClass):
mappingproxy({'__dict__': <attribute '__dict__' of 'SampleClass' objects>,
              '__doc__': None,
              '__init__': <function SampleClass.__init__ at 0x7fa8d03f0860>,
              '__module__': '__main__',
              '__weakref__': <attribute '__weakref__' of 'SampleClass' objects>,
              'class_method': <classmethod(<function SampleClass.class_method at 0x7fa8d03f09a0>)>,
              'class_var': 'I am a class variable',
              'method': <function SampleClass.method at 0x7fa8d03f0900>,
              'prop': <property object at 0x7fa8d0453600>,
              'static_method': <staticmethod(<function SampleClass.static_method at 0x7fa8d03f0a40>)>})

dir(SampleClass):
['__class__',
 '__delattr__',
 '__dict__',
 '__dir__',
 '__doc__',
 '__eq__',
 '__format__',
 '__ge__',
 '__getattribute__',
 '__getstate__',
 '__gt__',
 '__hash__',
 '__init__',
 '__init_subclass__',
 '__le__',
 '__lt__',
 '__module__',
 '__ne__',
 '__new__',
 '__reduce__',
 '__reduce_ex__',
 '__repr__',
 '__setattr__',
 '__sizeof__',
 '__str__',
 '__subclasshook__',
 '__weakref__',
 'class_method',
 'class_var',
 'method',
 'prop',
 'static_method']

SampleClass.__dict__:
mappingproxy({'__dict__': <attribute '__dict__' of 'SampleClass' objects>,
              '__doc__': None,
              '__init__': <function SampleClass.__init__ at 0x7fa8d03f0860>,
              '__module__': '__main__',
              '__weakref__': <attribute '__weakref__' of 'SampleClass' objects>,
              'class_method': <classmethod(<function SampleClass.class_method at 0x7fa8d03f09a0>)>,
              'class_var': 'I am a class variable',
              'method': <function SampleClass.method at 0x7fa8d03f0900>,
              'prop': <property object at 0x7fa8d0453600>,
              'static_method': <staticmethod(<function SampleClass.static_method at 0x7fa8d03f0a40>)>})

================================================================================
========================== 3. WITH AN OBJECT INSTANCE ==========================
================================================================================

vars(sample_obj):
{'_private_var': 'I am a private instance variable',
 'dynamic_attr': 'I am a dynamically added attribute',
 'instance_var': 'I am an instance variable'}

dir(sample_obj):
['__class__',
 '__delattr__',
 '__dict__',
 '__dir__',
 '__doc__',
 '__eq__',
 '__format__',
 '__ge__',
 '__getattribute__',
 '__getstate__',
 '__gt__',
 '__hash__',
 '__init__',
 '__init_subclass__',
 '__le__',
 '__lt__',
 '__module__',
 '__ne__',
 '__new__',
 '__reduce__',
 '__reduce_ex__',
 '__repr__',
 '__setattr__',
 '__sizeof__',
 '__str__',
 '__subclasshook__',
 '__weakref__',
 '_private_var',
 'class_method',
 'class_var',
 'dynamic_attr',
 'instance_var',
 'method',
 'prop',
 'static_method']

sample_obj.__dict__:
{'_private_var': 'I am a private instance variable',
 'dynamic_attr': 'I am a dynamically added attribute',
 'instance_var': 'I am an instance variable'}

================================================================================
========================== 4. SUMMARY OF DIFFERENCES ===========================
================================================================================


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
```

## Detailed Observations

### 1. Without Parameters

#### `vars()` without parameters
- Returns the local symbol table as a dictionary
- Contains all variables defined in the current scope
- Includes imported modules, defined functions, classes, and variables
- Shows memory addresses for objects and functions
- Contains special module-level variables like `__name__`, `__doc__`, etc.

#### `dir()` without parameters
- Returns a sorted list of names in the current scope
- Similar content to `vars()` but only shows the names, not the values
- More concise than `vars()` output
- Useful for quick inspection of available names

#### `__dict__` without parameters
- Produces an error: `name '__dict__' is not defined`
- This demonstrates that `__dict__` is not a standalone built-in function or variable
- It only exists as an attribute of objects, classes, or modules

### 2. With a Class

#### `vars(SampleClass)`
- Returns a `mappingproxy` object (a read-only dictionary view) of the class namespace
- Contains all attributes defined directly in the class
- Includes class variables, methods, and special attributes
- Shows `class_var`, `method`, `class_method`, `static_method`, and `prop`
- Also contains Python's internal attributes like `__module__`, `__doc__`, etc.

#### `dir(SampleClass)`
- Returns a sorted list of all valid attributes for the class
- More comprehensive than `vars(SampleClass)` as it includes inherited attributes
- Contains all the attributes from `vars(SampleClass)` plus inherited ones from `object`
- Shows dunder methods like `__eq__`, `__hash__`, etc. that weren't directly defined
- Useful for seeing everything available to the class, including inherited functionality

#### `SampleClass.__dict__`
- Identical to `vars(SampleClass)` in content
- Returns a `mappingproxy` object with the class namespace
- Contains only attributes defined directly in the class, not inherited ones
- Demonstrates that `vars(SampleClass)` is essentially a wrapper for `SampleClass.__dict__`

### 3. With an Object Instance

#### `vars(sample_obj)`
- Returns a regular dictionary (not a mappingproxy) of instance attributes
- Contains only instance attributes: `instance_var`, `_private_var`, and `dynamic_attr`
- Does not include class variables or methods
- Includes dynamically added attributes (`dynamic_attr`)
- Demonstrates that `vars()` with an object returns only instance-specific data

#### `dir(sample_obj)`
- Returns a comprehensive list of all attributes available to the instance
- Includes instance attributes, class attributes, methods, and inherited attributes
- Much more extensive than `vars(sample_obj)`
- Shows everything that can be accessed through the instance
- Lists both the instance-specific attributes and everything from the class

#### `sample_obj.__dict__`
- Identical to `vars(sample_obj)` in content
- Contains only instance attributes, not class attributes or methods
- Shows that `vars(sample_obj)` is essentially a wrapper for `sample_obj.__dict__`
- Demonstrates that `__dict__` on an instance only contains instance-specific data

## Overall Findings

### Key Differences

1. **Scope and Content**:
   - `vars()`: Returns dictionaries of attributes and their values
   - `dir()`: Returns lists of attribute names only
   - `__dict__`: Is an attribute itself that contains a dictionary of attributes

2. **Inheritance Handling**:
   - `vars()` and `__dict__`: Only show directly defined attributes
   - `dir()`: Shows all attributes including inherited ones

3. **Return Type**:
   - `vars(class)` and `class.__dict__`: Return a `mappingproxy` (read-only view)
   - `vars(object)` and `object.__dict__`: Return a regular dictionary
   - `dir()`: Always returns a list of strings

4. **Availability**:
   - `vars()` and `dir()`: Can be called without parameters
   - `__dict__`: Only exists as an attribute of objects, classes, or modules

5. **Comprehensiveness**:
   - `dir()`: Most comprehensive, showing all accessible attributes
   - `vars()` and `__dict__`: More focused, showing only directly defined attributes

### Practical Applications

1. **Debugging and Introspection**:
   - `dir()`: Best for quickly exploring what's available
   - `vars()`: Useful for seeing values along with names
   - `__dict__`: Helpful for direct dictionary manipulation of attributes

2. **Metaprogramming**:
   - `vars()` and `__dict__`: Allow for runtime modification of attributes
   - `dir()`: Better for comprehensive inspection

3. **Learning and Exploration**:
   - `dir()`: Excellent for discovering all available methods/attributes
   - `vars()`: Better for understanding the actual values

### Next Steps

For further exploration:
1. Examine how these functions behave with modules
2. Investigate how they interact with inheritance hierarchies
3. Study their behavior with custom `__slots__` implementations, which affect `__dict__`
4. Explore their performance characteristics with large classes/objects
