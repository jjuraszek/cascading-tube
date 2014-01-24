cascading-tube
==============
### *patterns for processing* ###

Tiny wrapper around hadoop cascading make it easy to chaining operations. All written in scala but it is not hiding the the cascading framework. You can always fallback to cascading style of defining flow if you felt you missing something in tube. Tube is using maps for providing any custom operations (the plane is to use also PartialFunctions) and because of that there is no limit of number of input columns for neither of that operations. 

**I am looking anybody who wants to help in developing this lib.**

## Motivation ##

Providing clean and not so complicated in use and debug wrapper which can be incorporated in your current hadoop processing project.
Writing MR hadoop jobs in tube is going to be more functional but in the other hand it will never hide you from cascading Pipe class.

## Roadmap ##
- **2.0.x [DONE]** Divide project into submodules:
  - **tube-testing** Containing class simplify testing of your flows by providing MemoryTap and boilerplate for running it within unit testing scope. Provide samples as unit test of tube.
  - **tube-core** Tube object and every operators refactored to be in separated files accordingly to logic purpose.
  - **tube-io** Place for SQL sink tap allowing major use case of presenting MR results to more responsive layer. Module for standard CSV tap builders and real flow runner using JobConf.
- **3.0.x [DONE]** Provide builders flow transformation
  - for joins and coGroups like ops including list of output fields.
  - for buffers, functions and filters.
  - deprecation of _currying_ style of those functions.
- **3.1.x** Support new operations from *cascading 2.5.x* like BufferJoin and more and operating on JSON inside any field of TupleEntry and returning list of Tuples (and List of Lists) as vectors of transformation
- **4.0.x** Removed deprecation functions and small improvement will be added
- **4.1.x** Updated road map and every libs dependencies
- **5.0.x** Support buffers, filters and functions to be defined as PartialFunction (no need for Map-s all over the place but number of input fields will be 22)

Despite of that **cascading-tube.testing** will be constantly extended to show the capability of tube.

## Sample use case ##
**SEE the tube-testing for examples**

## Design insight ##
* ```package object tube``` provide functions of implicit conversions over ```cascading.tuple.{TupleEntry, Fields, Tuple}```
* ```jj.tube.Tube``` contains main wrapping object allowing chaining of pipe transformation. Look there for detailed list of available transformations. Most of them are aliases to native cascading ops.
* ```jj.tube.CustomOps``` is helper object providing easy way to: 
  + construct Aggregators (for ```Tube.aggregateBy``` transformation)
  + transform closures to Buffers, Filters and Functions

## Repository, maven and supported versions ##
Maven repo: 
```
<repository>
  <id>conjars.org</id>
  <url>http://conjars.org/repo</url>
</repository>
...
<dependency>
  <groupId>org.tube</groupId>
  <artifactId>tube-core</artifactId>
  <!--current version defined in build.gradle -->
  <version>2.0.0</version>
</dependency>
<dependency>
  <groupId>org.tube</groupId>
  <artifactId>tube-io</artifactId>
  <!--current version defined in build.gradle -->
  <version>2.0.0</version>
</dependency>
<dependency>
  <groupId>org.tube</groupId>
  <artifactId>tube-testing</artifactId>
  <!--current version defined in build.gradle -->
  <version>2.0.0</version>
</dependency>
```

Supported versions of scala, hadoop and cascading:
* org.scala-lang:scala-library:2.10.3
* cascading:cascading-core:2.5.1

You can compile your own dependencies setup. Replacing only hadoop-client lib should be notimer because tube is not using it directly (scala compiler need it).

## License MIT ##
Copyright (C) 2013 Jacek Juraszek

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

