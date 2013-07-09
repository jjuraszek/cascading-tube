cascading-tube
==============
### *patterns for processing* ###

Tiny wrapper around hadoop cascading make it easy to chaining operations. All written in scala but it is not hiding the the cascading framework.
You can always fallback to cascading style of defining flow if you felt you missing something in tube.

**I am looking anybody who wants to help in developing this lib.**

## Motivation ##

Providing clean and not so complicated in use and debug wrapper which can be incorporated in your current hadoop processing project.
Writing MR hadoop jobs in tube is going to be more functional but in the other hand it will never hide you from cascading Pipe class.

## Sample use case ##

import jj.tube._
import jj.tube.CustomOps._
import jj.tube.Tube._

```
val popularFilms = Tube("allFilms")
  .filter("country"){ 
    //filter out box office results from usa
    _ == "usa"
  }
  .aggregateBy("title",sumInt("views"), sum("income"))
```

Now you can use that tube as you use cascading Pipe. Most basic operators you can find in javadoc. Look for content of ```jj.tube._``` package.

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
  <artifactId>cascading-tube</artifactId>
  <!--current version defined in build.gradle -->
  <version>1.0</version>
</dependency>
```

Supported versions of scala, hadoop and cascading:
* org.scala-lang:scala-library:2.10.2
* cascading:cascading-core:2.1.6

You can compile your own dependencies setup. Replacing only hadoop-client lib should be notimer because tube is not using it directly (scala compiler need it).

## License MIT ##
Copyright (C) 2013 Jacek Juraszek

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

