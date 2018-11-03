# SparkTools
A few useful transformers, user defined functions, user defined aggregation functions and more for spark.  

# Installation
Feel free to just copy the code you need.  To add as a dependency use our s3 repo or clone the source

## sbt S3 Dependency
Our maven repo is at "s3://s3-us-east-1.amazonaws.com/insilicaresolver" 
1. Add `addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.16.0")` to your `plugins.sbt`
2. Then add the below to your `build.sbt`  
```
resolvers ++= Seq("S3" at "s3://s3-us-east-1.amazonaws.com/insilicaresolver")
libraryDependencies += "co.insilica" %% "sparktools" % "0.6.0"
```

