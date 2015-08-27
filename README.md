PIG Custom Loader with Custom InputFormat & Custom RecordReader - Example
----------------------------------------------------------------------

Problem Statement:
------------------
    
data/sample_input_data.txt
   
- content enclosed between START and END tags together forms a record

<b>cat sample_input_data.txt </b>

<pre>

START
i am part of one
i am part of one
STOP START
i am part of two
i am part of two
STOP
START
i am part of three
i am art of three STOP
</pre>

   

Download jar
------------

- Download the jar file from target/pig-custom-loader-0.0.1-SNAPSHOT.jar to Edge Node
  (Node in which pig is running)
  
  
Usage
------


<b>pigscipt.pig</b>

<pre>

 register pig-custom-loader-0.0.1-SNAPSHOT.jar;
 A = LOAD '/home/vijay/pig-custom-input-loader/data.txt' USING  com.company.hadoop.StartStopCustomLoader('START','STOP');
 B = FOREACH A GENERATE FLATTEN(REPLACE($0,'\\n',' '));
 STORE B into './outputDir'; 

</pre>

Launch
-------

pig -x local pigscipt.pig         // for local mode


sample ouput:
-------------
<pre>
ls -l outputDir
cat part-m-00000

START i am part of one i am part of one STOP
START i am part of two i am part of two STOP
START i am part of three i am art of three STOP

</pre>


