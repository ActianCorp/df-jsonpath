# df-jsonpath

The JSONPath Runner is an Actian DataFlow operator for running JSONPath expressions on JSON data in record flows.  This implementation uses the [Jayway JsonPath 2.0](https://github.com/jayway/JsonPath) engine.

## Configuration

Before building df-jsonpath you need to define the following environment variables to point to the local DataFlow update site [dataflow-p2-site](https://github.com/ActianCorp/dataflow-p2-site) root directory and the DataFlow version.

    export DATAFLOW_REPO_HOME=/Users/myuser/dataflow-p2-site
    export DATAFLOW_VER=6.5.2.112

## Building

The update site is built using [Apache Maven 3.0.5 or later](http://maven.apache.org/).

To build, run:

    mvn clean install
    
You can update the version number by running

    mvn org.eclipse.tycho:tycho-versions-plugin:set-version -DnewVersion=version
    
where version is of the form x.y.z or x.y.z-SNAPSHOT.

## Using the JSONPath Runner with the DataFlow Engine

The build generates a JAR file in the target directory under
[df-jsonpath/DataFlowExtensions](https://github.com/ActianCorp/df-jsonpath/tree/master/DataFlowExtensions)
with a name similar to 

    jsonpath-dataflow-operator-1.y.z.jar

which can be included on the classpath when using the DataFlow engine.

## Installing the JSONPath Runner plug-in in KNIME

The build also produces a ZIP file which can be used as an archive file with the KNIME 'Help/Install New Software...' dialog.
The ZIP file can be found in the target directory under
[df-jsonpath/KnimeExtensions/Knime-Update-Site](https://github.com/ActianCorp/df-jsonpath/tree/master/KnimeExtensions/Knime-Update-Site) 
and with a name like 


    com.actian.services.knime.jsonpath.update-1.y.z.zip

The file [examples/KNIME/JSONPath_Runner_Example.zip](https://github.com/ActianCorp/df-jsonpath/raw/master/examples/KNIME/JSONPath_Twitter_Example.zip) 
contains a KNIME workflow that can be imported into KNIME and used to test the plug-in.



