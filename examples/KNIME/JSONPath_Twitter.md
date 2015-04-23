#JSONPath Twitter Example

This KNIME workflow shows a simple example using the JSONPath Runner component in KNIME using some sample data from a 
Twitter stream.   The workflow extracts the Id, User.name and hashtags referenced from all of the tweets that reference hashtags.

Once you import and load the [KNIME workflow](https://github.com/ActianCorp/df-jsonpath/blob/master/examples/KNIME/JSONPath_Twitter_Example.zip)
 you will see the following workflow on the canvas.   The workflow is configured and ready to run.


![JSONPath Runner Configuration Dialog](https://raw.githubusercontent.com/ActianCorp/df-jsonpath/master/examples/KNIME/JSONPath_Twitter_Example.png)


The configuration dialog shows how the JSONPath expressions are used to map the source tweet to the output record fields.


![JSONPath Runner Configuration Dialog](https://raw.githubusercontent.com/ActianCorp/df-jsonpath/master/examples/KNIME/JSONPath_Mappings.png)


After running the workflow you can see the output data in the KNIME Interactive Table viewer (right mouse and select View: Table View).


![JSONPath Runner Configuration Dialog](https://raw.githubusercontent.com/ActianCorp/df-jsonpath/master/examples/KNIME/JSONPath_Results.png)