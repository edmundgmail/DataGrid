http://192.168.56.102:8082/runner


Do a SparkSQL
{
    sessionKey:"123",
    parameter: {
        "className" : "com.ddp.access.QueryParameter",
        "sql":"select MYFUNC('show tables')"
    }
}


{
    sessionKey:"123",
    parameter: {
      "className" : "com.ddp.access.CopybookIngestionParameter",
      "cpyBookName":"RPWACT",
      "cpyBookHdfsPath":"/tmp/LRPWSACT.cpy",
      "fileStructure":"FixedLength",
      "binaryFormat": "FMT_MAINFRAME",
      "splitOptoin": "SplitNone",
      "dataFileHdfsPath":"/tmp/RPWACT.FIXED.END",
      "cpybookFont":"cp037"
    }
}

{
    sessionKey:"123",
    parameter: {
      "className" : "com.ddp.access.ScalaSourceParameter",
      "srcHdfsPath":"/tmp/apps/TestApp.scala"
    }
}

{
    sessionKey:"123",
    parameter: {
      "className" : "com.ddp.access.UserClassParameter",
      "userClassName":"user.TestApp"
    }
}


to test postJars
$curl -v -F upload=@PiJob.jar localhost:8082/postJars

{
    sessionKey:"123",
    parameter: {
      "className" : "com.ddp.access.csvIngestionParameter",
      "filePath":"hdfs://quickstart.cloudera:8020/tmp/cars.csv",
      "tableName":"Cars",
      "Schema" : "cif.Cars"
    }
}

{
    sessionKey:"123",
    needPadding:true,
    parameter: {
      "className" : "com.ddp.access.csvIngestionParameter",
      "filePath":"hdfs://quickstart.cloudera:8020/tmp/cars.csv",
      "tableName":"Cars",
      "templateTableName":"WDS_DEV.financial_summary",
      "schema" : ""
    }
}

{
    sessionKey:"123",
    parameter: {
      "className" : "com.ddp.access.xmlIngestionParameter",
      "filePath":"hdfs://quickstart.cloudera:8020/tmp/books.xml",
      "tableName":"Books",
      "Schema" : "cif.Cars"
    }
}

http://localhost:8082/postUserFunctionHierarchy
{
    "sessionKey":"123",
    "needPadding":false,
    "parameter": {
      "className" : "com.ddp.access.UserScriptParameter",
      "action":"add",
      "level":"owner",
      "name":"guoe3",
      "id":-1,
      "parentId": 0,
      "content": ""
    }
}

