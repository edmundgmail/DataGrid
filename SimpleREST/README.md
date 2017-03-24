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



