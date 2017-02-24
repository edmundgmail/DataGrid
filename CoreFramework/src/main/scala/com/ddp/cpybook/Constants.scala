package com.ddp.cpybook

/**
  * Created by cloudera on 8/28/16.
  */
object Constants {
  val CopybookName = "copyboook.name"
  val CopybookHdfsPath = "copyboook.hdfs.path"
  val DataFileHdfsPath = "datafile.hdfs.path"
  val CopybookSplitOpiton = "copybook.split.option"

  object CopybookSplitOptionValues extends Enumeration {
    val SplitNone = Value(0)
    val SplitRedefine = Value(1)
    val Split01Level= Value(2)
    val SplitHighestRepeating = Value(3)
  }

  val CopybookFileStructure = "copybook.file.structure"

  object CopybookFileStructureValues extends Enumeration{
    val FixedLength = Value(2)
  }

   val CopybookBinaryformat = "copybook.binary.format"

  object CopybookBinaryformatValues extends Enumeration {
    val FMT_MAINFRAME=Value(1)
    //val FMT_INTEL, FMT_MAINFRAME, FMT_FUJITSU , FMT_BIG_ENDIAN, FMT_OPEN_COBOL, FMT_FS2000,  FMT_OPEN_COBOL_MVS, FMT_OC_MICRO_FOCUS, FMT_OPEN_COBOL_BE,
     //FMT_FS2000_BE, FMT_OPEN_COBOL_MVS_BE, FMT_OC_MICRO_FOCUS_BE, FMT_MICRO_FOCUS, FMT_MAINFRAME_COMMA_DECIMAL,  FMT_FUJITSU_COMMA_DECIMAL = Value
  }

  val CopybookFont = "copybook.font"
  object CopybookFontValues extends Enumeration{
    val cp037 = Value("cp037")
  }


  object CopybookDataTypeValues extends  Enumeration{
    val Char = Value(0)
    val CharRightJust = Value(1)
    val CharNullTerminated = Value(2)
    val CharNullPadded = Value(3)
    val Hex = (4)
    val NumLeftJustified = (5)
    val NumRightJustified = (6)
    val NumZeroPadded = (7)
    val AssumedDecimal = (8)
    val SignSeparateLead = (9)
    val SignSeparateTrail = (10)
    val SignSepLeadActualDecimal = (44)
    val SignSepTrailActualDecimal = (45)
    val Decimal = (11)
    val BinaryInt = (15)
    val PostiveBinaryInt = (16)
    val Float = (17)
    val Double = (18)
    val NumAnyDecimal = (19)
    val PositiveNumAnyDecimal = (20)
    val Bit = (21)
    val AssumedDecimalPositive = (22)
    val BinaryIntPositive = (23)
    val NumZeroPaddedPN = (24)
    val NumZeroPaddedPositive = (25)
    val NumCommaDecimal = (26)
    val NumCommaDecimalPN = (27)
    val NumCommaDecimalPositive = (28)
    val NumRightJustifiedPN = (29)
    val PackedDecimal = (31)
    val ZonedNumeric = (32)
    val PackedDecimalPostive = (33)
    //	public static final int ZonedNumericPositive = (34;
    val BinaryBigEndian = (35)
    val BinaryBigEndianPositive = (39)
    val PositiveBinaryBigEndian = (36)
    val RmComp = (37)
    val RmCompPositive = (38)
    val FjZonedNumeric = (41)
    val NumRightJustCommaDp = (42)
    val NumRightJustCommaDpPN = (43)
    val GnuCblZonedNumeric = (46)
    /**
      * Use MultiLineChar instead
      */
    @deprecated val CharMultiLine = (51)
    val Date = (71)
    val DateYMD = (72)
    val DateYYMD = (73)
    val DateDMY = (74)
    val DateDMYY = (75)
    val CharRestOfFixedRecord = (80)
    val CharRestOfRecord = (81)
    val CharNoTrim = (82)
    val ProtoField = (91)
    val AvroField = (91)
    val ArrayField = (92)
    val ComboItemField = (93)
    val AvroUnionField = (94)
    val CheckBoxY = (109)
    val CheckBoxTrue = (110)
    val CheckBoxYN = (111)
    val CheckBoxTF = (112)
    val CheckBoxBoolean = (114)
    val CsvArray = (115)
    val XmlNameTag = (116)
    val MultiLineEdit = (117)
    val MultiLineChar = (118)
    /* used in PO / Tip Files */
    val HtmlField = (119)
    val RecordEditorType = (130)
    val NumOrEmpty = (131)
  }
}
