import java.io.File
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col


object nbs {

  def main (args:Array[String]) {



    val warehouseLocation = new File("hive-warehouse").getAbsolutePath
    println(warehouseLocation)

    //creating the spark session on local mode
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Wiki Datapipeline")
      .enableHiveSupport()
      .getOrCreate()

    // schema for the raw data
    val customSchema = StructType(Array(
      StructField("language", StringType, true),
      StructField("Page_Name", StringType, true),
      StructField("non_unique_views", StringType, true),
      StructField("bytes_transferred", StringType, true)))


    //please enter the path for source directory
    val datasourcePath = "/home/cloudera/Desktop/ETLDATA/"
    //please enter the path for the result csv file
    val resultPath = "/home/cloudera/Desktop/"
    //please enter the path for ISO data file which is attached along with the submission
    val isoPath = "/home/cloudera/Desktop/data/*"


    // enter the year,month,date,hour you wnat to read and query
    val dor  =  readLine("enter yyyymmdd-hhmmss> ")/*"20130101-000000"*/
    def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
      dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
      }
    }

    // function to check if the file exists
    def fileExists(fpath:String, y:String , h:String ) : List[String] = {
      if( new java.io.File(fpath).exists ){
        println("date file exists");
          return List(fpath,y,h)
      } else {
        var tdor  =  readLine("enter yyyymmdd-hhmmss> ")
        var tymd = tdor.slice(0, 7)
        var thms = tdor.slice(9, 14)
        var tpath = datasourcePath+"pagecounts-"+ tdor+".gz"
        println(tpath);
        fileExists(tpath,tymd,thms)
      }
    }

    val okFileExtensions = List("txt","gz")
    print(dor)
    //val files = getListOfFiles(new File("datasourcePath"), okFileExtensions)
    //print(files)
    val fileFullPath = datasourcePath+"pagecounts-"+ dor+".gz"
    val t_ymd = dor.slice(0, 7)
    val t_hms = dor.slice(9, 14)
    val fList = fileExists(fileFullPath,t_ymd,t_hms )
    val fExistsPath = fList (0)
    print(fExistsPath)
    val yearMonthDate = fList (1)
    val hourMinSec = fList (2)
    val year = yearMonthDate.slice(0, 3)
    val month = yearMonthDate.slice(4, 5)
    val day = yearMonthDate.slice(6, 7)
    val hour = hourMinSec.slice(0, 1)

    //dataframe to read the raw data
    val readWikiData = spark.read.format("text")
      .option("header", "false")
      .load(fExistsPath)

    //count of raw dat
    val rawCount = readWikiData.count().toInt

    //raw data is split based on space delimiter
    val wikiCol = readWikiData.withColumn("_tmp", split(col("value"), " ")).select(col("_tmp").getItem(0).as("LangCat"),
      col("_tmp").getItem(1).as("page"),
      col("_tmp").getItem(2).as("views"),
        col("_tmp").getItem(2).as("bytes")
    ).drop("_tmp")

    //the first column in the dataframe is split on '.' delimiter
    val wikiColFinal = wikiCol.withColumn("_tmp", split(col("LangCat"), "\\.")).select(
      col("_tmp").getItem(0).as("Lang"),
      col("_tmp").getItem(1).as("cat"),
      col("page"),
      col("views"),
      col("bytes")
    ).drop("_tmp")

    // creating temp table from the final datafram with 5 columns
    wikiColFinal.createOrReplaceTempView("STAGE_1_WIKI")

    //reading iso data into the dataframe and creating temp table iso_data
    val readISOData = spark.read.format("csv").option("header", "true").load(isoPath)
    readISOData.createOrReplaceTempView("ISO_DATA")

    // inserting the data from the temp table to hive wiki database raw table
    spark.sql("insert overwrite table wiki.wiki_stg  " +
      "SELECT *, "+yearMonthDate+" as ymd, "+hourMinSec+" as hour "+
      "FROM stage_1_wiki ")

    //inserting data to ods_join tbl by joining in the raw table and iso table and also creating category column based on cat
    spark.sql("insert into table wiki.wiki_ods_join "+
      "Select  wiki_stg.page_title, "+
      "wiki_stg.non_unique_views, wiki_stg.bytes_transferred, ISO_DATA.language , CASE "+
      "WHEN wiki_stg.category is null  THEN 'wikipedia' "+
      "WHEN wiki_stg.category = 'b' THEN 'wikibooks' "+
      "WHEN wiki_stg.category = 'd' THEN 'wiktionary' "+
      "WHEN wiki_stg.category = 'f' THEN 'wikimediafoundation' "+
      "WHEN wiki_stg.category = 'm' THEN 'wikimedia' "+
      "WHEN wiki_stg.category = 'n' THEN 'wikinews' "+
      "WHEN wiki_stg.category = 'q' THEN 'wikiquote' "+
      "WHEN wiki_stg.category = 's' THEN 'wikisource' "+
      "WHEN wiki_stg.category = 'v' THEN 'wikiversity' "+
      "WHEN wiki_stg.category = 'voy' THEN 'wikivoyage' "+
      "WHEN wiki_stg.category = 'w' THEN 'mediawiki' "+
      "WHEN wiki_stg.category = 'wd' THEN 'wikidata' "+
      "ELSE NULL END AS Cat, SUBSTR(wiki_stg.ymd,1,4) as year, SUBSTR(wiki_stg.ymd,5,2) as month, "+
      "SUBSTR(wiki_stg.ymd,7,2) as day, SUBSTR(wiki_stg.hour,1,2) as hour "+
      "from wiki.wiki_stg "+
      "JOIN ISO_DATA "+
      "ON (ISO_DATA.code = wiki_stg.langcode) "+
      "where "+
      "wiki_stg.category not like '%mw%' "+
      "and wiki_stg.page_title not like '%:%' ")


    // inserting the preprocessed data into the final partition table
    spark.sql("INSERT into TABLE wiki.wiki_final PARTITION(year,month,day,hour)"+
      "SELECT * from  wiki.wiki_ods_join")


    //count of raw dat
    val finalDF = spark.sql("select count(*) as c from wiki.wiki_final").select("c").first().mkString(" ")
    val finalCount = readWikiData.count().toInt

    println("the difference in the row count between the raw data and preprocessed and tranformed data is:")
    val cnt = rawCount - finalCount
    println(cnt)

    // query that yeilds the top 10 pages viewed in each language
    val resultDF = spark.sql( "select t1.* from "+
      "( select T.*, row_number() over(partition by T.language order by T.view_count desc) as rn "+
      "from ( "+
      "SELECT language , page_title  ,sum(views) as view_count FROM wiki.wiki_final "+
      "where year = "+year+" and month = "+month+" and day = "+day+" and hour = "+hour+" "+
      "group by language ,  page_title  )T "+
      " ) T1 "+
      " WHERE T1.rn <= 10" )

    // saving the dataframe to single csv file
    resultDF.coalesce(1).write.format("csv").save(resultPath+"result1.csv")


    // query that yeilds the percentage viewed for each project category
    val totalVwDF = spark.sql("select sum(views) as vw FROM wiki.wiki_final").select("vw").first().mkString(" ")
    val resultDF2 = spark.sql( "select project, sum(views) as page_hits, "+
      "(sum(views)/ "+totalVwDF+") *100 as percentage_views "+
      "from wiki.wiki_final "+
      "group by project "+
      "order by page_hits desc" )

    // saving the dataframe to single csv file
    resultDF2.coalesce(1).write.format("csv").save(resultPath+"result2.csv")

  }


}

