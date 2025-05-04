def transform_data():

    from pyspark.sql import SparkSession
    import pyspark.sql.functions as sf
    from pyspark.sql.types import StringType,IntegerType,DateType,StructField,StructType,LongType
    from datetime import datetime
    import emoji
    import re
    



    def read_data():
        
        spark = SparkSession.builder.appName("youtube").getOrCreate()
        current_date = datetime.now().strftime("%Y%m%d")
        output_path = f'/opt/airflow/data/Youtube_Trending_Data_Raw_{current_date}.csv'

        df = spark.read.format("csv").option("header",True).load(output_path)

        return df
    
    def clean_txt(txt:str):
        if txt is not None:
            txt = emoji.demojize(txt,delimiters = ("",""))
                  
            if txt.startswith('#'):
                txt = txt.replace('#','').strip()
            else:
                split_txt = txt.split("#")
                txt = split_txt[0]
            
                txt = txt.replace('\\"',"")
            
            
            return txt.strip()

        return txt 
    
    clean_txt_udf = sf.udf(clean_txt, StringType())


    df = read_data()

    df_cleaned = df.withColumn('title', clean_txt_udf(sf.col('title'))) \
                   .withColumn('channel_title', clean_txt_udf(sf.col('channel_title'))) \
                   .withColumn('published_at', sf.to_date(sf.col('published_at'))) \
                   .withColumn('view_count', sf.col('view_count').cast(LongType())) \
                   .withColumn('like_count', sf.col('like_count').cast(LongType())) \
                   .withColumn('comment_count', sf.col('comment_count').cast(LongType())) \
                   .dropna(subset=['video_id'])
    

     # Generate the filename based on the current date
    current_date = datetime.now().strftime("%Y%m%d")
    output_path = f'/opt/airflow/data/Transformed_Youtube_Data_{current_date}.csv'
    
    # Write cleaned DataFrame to the specified path
    df_cleaned.write.csv(output_path, header=True, mode='overwrite')   


        





