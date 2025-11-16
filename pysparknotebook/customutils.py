from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

class transformations:

    def dedup(self,df:DataFrame,dedup_cols:List,cdc:str):
        
        df = df.withColumn('dedupkey',concat(*dedup_cols))

        df = df.withColumn('dedupcounts',row_number().over(Window.partitionBy('dedupkey').orderBy('cdc')))

        df = df.filter(col('dedupcounts')==1)
        df = df.drop('dedupkey','dedupcounts')

