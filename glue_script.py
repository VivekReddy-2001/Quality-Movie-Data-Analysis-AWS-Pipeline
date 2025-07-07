import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node movies_data_from_s3
movies_data_from_s3_node1751730585612 = glueContext.create_dynamic_frame.from_catalog(database="movies_db", table_name="input", transformation_ctx="movies_data_from_s3_node1751730585612")

# Script generated for node Apply_Data_Quality_checks
Apply_Data_Quality_checks_node1751730874792_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        RowCount > 0,
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.5 and 10.3
    ]
"""

Apply_Data_Quality_checks_node1751730874792 = EvaluateDataQuality().process_rows(frame=movies_data_from_s3_node1751730585612, ruleset=Apply_Data_Quality_checks_node1751730874792_ruleset, publishing_options={"dataQualityEvaluationContext": "Apply_Data_Quality_checks_node1751730874792", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1751731879019 = SelectFromCollection.apply(dfc=Apply_Data_Quality_checks_node1751730874792, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1751731879019")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1751732285371 = SelectFromCollection.apply(dfc=Apply_Data_Quality_checks_node1751730874792, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1751732285371")

# Script generated for node Check_For_Pass_Fail
Check_For_Pass_Fail_node1751734082842 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1751732285371,
  group_filters = [GroupFilter(name = "bad_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node bad_records
bad_records_node1751734086091 = SelectFromCollection.apply(dfc=Check_For_Pass_Fail_node1751734082842, key="bad_records", transformation_ctx="bad_records_node1751734086091")

# Script generated for node default_group
default_group_node1751734085677 = SelectFromCollection.apply(dfc=Check_For_Pass_Fail_node1751734082842, key="default_group", transformation_ctx="default_group_node1751734085677")

# Script generated for node Drop_Columns
Drop_Columns_node1751734842439 = ApplyMapping.apply(frame=default_group_node1751734085677, mappings=[("poster_link", "string", "poster_link", "string"), ("series_title", "string", "series_title", "string"), ("released_year", "string", "released_year", "string"), ("certificate", "string", "certificate", "string"), ("runtime", "string", "runtime", "string"), ("genre", "string", "genre", "string"), ("imdb_rating", "double", "imdb_rating", "double"), ("overview", "string", "overview", "string"), ("meta_score", "long", "meta_score", "long"), ("director", "string", "director", "string"), ("star1", "string", "star1", "string"), ("star2", "string", "star2", "string"), ("star3", "string", "star3", "string"), ("star4", "string", "star4", "string"), ("no_of_votes", "long", "no_of_votes", "long"), ("gross", "string", "gross", "string")], transformation_ctx="Drop_Columns_node1751734842439")

# Script generated for node Rules_outcome_to_s3
Rules_outcome_to_s3_node1751733678793 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1751731879019, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-gds-project/rules_outcome/", "partitionKeys": []}, transformation_ctx="Rules_outcome_to_s3_node1751733678793")

# Script generated for node bad_records_in_s3
bad_records_in_s3_node1751734602948 = glueContext.write_dynamic_frame.from_options(frame=bad_records_node1751734086091, connection_type="s3", format="json", connection_options={"path": "s3://movies-data-gds-project/rule_failed_records/", "partitionKeys": []}, transformation_ctx="bad_records_in_s3_node1751734602948")

# Script generated for node Write_In_Redshift
Write_In_Redshift_node1751735083328 = glueContext.write_dynamic_frame.from_catalog(frame=Drop_Columns_node1751734842439, database="movies_db", table_name="dev_movies_imdb_movies_rating", redshift_tmp_dir="s3://movies-data-gds-project",additional_options={"aws_iam_role": "arn:aws:iam::616033937939:role/service-role/AmazonRedshift-CommandsAccessRole-20250626T102930"}, transformation_ctx="Write_In_Redshift_node1751735083328")

job.commit()