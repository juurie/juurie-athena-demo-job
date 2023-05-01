import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
data = [
    ("1101", '{"name" : "한송이", "city" : "Seoul", "gender" : "F"}', {"kor" : 78, "eng" : 87, "mat" : 83, "edp" : 78}),
    ("1102", '{"name" : "정다워", "city" : "Busan", "gender" : "M"}', {"kor" : 88, "eng" : 83, "mat" : 57, "edp" : 98}),
    ("1103", '{"name" : "그리운", "city" : "Incheon", "gender" : "M"}', {"kor" : 76, "eng" : 56, "mat" : 87, "edp" : 78}),
    ("1104", '{"name" : "고아라", "city" : "Daegu", "gender" : "F"}', {"kor" : 83, "eng" : 57, "mat" : 88, "edp" : 73}),
    ("1105", '{"name" : "사랑해", "city" : "Busan", "gender" : "F"}', {"kor" : 87, "eng" : 87, "mat" : 53, "edp" : 55}),
    ("1106", '{"name" : "튼튼이", "city" : "Gwangju", "gender" : "M"}', {"kor" : 98, "eng" : 97, "mat" : 93, "edp" : 88}),
    ("1107", '{"name" : "한아름", "city" : "Daegu", "gender" : "F"}', {"kor" : 68, "eng" : 67, "mat" : 83, "edp" : 89}),
    ("1108", '{"name" : "더크게", "city" : "Busan", "gender" : "M"}', {"kor" : 98, "eng" : 67, "mat" : 93, "edp" : 78}),
    ("1109", '{"name" : "더높이", "city" : "Seoul", "gender" : "M"}', {"kor" : 88, "eng" : 99, "mat" : 53, "edp" : 88}),
    ("1110", '{"name" : "아리랑", "city" : "Incheon", "gender" : "M"}', {"kor" : 68, "eng" : 79, "mat" : 63, "edp" : 66}),
    ("1111", '{"name" : "한산섬", "city" : "Busan", "gender" : "M"}', {"kor" : 98, "eng" : 89, "mat" : 73, "edp" : 78}),
    ("1112", '{"name" : "하나로", "city" : "Daegu", "gender" : "F"}', {"kor" : 89, "eng" : 97, "mat" : 78, "edp" : 88})
]

schema = ['hakbun', 'info', 'subjects']

df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
df.show(truncate=False)

output_path = 's3a://' + 'juurie-datalake-bucket' + '/output'
df.repartition(1).write.mode('overwrite').json(f'{output_path}/sample-json/')
print('JSON File Upload Successfully.')

job.commit()