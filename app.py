import os
from util import get_spark_session
from read import from_files


def main():
    env = os.environ.get('ENVIRON')
    src_dir = os.environ.get('SRC_DIR')
    file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
    src_file_format = os.environ.get('SRC_FILE_FORMAT')
    print('Testing spark session')
    spark = get_spark_session(env, 'GitHub Activity - Reading Data')
    print(type(spark))
    print(f'{src_dir}/{file_pattern}')
    df = from_files(spark, src_dir, file_pattern, src_file_format)
    df.printSchema()


if __name__ == '__main__':
    main()