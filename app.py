import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import config


def upload_to_s3(filename, bucket, key):
    '''
    Uploads a dataframe to an S3 bucket
    :param filename: File to upload
    :param bucket: Bucket to upload to (the top level directory under AWS S3)
    :param key: S3 object name (can contain subdirectories). If not specified then file_name is use
    :return:
    '''
    import boto3
    try:
        session = boto3.Session(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )
    except Exception as e:
        print(e)
        return False

    try:
        s3 = session.resource('s3')
        s3.meta.client.upload_file(Filename=filename, Bucket=bucket,
                                   Key=key)
    except Exception as e:
        print(e)
        return False
    return True


def read_dataset_parquet(spark, filename):
    '''
    Reads a parquet file into a dataframe
    :param spark: SparkSession
    :param filename: File to read
    :return: Dataframe
    '''
    return spark.read.parquet(filename)


def transform_timestamp(df, col_name):
    '''
    Transforms a timestamp column into a date column
    :param df:
    :param col_name:
    :return:
    '''
    return df.withColumn(col_name, (col(col_name).cast("timestamp")))


def write_csv_file(df, file_name):
    '''
    Writes a dataframe to a csv file
    :param df: Dataframe to write
    :param file_name: File to write
    :return: True if successful, False otherwise
    '''
    try:
        df.write.csv(f'saida_csv/{file_name}.csv', mode='overwrite', sep=';', header=True, encoding='utf-8')
    except Exception as e:
        print(e)
        return False
    return True


def pipeline(spark, filename):
    '''
    Pipeline to read a parquet file and upload it to S3
    :param spark: SparkSession
    :param filename: File to read
    :return: True if successful, False otherwise
    '''

    df = read_dataset_parquet(spark, filename)
    if df is None:
        return False

    # transform_timestamp

    df = transform_timestamp(df, 'Premiere')
    df = transform_timestamp(df, 'dt_inclusao')
    print(df.printSchema())
    # se type df.premiere == timestamp return true
    # else return false

    if not dict(df.dtypes)['dt_inclusao'] == 'timestamp' and dict(df.dtypes)['Premiere'] == 'timestamp':
        return False

    # Ordenar os dados por ativos e gênero de forma decrescente, 0 = inativo e 1 = ativo, todos com número 1 devem aparecer primeiro.
    df = df.sort(col("Active").desc(), col("Genre").desc())

    # Remover linhas duplicadas e trocar o resultado das linhas que tiverem a coluna "Seasons" de "TBA" para "a ser anunciado".
    print(f'Dataframe before drop duplicates: {df.toPandas().shape}')
    df = df.dropDuplicates()
    print(f'Dataframe after drop duplicates {df.toPandas().shape}')
    df = df.withColumn("Seasons", when(col("Seasons") == "TBA", "a ser anunciado").otherwise(col("Seasons")))
    print(f'Number rows "A ser anunciado:" {df.filter(col("Seasons") == "a ser anunciado").count()}')

    rename_dict = {
        'Title': 'Título',
        'Genre': 'Gênero',
        'GenreLabels': 'Labels de Gênero',
        'Premiere': 'Data de Lançamento',
        'Seasons': 'Temporada',
        'SeasonsParsed': 'Temporadas Parsadas',
        'EpisodesParsed': 'Episódios Parsados',
        'Length': 'Duração',
        'MinLength': 'Duração Mínima',
        'MaxLength': 'Duração Máxima',
        'Status': 'Status',
        'Active': 'Ativo',
        'Table': 'Tabela',
        'Language': 'Idioma',
        'dt_inclusao': 'Data de Inclusão'
    }

    for key, value in rename_dict.items():
        df = df.withColumnRenamed(key, value)

    # Criar uma coluna nova chamada "Data de Alteração" e dentro dela um timestamp.
    df = df.withColumn("Data de Alteração", (col("Data de Inclusão").cast("timestamp")))
    if not dict(df.dtypes)['Data de Alteração'] == 'timestamp':
        return False

    file_name_csv = 'Netflix - Python'
    if not write_csv_file(df, file_name_csv):
        return False

    if upload_to_s3(file_name_csv, config.s3_bucket, config.s3_key) is False:
        return False
    return True


if __name__ == '__main__':
    #####
    # Execute the pipeline and upload the results to S3
    #####

    spark = SparkSession.builder \
        .master('local') \
        .appName(config.app_name) \
        .config(config.config_memory, config.memory) \
        .config(config.config_cores, config.cores) \
        .getOrCreate()

    print('Pipeline result: {}'.format(pipeline(spark, config.file_path)))
    spark.stop()
    print('Spark Stopped')
    exit(0)
