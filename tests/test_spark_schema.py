import sys
from datetime import datetime
from math import pi

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DecimalType, TimestampType, Row, \
    StringType, BooleanType

from sparkschema.spark_schema import SparkSchema


def test_csv_with_only_strings(spark_session: SparkSession):
    df = spark_session.createDataFrame([('foo',), ('bar',), (None,)], ['str_col'])
    result = SparkSchema().get_schema(df)
    assert result == StructType([StructField('str_col', StringType())])


def test_csv_with_only_longs(spark_session: SparkSession):
    df = spark_session.createDataFrame([[str(sys.maxsize + 1)], [str(-sys.maxsize - 1)], [None]], ['my_col'])
    assert SparkSchema().get_schema(df) == StructType([StructField('my_col', DecimalType(20, 0))])


def test_csv_with_some_floats_some_longs(spark_session: SparkSession):
    df = spark_session.createDataFrame([[str(sys.maxsize + 1)], [str(-sys.maxsize - 1)], ['10.2']], ['my_col'])
    assert SparkSchema().get_schema(df) == StructType([StructField('my_col', DecimalType(21, 1))])


def test_csv_with_only_timestamps(spark_session: SparkSession):
    df = spark_session.createDataFrame([['2010-03-19T00:00:00.000+0000'], ['2010-03-17T00:00:00.000+0000']], ['my_col'])
    assert SparkSchema().get_schema(df) == StructType([StructField('my_col', TimestampType())])


def test_csv_with_only_floats(spark_session: SparkSession):
    df = spark_session.createDataFrame([[str(1 / 3)], [str(pi)]], ['float_col'])
    assert SparkSchema().get_schema(df) == StructType([StructField('float_col', DecimalType(17, 16))])


def test_csv_with_floats_and_bugs_string_value(spark_session: SparkSession):
    df = spark_session.createDataFrame([[str(1 / 3)], [str(pi)], ['bogus']], ['float_col'])
    assert SparkSchema().get_schema(df) == StructType([StructField('float_col', DecimalType(17, 16))])


def test_csv_with_floats_and_strings(spark_session: SparkSession):
    df = spark_session.createDataFrame([[str(1 / 3), 'foo'], [str(pi), 'bar']], ['float_col', 'string_col'])
    df = df.select('string_col', 'float_col')
    assert SparkSchema().get_schema(df) == StructType([StructField('string_col', StringType()),
                                                       StructField('float_col', DecimalType(17, 16))])


def test_full_sample(spark_session):
    data = [Row(ItemID=1, Class=None, ArticleNumber=10001, Brand='Knorr', MatchDescription='bouiltab kip',
                Description='knorr bouiltab kip', ListingSince=datetime(2010, 3, 19, 0, 0).isoformat(),
                ListingUntill=datetime(2015, 2, 22, 0, 0).isoformat(), DiscontinuedArticleNumber=None,
                ReplacementArticleNumber=153454, Active=False, Volume=100.0, VolumeUOM='gr', PackagingCode='pak',
                EANCE=8722700695844, SupplierNumber=11290, SupplierName='Unilever Nederland BV', ArticleType='ZDC',
                VariableWeight=False, CMCode='D28', CMName='KRUID/ETNISCH/SOEP', PresentationGroup='SOEPEN 1', VAT=1,
                PriceRange='A', PriceIndicator=None, SegmentNumber=325, SegmentName='BOUILLON - BLOKJE', MPC=1096991,
                MPCModel=1096991, MPCReferenceArticleNumber=10001, PromoDays=0, PromoStart=None,
                ImageURL='https://someurl.foo', ProductURL=None,
                BaseUnitCode='PAK', PackagingUnitCode='PAK', BaseUnitsPerDeliveryUnit=24, BaseUnitsPerSalesUnit=1,
                AverageWeeklySales=None, PromoType=None, PromoOffer=None, Beheersafdeling='DKW', VolumeBruto='100',
                ArticleCode='000000000000010001')]
    schema = StructType([StructField('ItemID', StringType(), True), StructField('Class', StringType(), True),
                         StructField('ArticleNumber', StringType(), True), StructField('Brand', StringType(), True),
                         StructField('MatchDescription', StringType(), True),
                         StructField('Description', StringType(), True),
                         StructField('ListingSince', StringType(), True),
                         StructField('ListingUntill', StringType(), True),
                         StructField('DiscontinuedArticleNumber', StringType(), True),
                         StructField('ReplacementArticleNumber', StringType(), True),
                         StructField('Active', BooleanType(), True), StructField('Volume', StringType(), True),
                         StructField('VolumeUOM', StringType(), True), StructField('PackagingCode', StringType(), True),
                         StructField('EANCE', StringType(), True), StructField('SupplierNumber', StringType(), True),
                         StructField('SupplierName', StringType(), True),
                         StructField('ArticleType', StringType(), True),
                         StructField('VariableWeight', BooleanType(), True), StructField('CMCode', StringType(), True),
                         StructField('CMName', StringType(), True),
                         StructField('PresentationGroup', StringType(), True), StructField('VAT', StringType(), True),
                         StructField('PriceRange', StringType(), True),
                         StructField('PriceIndicator', StringType(), True),
                         StructField('SegmentNumber', StringType(), True),
                         StructField('SegmentName', StringType(), True), StructField('MPC', StringType(), True),
                         StructField('MPCModel', StringType(), True),
                         StructField('MPCReferenceArticleNumber', StringType(), True),
                         StructField('PromoDays', StringType(), True),
                         StructField('PromoStart', StringType(), True), StructField('ImageURL', StringType(), True),
                         StructField('ProductURL', StringType(), True), StructField('BaseUnitCode', StringType(), True),
                         StructField('PackagingUnitCode', StringType(), True),
                         StructField('BaseUnitsPerDeliveryUnit', StringType(), True),
                         StructField('BaseUnitsPerSalesUnit', StringType(), True),
                         StructField('AverageWeeklySales', StringType(), True),
                         StructField('PromoType', StringType(), True), StructField('PromoOffer', StringType(), True),
                         StructField('Beheersafdeling', StringType(), True),
                         StructField('VolumeBruto', StringType(), True),
                         StructField('ArticleCode', StringType(), True)])
    df = spark_session.createDataFrame(data, schema=schema)
    schema = SparkSchema().get_schema(df)
    assert schema['ItemID'].dataType == DecimalType(1)
    assert schema['ListingSince'].dataType == TimestampType()
    assert schema['VolumeBruto'].dataType == DecimalType(3, 0)
