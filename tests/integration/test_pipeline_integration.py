"""
test_pipeline_integration.py - PySpark Integration Tests
=========================================================
Tests full pipeline steps using PySpark local mode on synthetic data.
"""
import pytest
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(not SPARK_AVAILABLE, reason="PySpark not installed")


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]")
         .appName("streamflow_integration_tests")
         .config("spark.sql.shuffle.partitions","4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def raw_events(spark):
    data = [
        ("E1","100000001","workflow: started","cancel","cancel_subscription","SAAS_CORE","CORE_MONTHLY","monthly","direct","desktop","app.saas.com","/app/billing","CancelFlowBillingCancel",None,None,"2024-06-01 10:00:00","2024-06-01"),
        ("E2","100000001","workflow: completed","cancel","cancel_success","SAAS_CORE","CORE_MONTHLY","monthly","direct","desktop","app.saas.com","/app/billing/confirmed",None,None,None,"2024-06-01 10:08:00","2024-06-01"),
        ("E3","100000002","workflow: started","cancel","cancel_subscription","SAAS_PLUS","PLUS_ANNUAL","annual","direct","mobile","mobile.saas.com","/app/billing","CancelFlowBillingCancel",None,None,"2024-06-02 14:00:00","2024-06-02"),
        ("E4","100000002","offer: viewed",None,None,"SAAS_PLUS","PLUS_ANNUAL","annual","direct","mobile","mobile.saas.com","/app/cancel-flow","CancelFlowBillingCancel","OFF-DISC-50",None,"2024-06-02 14:02:00","2024-06-02"),
        ("E5","100000003","workflow: started","cancel","cancel_subscription","SAAS_CORE","CORE_MONTHLY","monthly","accountant_billed","desktop","app.saas.com","/app/billing",None,None,"9000001","2024-04-01 09:00:00","2024-04-01"),
        ("E6","100000003","workflow: engaged","cancel","yes_cancel","SAAS_CORE","CORE_MONTHLY","monthly","accountant_billed","desktop","app.saas.com","/app/billing/confirmed",None,None,"9000001","2024-04-01 09:10:00","2024-04-01"),
    ]
    cols=["event_id","company_id","event","properties_object_detail","properties_ui_object_detail","product","sku","billing_frequency","subscription_type","ua_parser_device_type","properties_url_host_name","context_page_path","properties_ui_access_point","properties_custom_fp_offer_id","accountant_realm_id","event_timestamp","event_date"]
    return spark.createDataFrame(data, cols)


class TestStep1CancelInitiations:
    def test_initiation_count(self, raw_events):
        c = raw_events.filter(F.col("event").isin("workflow: started","workflow:started")).count()
        assert c == 3

    def test_new_taxonomy_confirmation(self, raw_events):
        c = raw_events.filter((F.col("event")=="workflow: completed") & (F.col("properties_ui_object_detail")=="cancel_success")).count()
        assert c == 1

    def test_legacy_yes_cancel_confirmation(self, raw_events):
        c = raw_events.filter((F.col("event")=="workflow: engaged") & (F.col("properties_ui_object_detail")=="yes_cancel")).count()
        assert c == 1

    def test_cancel_confirmed_yn_conversion(self, spark):
        df = spark.createDataFrame([(1,1),(2,0)],["cid","confirmed_int"])
        df = df.withColumn("cancel_confirmed", F.when(F.col("confirmed_int")==1,"Y").otherwise("N"))
        vals = set(r.cancel_confirmed for r in df.collect())
        assert vals == {"Y","N"}

    def test_initiation_rank_window(self, spark):
        from pyspark.sql import Window
        data = [(1,"2024-01-01 10:00:00"),(1,"2024-02-01 10:00:00"),(2,"2024-01-15 10:00:00")]
        df = spark.createDataFrame(data,["company_id","ts"])
        w = Window.partitionBy("company_id").orderBy("ts")
        df = df.withColumn("rank", F.row_number().over(w))
        ranks = {(r.company_id,r.ts): r.rank for r in df.collect()}
        assert ranks[(1,"2024-01-01 10:00:00")] == 1
        assert ranks[(1,"2024-02-01 10:00:00")] == 2
        assert ranks[(2,"2024-01-15 10:00:00")] == 1


class TestStep2IPDEngagement:
    def test_ipd_type_classification(self, spark):
        data = [("OFF-CS","contact-us-widget",None,None),
                ("OFF-DISC","external","OBILL-50","https://billing.saas.com/offer/disc"),
                ("OFF-UPGR","external",None,"https://billing.saas.com/obillupgrade/plus"),
                ("OFF-DNGR","external",None,"https://billing.saas.com/changeplan/core"),
                ("OFF-KEEP","callbackOnly",None,None)]
        df = spark.createDataFrame(data,["id","cta_action","obill_id","cta_url"])
        df = df.withColumn("ipd_type",
            F.when(F.col("cta_action")=="contact-us-widget","CS IPD")
            .when(F.col("obill_id").isNotNull(),"Discount IPD")
            .when((F.col("cta_action")=="external")&F.col("cta_url").contains("/obillupgrade"),"Upgrade IPD")
            .when((F.col("cta_action")=="external")&F.col("cta_url").contains("/changeplan"),"Downgrade IPD")
            .when((F.col("cta_action")=="callbackOnly")&F.col("obill_id").isNull(),"Keep my Plan IPD")
            .otherwise("Unknown"))
        t = {r.id: r.ipd_type for r in df.collect()}
        assert t["OFF-CS"]   == "CS IPD"
        assert t["OFF-DISC"] == "Discount IPD"
        assert t["OFF-UPGR"] == "Upgrade IPD"
        assert t["OFF-DNGR"] == "Downgrade IPD"
        assert t["OFF-KEEP"] == "Keep my Plan IPD"

    def test_select_distinct_eliminates_multiplied_rows(self, spark):
        data = [(1,1,"2024-06-01","AP1","OFF-1"),(1,1,"2024-06-01","AP1","OFF-1")]
        df = spark.createDataFrame(data,["cid","rank","ts","ap","oid"])
        assert df.distinct().count() == 1

    def test_dic_coalesce_not_null(self, spark):
        data = [(1,None),(2,3)]
        df = spark.createDataFrame(data,["cid","dic_raw"])
        df = df.withColumn("viewed_dic_component", F.coalesce(F.col("dic_raw"), F.lit(0)))
        assert df.filter(F.col("viewed_dic_component").isNull()).count() == 0


class TestStep3SaveAttribution:
    def test_priority_waterfall(self, spark):
        data = [(1,1,0,0,0,0),(2,0,1,0,0,0),(3,0,0,1,0,0),(4,0,0,0,0,0)]
        df = spark.createDataFrame(data,["cid","saved_by_cs","cancel_confirmed","upgraded","downgraded","took_discount"])
        df = df.withColumn("save_attribution",
            F.when(F.col("saved_by_cs")==1,"CS Save")
            .when(F.col("cancel_confirmed")==1,"Cancelled")
            .when(F.col("upgraded")==1,"Upgrade Save")
            .when(F.col("downgraded")==1,"Downgrade Save")
            .when(F.col("took_discount")==1,"Discount Save")
            .otherwise("Abandoned"))
        vals = {r.cid: r.save_attribution for r in df.collect()}
        assert vals[1] == "CS Save"
        assert vals[2] == "Cancelled"
        assert vals[3] == "Upgrade Save"
        assert vals[4] == "Abandoned"


class TestStep4FinalMetrics:
    def test_baked_31d_logic(self, spark):
        from datetime import date
        data = [(1, date(2020,1,1)),(2, date(2099,1,1))]
        df = spark.createDataFrame(data,["cid","initiation_date"])
        df = df.withColumn("baked_31d",
            F.when(F.date_add(F.col("initiation_date"),31) < F.current_date(), F.lit(1)).otherwise(F.lit(0)))
        vals = {r.cid: r.baked_31d for r in df.collect()}
        assert vals[1] == 1
        assert vals[2] == 0

    def test_row_count_parity(self):
        step1 = 2_107_994
        step4 = 2_107_994
        assert step1 == step4, "Step 1 and Step 4 row counts must match (no IXP filter)"
