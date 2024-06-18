# Databricks notebook source
from delta.tables import *
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import date, timedelta, datetime
import pandas as pd

# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/qnr_scrubbing

# COMMAND ----------

# MAGIC %run /Shared/release/HayStack/Common_Functions/Haystack_Audit

# COMMAND ----------

dbutils.widgets.text("Audit_Job_ID", "", "Audit_Job_ID")
Audit_Job_ID = dbutils.widgets.get("Audit_Job_ID")

dbutils.widgets.text("ADF_Name", "", "ADF_Name")
ENV = dbutils.widgets.get("ADF_Name")

dbutils.widgets.text("Water_mark_back_days", "", "Water_mark_back_days")
Water_mark_back_days  = dbutils.widgets.get("Water_mark_back_days")

# COMMAND ----------

if ENV == 'az23d1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ENV == 'az23q1-gf-quality-adf':
    Catalog_Name_L1 = 'qa_l1'
    Catalog_Name_L2 = 'qa_l2'
elif ENV == 'az23t1-gf-quality-adf':
    Catalog_Name_L1 = 'dev_l1'
    Catalog_Name_L2 = 'dev_l2'
elif ENV == 'az23p1-gf-quality-adf':
    Catalog_Name_L1 = 'prod_l1'
    Catalog_Name_L2 = 'prod_l2'    
Source_Name='QDS'

# COMMAND ----------

Table_Name=Catalog_Name_L2+'.qnr.complaint'
Audit_Table_Name='COMPLAINT'
Merge_key =Find_Merge_Key(Table_Name,Audit_Table_Name)
Audit_Master_ID =Find_Audit_Master_ID(Table_Name,Audit_Table_Name)
Water_Mark_DT =Find_Water_Mark_DT(Table_Name,Audit_Table_Name)

# COMMAND ----------

# df = spark.createDataFrame(
#     [(range(1,380,1),range(0,380000,10000),range(10000,390000,10000))
#      ],
#     ["id", "start","end"]  # add your column names here
# )

# COMMAND ----------

df = spark.createDataFrame(
    [
 (1,0,10000),
 (2,10000,20000),
 (3,20000,30000),
 (4,30000,40000),
 (5,40000,50000),
 (6,50000,60000),
 (7,60000,70000),
 (8,70000,80000),
 (9,80000,90000),
 (10,90000,100000),
 (11,100000,110000),
 (12,110000,120000),
 (13,120000,130000),
 (14,130000,140000),
 (15,140000,150000),
 (16,150000,160000),
 (17,160000,170000),
 (18,170000,180000),
 (19,180000,190000),
 (20,190000,200000),
 (21,200000,210000),
 (22,210000,220000),
 (23,220000,230000),
 (24,230000,240000),
 (25,240000,250000),
 (26,250000,260000),
 (27,260000,270000),
 (28,270000,280000),
 (29,280000,290000),
 (30,290000,300000),
 (31,300000,310000),
 (32,310000,320000),
 (33,320000,330000),
 (34,330000,340000),
 (35,340000,350000),
 (36,350000,360000),
 (37,360000,370000),
 (38,370000,380000),
 (39,380000,390000),
 (40,390000,400000),
 (41,400000,410000),
 (42,410000,420000),
 (43,420000,430000),
 (44,430000,440000),
 (45,440000,450000),
 (46,450000,460000),
 (47,460000,470000),
 (48,470000,480000),
 (49,480000,490000),
 (50,490000,500000),
 (51,500000,510000),
 (52,510000,520000),
 (53,520000,530000),
 (54,530000,540000),
 (55,540000,550000),
 (56,550000,560000),
 (57,560000,570000),
 (58,570000,580000),
 (59,580000,590000),
 (60,590000,600000),
 (61,600000,610000),
 (62,610000,620000),
 (63,620000,630000),
 (64,630000,640000),
 (65,640000,650000),
 (66,650000,660000),
 (67,660000,670000),
 (68,670000,680000),
 (69,680000,690000),
 (70,690000,700000),
 (71,700000,710000),
 (72,710000,720000),
 (73,720000,730000),
 (74,730000,740000),
 (75,740000,750000),
 (76,750000,760000),
 (77,760000,770000),
 (78,770000,780000),
 (79,780000,790000),
 (80,790000,800000),
 (81,800000,810000),
 (82,810000,820000),
 (83,820000,830000),
 (84,830000,840000),
 (85,840000,850000),
 (86,850000,860000),
 (87,860000,870000),
 (88,870000,880000),
 (89,880000,890000),
 (90,890000,900000),
 (91,900000,910000),
 (92,910000,920000),
 (93,920000,930000),
 (94,930000,940000),
 (95,940000,950000),
 (96,950000,960000),
 (97,960000,970000),
 (98,970000,980000),
 (99,980000,990000),
 (100,990000,1000000),
 (101,1000000,1010000),
 (102,1010000,1020000),
 (103,1020000,1030000),
 (104,1030000,1040000),
 (105,1040000,1050000),
 (106,1050000,1060000),
 (107,1060000,1070000),
 (108,1070000,1080000),
 (109,1080000,1090000),
 (110,1090000,1100000),
 (111,1100000,1110000),
 (112,1110000,1120000),
 (113,1120000,1130000),
 (114,1130000,1140000),
 (115,1140000,1150000),
 (116,1150000,1160000),
 (117,1160000,1170000),
 (118,1170000,1180000),
 (119,1180000,1190000),
 (120,1190000,1200000),
 (121,1200000,1210000),
 (122,1210000,1220000),
 (123,1220000,1230000),
 (124,1230000,1240000),
 (125,1240000,1250000),
 (126,1250000,1260000),
 (127,1260000,1270000),
 (128,1270000,1280000),
 (129,1280000,1290000),
 (130,1290000,1300000),
 (131,1300000,1310000),
 (132,1310000,1320000),
 (133,1320000,1330000),
 (134,1330000,1340000),
 (135,1340000,1350000),
 (136,1350000,1360000),
 (137,1360000,1370000),
 (138,1370000,1380000),
 (139,1380000,1390000),
 (140,1390000,1400000),
 (141,1400000,1410000),
 (142,1410000,1420000),
 (143,1420000,1430000),
 (144,1430000,1440000),
 (145,1440000,1450000),
 (146,1450000,1460000),
 (147,1460000,1470000),
 (148,1470000,1480000),
 (149,1480000,1490000),
 (150,1490000,1500000),
 (151,1500000,1510000),
 (152,1510000,1520000),
 (153,1520000,1530000),
 (154,1530000,1540000),
 (155,1540000,1550000),
 (156,1550000,1560000),
 (157,1560000,1570000),
 (158,1570000,1580000),
 (159,1580000,1590000),
 (160,1590000,1600000),
 (161,1600000,1610000),
 (162,1610000,1620000),
 (163,1620000,1630000),
 (164,1630000,1640000),
 (165,1640000,1650000),
 (166,1650000,1660000),
 (167,1660000,1670000),
 (168,1670000,1680000),
 (169,1680000,1690000),
 (170,1690000,1700000),
 (171,1700000,1710000),
 (172,1710000,1720000),
 (173,1720000,1730000),
 (174,1730000,1740000),
 (175,1740000,1750000),
 (176,1750000,1760000),
 (177,1760000,1770000),
 (178,1770000,1780000),
 (179,1780000,1790000),
 (180,1790000,1800000),
 (181,1800000,1810000),
 (182,1810000,1820000),
 (183,1820000,1830000),
 (184,1830000,1840000),
 (185,1840000,1850000),
 (186,1850000,1860000),
 (187,1860000,1870000),
 (188,1870000,1880000),
 (189,1880000,1890000),
 (190,1890000,1900000),
 (191,1900000,1910000),
 (192,1910000,1920000),
 (193,1920000,1930000),
 (194,1930000,1940000),
 (195,1940000,1950000),
 (196,1950000,1960000),
 (197,1960000,1970000),
 (198,1970000,1980000),
 (199,1980000,1990000),
 (200,1990000,2000000),
 (201,2000000,2010000),
 (202,2010000,2020000),
 (203,2020000,2030000),
 (204,2030000,2040000),
 (205,2040000,2050000),
 (206,2050000,2060000),
 (207,2060000,2070000),
 (208,2070000,2080000),
 (209,2080000,2090000),
 (210,2090000,2100000),
 (211,2100000,2110000),
 (212,2110000,2120000),
 (213,2120000,2130000),
 (214,2130000,2140000),
 (215,2140000,2150000),
 (216,2150000,2160000),
 (217,2160000,2170000),
 (218,2170000,2180000),
 (219,2180000,2190000),
 (220,2190000,2200000),
 (221,2200000,2210000),
 (222,2210000,2220000),
 (223,2220000,2230000),
 (224,2230000,2240000),
 (225,2240000,2250000),
 (226,2250000,2260000),
 (227,2260000,2270000),
 (228,2270000,2280000),
 (229,2280000,2290000),
 (230,2290000,2300000),
 (231,2300000,2310000),
 (232,2310000,2320000),
 (233,2320000,2330000),
 (234,2330000,2340000),
 (235,2340000,2350000),
 (236,2350000,2360000),
 (237,2360000,2370000),
 (238,2370000,2380000),
 (239,2380000,2390000),
 (240,2390000,2400000),
 (241,2400000,2410000),
 (242,2410000,2420000),
 (243,2420000,2430000),
 (244,2430000,2440000),
 (245,2440000,2450000),
 (246,2450000,2460000),
 (247,2460000,2470000),
 (248,2470000,2480000),
 (249,2480000,2490000),
 (250,2490000,2500000),
 (251,2500000,2510000),
 (252,2510000,2520000),
 (253,2520000,2530000),
 (254,2530000,2540000),
 (255,2540000,2550000),
 (256,2550000,2560000),
 (257,2560000,2570000),
 (258,2570000,2580000),
 (259,2580000,2590000),
 (260,2590000,2600000),
 (261,2600000,2610000),
 (262,2610000,2620000),
 (263,2620000,2630000),
 (264,2630000,2640000),
 (265,2640000,2650000),
 (266,2650000,2660000),
 (267,2660000,2670000),
 (268,2670000,2680000),
 (269,2680000,2690000),
 (270,2690000,2700000),
 (271,2700000,2710000),
 (272,2710000,2720000),
 (273,2720000,2730000),
 (274,2730000,2740000),
 (275,2740000,2750000),
 (276,2750000,2760000),
 (277,2760000,2770000),
 (278,2770000,2780000),
 (279,2780000,2790000),
 (280,2790000,2800000),
 (281,2800000,2810000),
 (282,2810000,2820000),
 (283,2820000,2830000),
 (284,2830000,2840000),
 (285,2840000,2850000),
 (286,2850000,2860000),
 (287,2860000,2870000),
 (288,2870000,2880000),
 (289,2880000,2890000),
 (290,2890000,2900000),
 (291,2900000,2910000),
 (292,2910000,2920000),
 (293,2920000,2930000),
 (294,2930000,2940000),
 (295,2940000,2950000),
 (296,2950000,2960000),
 (297,2960000,2970000),
 (298,2970000,2980000),
 (299,2980000,2990000),
 (300,2990000,3000000),
 (301,3000000,3010000),
 (302,3010000,3020000),
 (303,3020000,3030000),
 (304,3030000,3040000),
 (305,3040000,3050000),
 (306,3050000,3060000),
 (307,3060000,3070000),
 (308,3070000,3080000),
 (309,3080000,3090000),
 (310,3090000,3100000),
 (311,3100000,3110000),
 (312,3110000,3120000),
 (313,3120000,3130000),
 (314,3130000,3140000),
 (315,3140000,3150000),
 (316,3150000,3160000),
 (317,3160000,3170000),
 (318,3170000,3180000),
 (319,3180000,3190000),
 (320,3190000,3200000),
 (321,3200000,3210000),
 (322,3210000,3220000),
 (323,3220000,3230000),
 (324,3230000,3240000),
 (325,3240000,3250000),
 (326,3250000,3260000),
 (327,3260000,3270000),
 (328,3270000,3280000),
 (329,3280000,3290000),
 (330,3290000,3300000),
 (331,3300000,3310000),
 (332,3310000,3320000),
 (333,3320000,3330000),
 (334,3330000,3340000),
 (335,3340000,3350000),
 (336,3350000,3360000),
 (337,3360000,3370000),
 (338,3370000,3380000),
 (339,3380000,3390000),
 (340,3390000,3400000),
 (341,3400000,3410000),
 (342,3410000,3420000),
 (343,3420000,3430000),
 (344,3430000,3440000),
 (345,3440000,3450000),
 (346,3450000,3460000),
 (347,3460000,3470000),
 (348,3470000,3480000),
 (349,3480000,3490000),
 (350,3490000,3500000),
 (351,3500000,3510000),
 (352,3510000,3520000),
 (353,3520000,3530000),
 (354,3530000,3540000),
 (355,3540000,3550000),
 (356,3550000,3560000),
 (357,3560000,3570000),
 (358,3570000,3580000),
 (359,3580000,3590000),
 (360,3590000,3600000),
 (361,3600000,3610000),
 (362,3610000,3620000),
 (363,3620000,3630000),
 (364,3630000,3640000),
 (365,3640000,3650000),
 (366,3650000,3660000),
 (367,3660000,3670000),
 (368,3670000,3680000),
 (369,3680000,3690000),
 (370,3690000,3700000),
 (371,3700000,3710000),
 (372,3710000,3720000),
 (373,3720000,3730000),
 (374,3730000,3740000),
 (375,3740000,3750000),
 (376,3750000,3760000),
 (377,3760000,3770000),
 (378,3770000,3780000),
 (379,3780000,3790000),
 (380,3790000,3800000)

    ],
    ["id", "start","end"]  # add your column names here
)

# COMMAND ----------

chunkdf=df.toPandas()
for index, row in chunkdf.iterrows():
    print(row[0],row[1],row[2])
    print(datetime.now())
    Complaint_Query="""select 
    PR1.ID  as COMPLAINT_ID ,
    17973 as APPLICATION_ID,
    cast(DF_530 as DATE) as EVENT_DATE ,
    PR1.DF_3 as DATE_OPENED,
    ADT_379.Name as PRODUCT_FAMILY ,
    ADT_84.Name as OWNING_ENTITY_NAME ,
    DF_1157 as CATALOG_ITEM_ID ,
    DF_1162 as CATALOG_ITEM_NAME ,
    DF_1137 as TRADE_ITEM_ID ,
    DF_1154 as TRADE_ITEM_NAME ,
    DF_574 as DEVICE_IDENTIFIER ,
    case when lower(trim(ADT_1286.NAME))='no' then 'N' when lower(trim(ADT_1286.NAME))='yes' THEN 'y' ELSE NULL end as IS_THIRD_PARTY_DEVICE , 
    DF_857 as MODEL_NUMBER ,
    case when lower(trim(ADT_1156.NAME))='no' THEN 'N' when  lower(trim(ADT_1156.NAME))='yes' THEN 'Y'  else NULL END as IS_MEDICAL_DEVICE ,
    DF_554 as PRODUCT_UDI ,
    DF_534 as MANUFACTURE_DATE ,
    DF_543 as SHIP_DATE ,
    DF_531 as EXPIRATION_DATE ,
    ADT_635.NAME  as REPORTER_COUNTRY ,
    ADT_662.NAME as EVENT_COUNTRY ,
    DF_535 as PHILIPS_NOTIFIED_DATE ,
    DF_533 as INITIATE_DATE ,
    DF_512 as BECOME_AWARE_DATE ,
    PR1.DF_0 as COMPLAINT_SHORT_DESCRIPTION ,
    DF_398 as COMPLAINT_CUSTOMER_DESCRIPTION ,
    ADT_1249.NAME as REPORTED_PROBLEM_CODE_L1,
    ADT_1250.NAME as REPORTED_PROBLEM_CODE_L2,
    ADT_1366.NAME as REPORTED_PROBLEM_CODE_L3,
    ADT_460.NAME as DEVICE_USE_AT_TIME_OF_EVENT ,
    case when lower(trim(ADT_619.NAME))='no' THEN 'N' WHEN lower(trim(ADT_619.NAME)) ='yes' THEN 'Y' ELSE NULL END as IS_PATIENT_USER_HARMED ,
    case when lower(trim(ADT_624.NAME))='no' THEN 'N' when lower(trim(ADT_624.NAME))='yes' then 'Y' ELSE NULL END as IS_POTENTIAL_SAFETY_ALERT,
    case when lower(trim(ADT_625.NAME))='no' THEN 'N' when lower(trim(ADT_625.NAME))='yes' THEN 'Y' ELSE NULL end as IS_POTENTIAL_SAFETY_EVENT,
    case when lower(trim(ADT_1368.NAME))='no' THEN 'N' when lower(trim(ADT_1368.NAME))='yes' THEN 'Y' ELSE NULL end as IS_ALLEGATION_OF_INJURY_OR_DEATH,
	case when lower(trim(ADT_450.NAME))='no' THEN 'N' when lower(trim(ADT_450.NAME))='yes' THEN 'Y' ELSE NULL end as  HAS_DEVICE_ALARMED,
    ADT_1369.NAME as INCIDENT_KEY_WORDS,
    ADT_654.NAME as TYPE_OF_REPORTED_COMPLAINT,
    ADT_1052.NAME as HAZARDOUS_SITUATION,
    DF_72 as COMPLAINT_LONG_DESCRIPTION ,
    DF_399 as SOURCE_NOTES ,
    DF_122 as COMMENTS ,
    DF_392 as INVESTIGATION_SUMMARY ,
    DF_391 as INVESTIGATION_NOTES ,
    ADT_1371.NAME as PROBLEM_SOURCE_CODE,
    ADT_1372.NAME as PROBLEM_REASON_CODE,
    case when lower(trim(ADT_1293.NAME))='no' THEN 'N'   when lower(trim(ADT_1293.NAME))='yes' THEN 'Y' ELSE NULL end as IS_CAPA_ADDTL_INVEST_REQUIRED,
    DF_393 as OTHER_RELEVANT_EVENT_INFO ,
    DF_1095 as PATIENT_ACTIONS_TAKEN ,
    DF_563 as PROBLEM_SYMPTOMS_AND_FREQUENCY ,
    DF_496 as ADDTL_INFORMATION,
    DF_1388 as MEDICAL_RATIONALE,
    ADT_649.NAME as SOURCE_SYSTEM,
    ADT_646.NAME as SOURCE_OF_COMPLAINT,
    PR1.DF_493 as SOURCE_SERIAL_NUMBER,
    PR1.DF_489 as SERIAL_NUMBER, 
    PR1.DATE_CREATED as DATE_CREATED,
    PR1.DATE_CLOSED as  DATE_CLOSED,
    PR1.DF_1159 as SOURCE_CATALOG_ITEM_ID,
    PR1.DF_1163 as SOURCE_CATALOG_ITEM_NAME,
    PR1.DF_593 as LOT_OR_BATCH_NUMBER,
    ADT_645.NAME as SOLUTION_FOR_THE_CUSTOMER,
    PR1.DF_557 as CUSTOMER_NUMBER,
    PR1.DF_476 as NO_FURTHER_INVEST_REQ_ON,
    ADT_1256.NAME as SOURCE_EVENT_COUNTRY,
    NULL as ADDITIONAL_EVALUATION_COMMENTS,
    NULL as SYMPTOM_CODE_1,
    NULL as SYMPTOM_CODE_2,
    NULL as SYMPTOM_CODE_3,
    cast("{1}" as string) as Audit_Job_ID,
    PR1.ADLS_LOADED_DATE as L1_LOADED_DATE,
    PR1.LAST_UPDATED_DATE as L1_UPDATED_DATE,
    getdate() as  L2_LOADED_DATE,
    getdate() as L2_UPDATED_DATE
    from {0}.qds.vw_RDS_PR_1 PR1 LEFT JOIN {0}.qds.vw_RDS_PR_2 PR2
    ON PR1.ID=PR2.ID
    Left join {0}.qds.vw_ADDTL_TYPE ADT_379
      on ADT_379.ID = PR1.DF_379
    Left join {0}.qds.vw_ADDTL_TYPE ADT_84
      on  ADT_84.ID = PR1.DF_84  
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1286
      on  ADT_1286.ID = PR2.DF_1286  
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1156
      on  ADT_1156.ID = PR1.DF_1156  
    Left join {0}.qds.vw_ADDTL_TYPE ADT_635
      on  ADT_635.ID = PR1.DF_635 
    Left join {0}.qds.vw_ADDTL_TYPE ADT_662
      on  ADT_662.ID = PR1.DF_662
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1249
      on  ADT_1249.ID = PR2.DF_1249
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1250
      on ADT_1250.ID = PR2.DF_1250
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1366
      on  ADT_1366.ID = PR2.DF_1366
    Left join {0}.qds.vw_ADDTL_TYPE ADT_460
      on  ADT_460.ID = PR1.DF_460
    Left join {0}.qds.vw_ADDTL_TYPE ADT_619
      on  ADT_619.ID = PR1.DF_619
    Left join {0}.qds.vw_ADDTL_TYPE ADT_624
      on  ADT_624.ID = PR1.DF_624
    Left join {0}.qds.vw_ADDTL_TYPE ADT_625
      on  ADT_625.ID = PR1.DF_625
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1368
      on ADT_1368.ID = PR2.DF_1368  
    Left join {0}.qds.vw_ADDTL_TYPE ADT_450
      on  ADT_450.ID = PR1.DF_450   
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1369
      on  ADT_1369.ID = PR2.DF_1369
    Left join {0}.qds.vw_ADDTL_TYPE ADT_654
      on  ADT_654.ID = PR1.DF_654
	  Left join {0}.qds.vw_ADDTL_TYPE ADT_1052
      on ADT_1052.ID = PR1.DF_1052
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1371
      on  ADT_1371.ID = PR2.DF_1371
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1372
      on ADT_1372.ID = PR2.DF_1372
    Left join {0}.qds.vw_ADDTL_TYPE ADT_1293  
      on  ADT_1293.ID = PR2.DF_1293   
    Left join {0}.qds.vw_ADDTL_TYPE ADT_649  
      on  ADT_649.ID = PR1.DF_649 
  Left join {0}.qds.vw_ADDTL_TYPE ADT_646  
      on  ADT_646.ID = PR1.DF_646    
  Left join {0}.qds.vw_ADDTL_TYPE ADT_645  
      on  ADT_645.ID = PR1.DF_645    
  Left join {0}.qds.vw_ADDTL_TYPE ADT_1256  
      on  ADT_1256.ID = PR2.DF_1256 
    WHERE PR1.PROJECT_ID IN (16,17) 
    and PR1.id between {3} and {4}
    """.format(Catalog_Name_L1,Audit_Job_ID,Water_Mark_DT,row[1],row[2])
    print(datetime.now())
    # print(Complaint_Query)
    # break
    df_Complaint=sqlContext.sql(Complaint_Query)
    df_scrub =df_Complaint.select(col("COMPLAINT_ID"),col("APPLICATION_ID")
                                  ,col("COMPLAINT_SHORT_DESCRIPTION"),col("COMPLAINT_CUSTOMER_DESCRIPTION"),col("COMPLAINT_LONG_DESCRIPTION"),col("SOURCE_NOTES"),col("COMMENTS"),col("INVESTIGATION_SUMMARY"),col("INVESTIGATION_NOTES"),col("OTHER_RELEVANT_EVENT_INFO")
                                  ,col("PATIENT_ACTIONS_TAKEN"),col("PROBLEM_SYMPTOMS_AND_FREQUENCY")
                                  ,col("ADDTL_INFORMATION"),col("MEDICAL_RATIONALE")
                                  )
    ###Null handling in order to convert to pandas for looping
    print("looping starts")
    print(row[1]) 
    print(row[2])
    print(datetime.now())
    df_scrub_Intermediate = df_scrub.na.fill(" ")
    df_scrub_Intermediate.persist()
    column_list =["COMPLAINT_SHORT_DESCRIPTION","COMPLAINT_CUSTOMER_DESCRIPTION","COMPLAINT_LONG_DESCRIPTION",
                "SOURCE_NOTES","COMMENTS","INVESTIGATION_SUMMARY","INVESTIGATION_NOTES","OTHER_RELEVANT_EVENT_INFO","PATIENT_ACTIONS_TAKEN"
              ,"PROBLEM_SYMPTOMS_AND_FREQUENCY","ADDTL_INFORMATION","MEDICAL_RATIONALE"
              ]
    ploopdf=df_scrub_Intermediate.toPandas()
    for i in column_list:
        for index, row in ploopdf.iterrows():
            if type(row[i]) != str and type(row[i]) != bytes:
                row[i] = str(row[i])
            ploopdf[i]= ploopdf[i].replace(row[i], qnrscrubber.clean(row[i]))
    df_scrub_out=spark.createDataFrame(ploopdf)
    print("looping ends")
    print(datetime.now())
    #####Join with main to table to replace scrubbed columns
    df_Complaint_scrubbed=df_Complaint.join(df_scrub_out,(df_Complaint.COMPLAINT_ID == df_scrub_out.COMPLAINT_ID) &(df_Complaint.APPLICATION_ID == df_scrub_out.APPLICATION_ID) ,"inner").select(df_Complaint["COMPLAINT_ID"], df_Complaint["APPLICATION_ID"], df_Complaint["EVENT_DATE"], df_Complaint["DATE_OPENED"], df_Complaint["PRODUCT_FAMILY"], df_Complaint["OWNING_ENTITY_NAME"], df_Complaint["CATALOG_ITEM_ID"], df_Complaint["CATALOG_ITEM_NAME"], df_Complaint["TRADE_ITEM_ID"], df_Complaint["TRADE_ITEM_NAME"], df_Complaint["DEVICE_IDENTIFIER"], df_Complaint["IS_THIRD_PARTY_DEVICE"], df_Complaint["MODEL_NUMBER"], df_Complaint["IS_MEDICAL_DEVICE"], df_Complaint["PRODUCT_UDI"], df_Complaint["MANUFACTURE_DATE"], df_Complaint["SHIP_DATE"], df_Complaint["EXPIRATION_DATE"], df_Complaint["REPORTER_COUNTRY"], df_Complaint["EVENT_COUNTRY"], df_Complaint["PHILIPS_NOTIFIED_DATE"], df_Complaint["INITIATE_DATE"], df_Complaint["BECOME_AWARE_DATE"], df_scrub_out["COMPLAINT_SHORT_DESCRIPTION"], df_scrub_out["COMPLAINT_CUSTOMER_DESCRIPTION"], df_Complaint["REPORTED_PROBLEM_CODE_L1"], df_Complaint["REPORTED_PROBLEM_CODE_L2"], df_Complaint["REPORTED_PROBLEM_CODE_L3"], df_Complaint["DEVICE_USE_AT_TIME_OF_EVENT"], df_Complaint["IS_PATIENT_USER_HARMED"], df_Complaint["IS_POTENTIAL_SAFETY_ALERT"], df_Complaint["IS_POTENTIAL_SAFETY_EVENT"], df_Complaint["IS_ALLEGATION_OF_INJURY_OR_DEATH"], df_Complaint["HAS_DEVICE_ALARMED"], df_Complaint["INCIDENT_KEY_WORDS"], df_Complaint["TYPE_OF_REPORTED_COMPLAINT"], df_Complaint["HAZARDOUS_SITUATION"], df_scrub_out["COMPLAINT_LONG_DESCRIPTION"], df_scrub_out["SOURCE_NOTES"], df_scrub_out["COMMENTS"], df_scrub_out["INVESTIGATION_SUMMARY"], df_scrub_out["INVESTIGATION_NOTES"], df_Complaint["PROBLEM_SOURCE_CODE"], df_Complaint["PROBLEM_REASON_CODE"], df_Complaint["IS_CAPA_ADDTL_INVEST_REQUIRED"], df_scrub_out["OTHER_RELEVANT_EVENT_INFO"], df_scrub_out["PATIENT_ACTIONS_TAKEN"], df_scrub_out["PROBLEM_SYMPTOMS_AND_FREQUENCY"], df_scrub_out["ADDTL_INFORMATION"], df_scrub_out["MEDICAL_RATIONALE"], df_Complaint["SOURCE_SYSTEM"], df_Complaint["SOURCE_OF_COMPLAINT"], df_Complaint["SOURCE_SERIAL_NUMBER"], df_Complaint["SERIAL_NUMBER"], df_Complaint["DATE_CREATED"], df_Complaint["DATE_CLOSED"], df_Complaint["SOURCE_CATALOG_ITEM_ID"], df_Complaint["SOURCE_CATALOG_ITEM_NAME"], df_Complaint["LOT_OR_BATCH_NUMBER"], df_Complaint["SOLUTION_FOR_THE_CUSTOMER"], df_Complaint["CUSTOMER_NUMBER"], df_Complaint["NO_FURTHER_INVEST_REQ_ON"], df_Complaint["SOURCE_EVENT_COUNTRY"], df_Complaint["ADDITIONAL_EVALUATION_COMMENTS"], df_Complaint["SYMPTOM_CODE_1"], df_Complaint["SYMPTOM_CODE_2"], df_Complaint["SYMPTOM_CODE_3"], df_Complaint["Audit_Job_ID"], df_Complaint["L1_LOADED_DATE"], df_Complaint["L1_UPDATED_DATE"], df_Complaint["L2_LOADED_DATE"], df_Complaint["L2_UPDATED_DATE"])
    ############################################################################################################################
    # Step 1: Enter in L2_Audit table for process start                                                                        #
    # Step 2: Merage Complaint table based on merge key set above                                                              #
    # Step 3a: update Audit table entry  by setting as status Completed                                                        #
    # Step 3b: In case of failure update  the Audit table entry  by updating the status as Error with an error message         # 
    ############################################################################################################################
    try: 
        Audit_Start_date_Time = datetime.now()
        Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,None ,Source_Name ,Table_Name ,'Inprogress',None, None,None, None)
        # df_Complaint=sqlContext.sql(Complaint_Query)
        var_Complaint_count = df_Complaint_scrubbed.count()
        var_Complaint_ID_min=df_Complaint_scrubbed.agg({'COMPLAINT_ID': 'min'}).collect()[0][0]
        var_Complaint_ID_max=df_Complaint_scrubbed.agg({'COMPLAINT_ID': 'max'}).collect()[0][0]
        delta_merge(Table_Name,df_Complaint_scrubbed,Merge_key ) 
        Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Completed',var_Complaint_count,var_Complaint_ID_max,var_Complaint_ID_min,None)
    except Exception as err:
        Audit_logging(Audit_Master_ID ,Audit_Job_ID ,Audit_Start_date_Time ,datetime.now() ,Source_Name ,Table_Name,'Error',None,None,None,err)
    
