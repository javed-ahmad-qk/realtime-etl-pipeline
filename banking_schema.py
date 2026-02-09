
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, DateType, IntegerType, BooleanType

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("address_line1", StringType(), True),
    StructField("address_line2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("registration_date", TimestampType(), True),
    StructField("last_update_timestamp", TimestampType(), True)
])

ACCOUNT_SCHEMA = StructType([
    StructField("account_id", LongType(), False),
    StructField("customer_id", LongType(), False),
    StructField("account_type", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("open_date", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("last_activity_date", TimestampType(), True)
])

ATM_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("atm_id", LongType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True)
])

LOAN_SCHEMA = StructType([
    StructField("loan_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("loan_type", StringType(), True),
    StructField("principal_amount", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("outstanding_balance", DoubleType(), True)
])

CARD_SCHEMA = StructType([
    StructField("card_id", LongType(), False),
    StructField("account_id", LongType(), False),
    StructField("card_type", StringType(), True),
    StructField("card_number_hash", StringType(), True),
    StructField("expiration_date", DateType(), True),
    StructField("issue_date", DateType(), True),
    StructField("status", StringType(), True)
])

BRANCH_SCHEMA = StructType([
    StructField("branch_id", LongType(), False),
    StructField("branch_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("phone_number", StringType(), True)
])

EMPLOYEE_SCHEMA = StructType([
    StructField("employee_id", LongType(), False),
    StructField("branch_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("hire_date", DateType(), True),
    StructField("email", StringType(), True)
])

TRANSACTION_TYPE_SCHEMA = StructType([
    StructField("type_id", LongType(), False),
    StructField("type_name", StringType(), True),
    StructField("description", StringType(), True)
])

AUDIT_LOG_SCHEMA = StructType([
    StructField("log_id", LongType(), False),
    StructField("user_id", LongType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("details", StringType(), True)
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id", LongType(), False),
    StructField("product_name", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("min_balance", DoubleType(), True)
])

SERVICE_SCHEMA = StructType([
    StructField("service_id", LongType(), False),
    StructField("service_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("status", StringType(), True)
])

TRANSACTION_DETAILS_SCHEMA = StructType([
    StructField("detail_id", LongType(), False),
    StructField("transaction_id", LongType(), False),
    StructField("item_description", StringType(), True),
    StructField("item_amount", DoubleType(), True),
    StructField("category", StringType(), True)
])

MERCHANT_SCHEMA = StructType([
    StructField("merchant_id", LongType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

BANKING_TABLE_SCHEMAS = {
    "customers": CUSTOMER_SCHEMA,
    "accounts": ACCOUNT_SCHEMA,
    "atm_transactions": ATM_TRANSACTION_SCHEMA,
    "loans": LOAN_SCHEMA,
    "cards": CARD_SCHEMA,
    "branches": BRANCH_SCHEMA,
    "employees": EMPLOYEE_SCHEMA,
    "transaction_types": TRANSACTION_TYPE_SCHEMA,
    "audit_logs": AUDIT_LOG_SCHEMA,
    "products": PRODUCT_SCHEMA,
    "services": SERVICE_SCHEMA,
    "transaction_details": TRANSACTION_DETAILS_SCHEMA,
    "merchants": MERCHANT_SCHEMA
}

DEBEZIUM_PAYLOAD_SCHEMA = StructType([
    StructField("before", MapType(StringType(), StringType()), True),
    StructField("after", MapType(StringType(), StringType()), True),
    StructField("source", StructType([
        StructField("db", StringType(), True),
        StructField("table", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])
