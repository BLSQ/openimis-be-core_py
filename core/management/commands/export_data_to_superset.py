import logging
import tempfile
import os

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.paginator import Paginator
from django.db import connection
import csv
import pandas as pd
from django.db.models import QuerySet
from pandas import DataFrame
from sqlalchemy import create_engine, MetaData, Column, Table, Integer, DOUBLE_PRECISION, Boolean, String, Date
from sqlalchemy.sql.type_api import TypeEngine

from claim.models import Claim, ClaimDetail, ClaimItem, ClaimService
from claim_batch.models import BatchRun
from contribution.models import Premium
from core.models import InteractiveUser
from insuree.models import Insuree, InsureePolicy
from invoice.models import Bill
from location.models import HealthFacility, Location
from medical.models import Diagnosis
from policy.models import Policy
from product.models import Product

logger = logging.getLogger(__name__)

SQL_CLAIM_STATISTICS_QUERY_MSSQL = f"""
SELECT
  COUNT(tblClaim.ClaimID) AS 'Number of claims',
  tblHf.HFName AS 'Hospital Name',
  region_locations.LocationName AS 'Region',
  district_locations.LocationName AS 'District',
  CONVERT(DATE, tblClaim.DateClaimed) AS 'Date',
  (CASE tblInsuree.Gender
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
    ELSE 'Unknown'
  END) AS Gender,
  (CASE tblClaim.ClaimStatus
    WHEN 1 THEN 'Rejected'
    WHEN 2 THEN 'Entered'
    WHEN 4 THEN 'Reviewed'
    WHEN 8 THEN 'Approved'
    WHEN 16 THEN 'Approved'
    ELSE 'Unknown'
  END) AS 'Status',
  tblICDCodes.ICDName AS 'Diagnosis',
  tblItems.ItemName AS 'Item',
  tblServices.ServName AS 'Service',
  SUM(COALESCE(tblClaimItems.RemuneratedAmount, 0))
  + SUM(COALESCE(tblClaimServices.RemuneratedAmount, 0)) AS 'Total paid amount',
  CASE
    WHEN tblClaimItems.RejectionReason = -1 OR tblClaimServices.RejectionReason = -1 THEN 'Rejected by a medical officer'
    WHEN tblClaim.RejectionReason = 0 OR tblClaimItems.RejectionReason = 0 OR tblClaimServices.RejectionReason = 0 THEN 'Accepted'
    WHEN tblClaim.RejectionReason = 1 THEN 'Item/Service not in the registers of medical items/services'
    WHEN tblClaimItems.RejectionReason = 1 THEN 'Item not in the registers of medical items'
    WHEN tblClaimServices.RejectionReason = 1 THEN 'Service not in the registers of medical services'
    WHEN tblClaimItems.RejectionReason = 2 THEN 'Item not in the pricelists associated with the health facility'
    WHEN tblClaimServices.RejectionReason = 2 THEN 'Service not in the pricelists associated with the health facility'
    WHEN tblClaimItems.RejectionReason = 3 THEN 'Item is not covered by an active policy of the patient'
    WHEN tblClaimServices.RejectionReason = 3 THEN 'Service is not covered by an active policy of the patient'
    WHEN tblClaimItems.RejectionReason = 4 THEN 'Item doesn\'t comply with limitations on patients (men/women, adults/children)'
    WHEN tblClaimServices.RejectionReason = 4 THEN 'Service doesn\'t comply with limitations on patients (men/women, adults/children)'
    WHEN tblClaimItems.RejectionReason = 5 THEN 'Item doesn\'t comply with frequency constraint'
    WHEN tblClaimServices.RejectionReason = 5 THEN 'Service doesn\'t comply with frequency constraint'
    WHEN tblClaimItems.RejectionReason = 6 THEN 'Item duplicated'
    WHEN tblClaimServices.RejectionReason = 6 THEN 'Service duplicated'
    WHEN tblClaimItems.RejectionReason = 7 OR tblClaimServices.RejectionReason = 7 THEN 'Not valid insurance number'
    WHEN tblClaimItems.RejectionReason = 8 OR tblClaimServices.RejectionReason = 8 THEN 'Diagnosis code not in the current list of diagnoses'
    WHEN tblClaimItems.RejectionReason = 9 OR tblClaimServices.RejectionReason = 9 THEN 'Target date of provision of health care invalid'
    WHEN tblClaimItems.RejectionReason = 10 THEN 'Item doesn\'t comply with type of care constraint'
    WHEN tblClaimServices.RejectionReason = 10 THEN 'Service doesn\'t comply with type of care constraint'
    WHEN tblClaimItems.RejectionReason = 11 OR tblClaimServices.RejectionReason = 11 THEN 'Maximum number of in-patient admissions exceeded'
    WHEN tblClaimItems.RejectionReason = 12 OR tblClaimServices.RejectionReason = 12 THEN 'Maximum number of out-patient visits exceeded'
    WHEN tblClaimItems.RejectionReason = 13 OR tblClaimServices.RejectionReason = 13 THEN 'Maximum number of consultations exceeded'
    WHEN tblClaimItems.RejectionReason = 14 OR tblClaimServices.RejectionReason = 14 THEN 'Maximum number of surgeries exceeded'
    WHEN tblClaimItems.RejectionReason = 15 OR tblClaimServices.RejectionReason = 15 THEN 'Maximum number of deliveries exceeded'
    WHEN tblClaimItems.RejectionReason = 16 OR tblClaimServices.RejectionReason = 16 THEN 'Maximum number of provisions of item/service exceeded'
    WHEN tblClaimItems.RejectionReason = 17 THEN 'Item cannot be covered within waiting period'
    WHEN tblClaimServices.RejectionReason = 17 THEN 'Service cannot be covered within waiting period'
    WHEN tblClaimItems.RejectionReason = 18 OR tblClaimServices.RejectionReason = 18 THEN 'N/A'
    WHEN tblClaimItems.RejectionReason = 19 OR tblClaimServices.RejectionReason = 19 THEN 'Maximum number of antenatal contacts exceeded'
    ELSE 'Unknown'
  END AS 'Rejection Reason',
  CASE
    WHEN tblClaim.RunID IS NOT NULL THEN 'Yes'
    ELSE 'No'
  END AS 'Paid'
FROM tblClaim
JOIN tblHF ON tblClaim.HfID = tblHF.HfID
JOIN tblLocations AS district_locations ON tblHf.LocationId = district_locations.LocationId
JOIN tblLocations AS region_locations ON district_locations.ParentLocationId = region_locations.LocationId
JOIN tblInsuree ON tblClaim.InsureeID = tblInsuree.InsureeID
JOIN tblICDCodes ON tblClaim.ICDID = tblICDCodes.ICDID
LEFT JOIN tblClaimItems ON tblClaim.ClaimID = tblClaimItems.ClaimID
LEFT JOIN tblClaimServices ON tblClaim.ClaimID = tblClaimServices.ClaimID
LEFT JOIN tblItems ON tblClaimItems.ClaimItemID = tblItems.ItemID
LEFT JOIN tblServices ON tblClaimServices.ServiceID = tblServices.ServiceID
WHERE
  district_locations.LocationType = 'D'
  AND region_locations.LocationType = 'R'
  AND tblClaim.validityTo IS NULL
  AND tblHF.validityTo IS NULL
  AND tblInsuree.validityTo IS NULL
  AND tblICDCodes.validityTo IS NULL
  AND tblClaimItems.validityTo IS NULL
  AND tblClaimServices.validityTo IS NULL
  AND district_locations.validityTo IS NULL
  AND region_locations.validityTo IS NULL
  AND tblItems.validityTo IS NULL
  AND tblServices.validityTo IS NULL
GROUP BY
  region_locations.LocationName,
  district_locations.LocationName,
  tblHF.HFName,
  tblClaim.DateClaimed, 
  tblInsuree.Gender,
  tblClaim.ClaimStatus,
  tblICDCodes.ICDName,
  tblClaimItems.RejectionReason,
  tblClaimServices.RejectionReason,
  tblClaim.RunID,
  tblItems.ItemName,
  tblServices.ServName,
  tblClaim.RejectionReason
"""

SQL_CLAIM_STATISTICS_QUERY_POSTGRESQL = f"""
SELECT
  COUNT("tblClaim"."ClaimID") AS "Number of claims",
  "tblHF"."HFName" AS "Hospital Name",
  "region_locations"."LocationName" AS "Region",
  "district_locations"."LocationName" AS "District",
  CAST("tblClaim"."DateClaimed" AS date) AS "Date",
  (CASE "tblInsuree"."Gender"
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
    ELSE 'Unknown'
  END) AS "Gender",
  (CASE "tblClaim"."ClaimStatus"
    WHEN 1 THEN 'Rejected'
    WHEN 2 THEN 'Entered'
    WHEN 4 THEN 'Reviewed'
    WHEN 8 THEN 'Approved'
    WHEN 16 THEN 'Approved'
    ELSE 'Unknown'
  END) AS "Status",
  "tblICDCodes"."ICDName" AS "Diagnosis",
  "tblItems"."ItemName" AS "Item",
  "tblServices"."ServName" AS "Service",
  SUM(COALESCE("tblClaimItems"."RemuneratedAmount", 0))
  + SUM(COALESCE("tblClaimServices"."RemuneratedAmount", 0)) AS "Total paid amount",
  CASE
    WHEN "tblClaimItems"."RejectionReason" = -1 OR "tblClaimServices"."RejectionReason" = -1 THEN 'Rejected by a medical officer'
    WHEN "tblClaim"."RejectionReason" = 0 OR "tblClaimItems"."RejectionReason" = 0 OR "tblClaimServices"."RejectionReason" = 0 THEN 'Accepted'
    WHEN "tblClaim"."RejectionReason" = 1 THEN 'Item/Service not in the registers of medical items/services'
    WHEN "tblClaimItems"."RejectionReason" = 1 THEN 'Item not in the registers of medical items'
    WHEN "tblClaimServices"."RejectionReason" = 1 THEN 'Service not in the registers of medical services'
    WHEN "tblClaimItems"."RejectionReason" = 2 THEN 'Item not in the pricelists associated with the health facility'
    WHEN "tblClaimServices"."RejectionReason" = 2 THEN 'Service not in the pricelists associated with the health facility'
    WHEN "tblClaimItems"."RejectionReason" = 3 THEN 'Item is not covered by an active policy of the patient'
    WHEN "tblClaimServices"."RejectionReason" = 3 THEN 'Service is not covered by an active policy of the patient'
    WHEN "tblClaimItems"."RejectionReason" = 4 THEN 'Item doesn\'t comply with limitations on patients (men/women, adults/children)'
    WHEN "tblClaimServices"."RejectionReason" = 4 THEN 'Service doesn\'t comply with limitations on patients (men/women, adults/children)'
    WHEN "tblClaimItems"."RejectionReason" = 5 THEN 'Item doesn\'t comply with frequency constraint'
    WHEN "tblClaimServices"."RejectionReason" = 5 THEN 'Service doesn\'t comply with frequency constraint'
    WHEN "tblClaimItems"."RejectionReason" = 6 THEN 'Item duplicated'
    WHEN "tblClaimServices"."RejectionReason" = 6 THEN 'Service duplicated'
    WHEN "tblClaimItems"."RejectionReason" = 7 OR "tblClaimServices"."RejectionReason" = 7 THEN 'Not valid insurance number'
    WHEN "tblClaimItems"."RejectionReason" = 8 OR "tblClaimServices"."RejectionReason" = 8 THEN 'Diagnosis code not in the current list of diagnoses'
    WHEN "tblClaimItems"."RejectionReason" = 9 OR "tblClaimServices"."RejectionReason" = 9 THEN 'Target date of provision of health care invalid'
    WHEN "tblClaimItems"."RejectionReason" = 10 THEN 'Item doesn\'t comply with type of care constraint'
    WHEN "tblClaimServices"."RejectionReason" = 10 THEN 'Service doesn\'t comply with type of care constraint'
    WHEN "tblClaimItems"."RejectionReason" = 11 OR "tblClaimServices"."RejectionReason" = 11 THEN 'Maximum number of in-patient admissions exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 12 OR "tblClaimServices"."RejectionReason" = 12 THEN 'Maximum number of out-patient visits exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 13 OR "tblClaimServices"."RejectionReason" = 13 THEN 'Maximum number of consultations exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 14 OR "tblClaimServices"."RejectionReason" = 14 THEN 'Maximum number of surgeries exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 15 OR "tblClaimServices"."RejectionReason" = 15 THEN 'Maximum number of deliveries exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 16 OR "tblClaimServices"."RejectionReason" = 16 THEN 'Maximum number of provisions of item/service exceeded'
    WHEN "tblClaimItems"."RejectionReason" = 17 THEN 'Item cannot be covered within waiting period'
    WHEN "tblClaimServices"."RejectionReason" = 17 THEN 'Service cannot be covered within waiting period'
    WHEN "tblClaimItems"."RejectionReason" = 18 OR "tblClaimServices"."RejectionReason" = 18 THEN 'N/A'
    WHEN "tblClaimItems"."RejectionReason" = 19 OR "tblClaimServices"."RejectionReason" = 19 THEN 'Maximum number of antenatal contacts exceeded'
    ELSE 'Unknown'
  END AS "Rejection Reason",
  CASE
    WHEN "tblClaim"."RunID" IS NOT NULL THEN 'Yes'
    ELSE 'No'
  END AS "Paid"
FROM "tblClaim"
JOIN "tblHF" ON "tblClaim"."HFID" = "tblHF"."HfID"
JOIN "tblLocations" AS "district_locations" ON "tblHF"."LocationId" = "district_locations"."LocationId"
JOIN "tblLocations" AS "region_locations" ON "district_locations"."ParentLocationId" = "region_locations"."LocationId"
JOIN "tblInsuree" ON "tblClaim"."InsureeID" = "tblInsuree"."InsureeID"
JOIN "tblICDCodes" ON "tblClaim"."ICDID" = "tblICDCodes"."ICDID"
LEFT JOIN "tblClaimItems" ON "tblClaim"."ClaimID" = "tblClaimItems"."ClaimID"
LEFT JOIN "tblClaimServices" ON "tblClaim"."ClaimID" = "tblClaimServices"."ClaimID"
LEFT JOIN "tblItems" ON "tblClaimItems"."ClaimItemID" = "tblItems"."ItemID"
LEFT JOIN "tblServices" ON "tblClaimServices"."ServiceID" = "tblServices"."ServiceID"
WHERE
  "district_locations"."LocationType" = 'D'
  AND "region_locations"."LocationType" = 'R'
  AND "tblClaim"."ValidityTo" IS NULL
  AND "tblHF"."ValidityTo" IS NULL
  AND "tblInsuree"."ValidityTo" IS NULL
  AND "tblICDCodes"."ValidityTo" IS NULL
  AND "tblClaimItems"."ValidityTo" IS NULL
  AND "tblClaimServices"."ValidityTo" IS NULL
  AND "district_locations"."ValidityTo" IS NULL
  AND "region_locations"."ValidityTo" IS NULL
  AND "tblItems"."ValidityTo" IS NULL
  AND "tblServices"."ValidityTo" IS NULL
GROUP BY
  "region_locations"."LocationName",
  "district_locations"."LocationName",
  "tblHF"."HFName",
  "tblClaim"."DateClaimed", 
  "tblInsuree"."Gender",
  "tblClaim"."ClaimStatus",
  "tblICDCodes"."ICDName",
  "tblClaimItems"."RejectionReason",
  "tblClaimServices"."RejectionReason",
  "tblClaim"."RunID",
  "tblItems"."ItemName",
  "tblServices"."ServName",
  "tblClaim"."RejectionReason"
"""

SUPERSET_TABLE_ENROLLMENTS = "openimis-dataset-enrollments"
SUPERSET_TABLE_PAYMENTS = "openimis-dataset-payments"
SUPERSET_TABLE_POPULATION = "openimis-dataset-population"
SUPERSET_TABLE_CLAIM_GENERAL = "openimis-dataset-claim-general"
SUPERSET_TABLE_CLAIM_DETAILS = "openimis-dataset-claim-details"
SUPERSET_TABLE_BILLS = "openimis-dataset-bills"

LABEL_ENROLLMENT_HF = "Health Facility Enrollment"
LABEL_ENROLLMENT_LGA = "Health Facility LGA"
LABEL_ENROLLMENT_PRODUCT = "Product"
LABEL_ENROLLMENT_DATE = "Enrollment Date"
LABEL_ENROLLMENT_AGE = "Insuree Age"
LABEL_ENROLLMENT_GENDER = "Insuree Gender"
LABEL_ENROLLMENT_SECTOR = "Insuree Sector"
LABEL_ENROLLMENT_PAYMENT_AMOUNT = "Payment Amount"
LABEL_ENROLLMENT_POLICY_STATUS = "Policy Status"
LABELS_FOR_ENROLLMENT = [
    LABEL_ENROLLMENT_HF,
    LABEL_ENROLLMENT_LGA,
    LABEL_ENROLLMENT_PRODUCT,
    LABEL_ENROLLMENT_DATE,
    LABEL_ENROLLMENT_AGE,
    LABEL_ENROLLMENT_GENDER,
    LABEL_ENROLLMENT_SECTOR,
    LABEL_ENROLLMENT_PAYMENT_AMOUNT,
    LABEL_ENROLLMENT_POLICY_STATUS,
]

LABEL_ECRVS_LGA = "Insuree LGA"
LABEL_ECRVS_AGE = "Insuree Age"
LABEL_ECRVS_GENDER = "Insuree Gender"
LABEL_ECRVS_DATE = "Insuree Data Reception Date"
LABEL_ECRVS_ALREADY_ENROLLED = "Insuree Already Enrolled"
LABELS_FOR_ECRVS = [
    LABEL_ECRVS_LGA,
    LABEL_ECRVS_AGE,
    LABEL_ECRVS_GENDER,
    LABEL_ECRVS_DATE,
    LABEL_ECRVS_ALREADY_ENROLLED,
]

LABEL_ENROLLMENT_PAYMENT_LGA = "Health Facility LGA"
LABEL_ENROLLMENT_PAYMENT_HF = "Health Facility"
LABEL_ENROLLMENT_PAYMENT_DATE = "Payment Date"
LABEL_ENROLLMENT_PAYMENT_TYPE = "Payment Type"
LABEL_ENROLLMENT_PAYMENT_PRODUCT = "Product"
LABEL_ENROLLMENT_PAYMENT_AMOUNT = "Amount"
LABEL_ENROLLMENT_PAYMENT_SECTOR = "Insuree Sector"
LABELS_FOR_ENROLLMENT_PAYMENTS = [
    LABEL_ENROLLMENT_PAYMENT_LGA,
    LABEL_ENROLLMENT_PAYMENT_HF,
    LABEL_ENROLLMENT_PAYMENT_DATE,
    LABEL_ENROLLMENT_PAYMENT_TYPE,
    LABEL_ENROLLMENT_PAYMENT_PRODUCT,
    LABEL_ENROLLMENT_PAYMENT_AMOUNT,
    LABEL_ENROLLMENT_PAYMENT_SECTOR,
]

LABEL_CLAIM_GENERAL_HF = "Health Facility"
LABEL_CLAIM_GENERAL_HF_LGA = "Health Facility LGA"
LABEL_CLAIM_GENERAL_ADMIN = "Claim Admin"
LABEL_CLAIM_GENERAL_STATUS = "Claim Status"
LABEL_CLAIM_GENERAL_ICD = "Diagnosis"
LABEL_CLAIM_GENERAL_GENDER = "Insuree Gender"
LABEL_CLAIM_GENERAL_AGE = "Insuree Age"
LABEL_CLAIM_GENERAL_PRODUCT = "Product"
LABEL_CLAIM_GENERAL_REJECTION = "Claim Rejection Reason"
LABEL_CLAIM_GENERAL_CLAIMED_DATE = "Claimed Date"
LABEL_CLAIM_GENERAL_SUBMISSION_DATE = "Submission Date"
LABEL_CLAIM_GENERAL_PROCESSED_DATE = "Processed Date"
LABEL_CLAIM_GENERAL_BILL_DATE = "Bill Date"
LABEL_CLAIM_GENERAL_AMOUNT_REQUESTED = "Amount Requested"
LABEL_CLAIM_GENERAL_AMOUNT_APPROVED = "Amount Approved"
LABEL_CLAIM_GENERAL_AMOUNT_PAID = "Amount Paid"
LABEL_CLAIM_GENERAL_NUMBER_ITEMS = "Number of Items"
LABEL_CLAIM_GENERAL_NUMBER_ITEMS_REJECTED = "Number of Rejected Items"
LABEL_CLAIM_GENERAL_NUMBER_ITEMS_APPROVED = "Number of Approved Items"
LABEL_CLAIM_GENERAL_NUMBER_SERVICES = "Number of Services"
LABEL_CLAIM_GENERAL_NUMBER_SERVICES_REJECTED = "Number of Rejected Services"
LABEL_CLAIM_GENERAL_NUMBER_SERVICES_APPROVED = "Number of Approved Services"
LABELS_FOR_GENERAL_CLAIMS = [
    LABEL_CLAIM_GENERAL_HF,
    LABEL_CLAIM_GENERAL_HF_LGA,
    LABEL_CLAIM_GENERAL_ADMIN,
    LABEL_CLAIM_GENERAL_STATUS,
    LABEL_CLAIM_GENERAL_ICD,
    LABEL_CLAIM_GENERAL_GENDER,
    LABEL_CLAIM_GENERAL_AGE,
    LABEL_CLAIM_GENERAL_PRODUCT,
    LABEL_CLAIM_GENERAL_REJECTION,
    LABEL_CLAIM_GENERAL_CLAIMED_DATE,
    LABEL_CLAIM_GENERAL_SUBMISSION_DATE,
    LABEL_CLAIM_GENERAL_PROCESSED_DATE,
    LABEL_CLAIM_GENERAL_BILL_DATE,
    LABEL_CLAIM_GENERAL_AMOUNT_REQUESTED,
    LABEL_CLAIM_GENERAL_AMOUNT_APPROVED,
    LABEL_CLAIM_GENERAL_AMOUNT_PAID,
    LABEL_CLAIM_GENERAL_NUMBER_ITEMS,
    LABEL_CLAIM_GENERAL_NUMBER_ITEMS_REJECTED,
    LABEL_CLAIM_GENERAL_NUMBER_ITEMS_APPROVED,
    LABEL_CLAIM_GENERAL_NUMBER_SERVICES,
    LABEL_CLAIM_GENERAL_NUMBER_SERVICES_REJECTED,
    LABEL_CLAIM_GENERAL_NUMBER_SERVICES_APPROVED,
]

LABEL_CLAIM_DETAILS_NAME = "Name"
LABEL_CLAIM_DETAILS_TYPE = "Type"
LABEL_CLAIM_DETAILS_STATUS = "Item/Service Status"
LABEL_CLAIM_DETAILS_REJECTION = "Rejection Reason"
LABEL_CLAIM_DETAILS_CLAIM_ID = "Claim ID"
LABEL_CLAIM_DETAILS_CLAIM_STATUS = "Claim Status"
LABEL_CLAIM_DETAILS_CLAIM_ICD = "Claim Diagnosis"
LABEL_CLAIM_DETAILS_HF = "Health Facility"
LABEL_CLAIM_DETAILS_HF_LGA = "Health Facility LGA"
LABEL_FOR_CLAIM_DETAILS = [
    LABEL_CLAIM_DETAILS_NAME,
    LABEL_CLAIM_DETAILS_TYPE,
    LABEL_CLAIM_DETAILS_STATUS,
    LABEL_CLAIM_DETAILS_REJECTION,
    LABEL_CLAIM_DETAILS_CLAIM_ID,
    LABEL_CLAIM_DETAILS_CLAIM_STATUS,
    LABEL_CLAIM_DETAILS_CLAIM_ICD,
    LABEL_CLAIM_DETAILS_HF,
    LABEL_CLAIM_DETAILS_HF_LGA,
]

LABEL_BILL_HF = "Health Facility"
LABEL_BILL_HF_LGA = "Health Facility LGA"
LABEL_BILL_AMOUNT = "Amount"
LABEL_BILL_CREATION_DATE = "Creation Date"
LABEL_BILL_MONTH = "Bill Month"
LABEL_BILL_YEAR = "Bill Year"
LABEL_BILL_PRODUCT = "Bill Product"
LABELS_FOR_BILLS = [
    LABEL_BILL_HF,
    LABEL_BILL_HF_LGA,
    LABEL_BILL_AMOUNT,
    LABEL_BILL_CREATION_DATE,
    LABEL_BILL_MONTH,
    LABEL_BILL_YEAR,
    LABEL_BILL_PRODUCT,

]

REJECTION_REASONS = {
    -1: "Rejected by the reviewer",
    0: "Accepted",
    1: "Unknown Item/Service",
    2: "Item/Service not in the pricelists associated with the health facility",
    3: "Item/Service not covered by an active policy of the patient",
    4: "Item/Service doesn't comply with limitations on patients (men/women, adults/children)",
    5: "Item/Service doesn't comply with frequency constraint",
    6: "Item/Service duplicated",
    7: "Invalid Insuree NIN",
    8: "Unknown diagnosis",
    9: "Invalid admission/release dates",
    10: "Item/Service doesn't comply with type of care constraint",
    11: "Maximum number of in-patient admissions exceeded",
    12: "Maximum number of out-patient visits exceeded",
    13: "Maximum number of consultations exceeded",
    14: "Maximum number of surgeries exceeded",
    15: "Maximum number of deliveries exceeded",
    16: "Maximum number of provisions of Item/Service exceeded",
    17: "Item/Service cannot be covered within waiting period",
    18: "N/A",
    19: "Maximum number of antenatal contacts exceeded",
}

GENDERS = {
    "M": "Male",
    "F": "Female",
    "O": "Other",
}

PAYMENT_TYPES = {
    "B": "Bank transfer",
    "C": "Cash",
    "F": "Funding",
    "M": "Mobile payment",
}

POLICY_STATUSES = {
    Policy.STATUS_IDLE: "Idle",
    Policy.STATUS_ACTIVE: "Active",
    Policy.STATUS_EXPIRED: "Expired",
    Policy.STATUS_SUSPENDED: "Suspended",
}

CLAIM_STATUSES = {
    Claim.STATUS_ENTERED: "Entered",
    Claim.STATUS_REJECTED: "Rejected",
    Claim.STATUS_CHECKED: "Checked",
    Claim.STATUS_VALUATED: "Valuated",
    Claim.STATUS_PROCESSED: "Processed",
}

CLAIM_DETAIL_STATUSES = {
    ClaimDetail.STATUS_PASSED: "Passed",
    ClaimDetail.STATUS_REJECTED: "Rejected",
}

PAGE_SIZE_FETCH_POLICIES = 1000
PAGE_SIZE_FETCH_POPULATION = 1000
PAGE_SIZE_FETCH_PAYMENTS = 1000
PAGE_SIZE_FETCH_GENERAL_CLAIMS = 1000
PAGE_SIZE_FETCH_CLAIM_DETAILS = 1000
PAGE_SIZE_FETCH_BILLS = 1000

UNKNOWN = "Unknown"


def map_dtype(dtype) -> TypeEngine:
    """
    Map pandas dtype to SQLAlchemy types.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return DOUBLE_PRECISION
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        # You can choose between Date and DateTime based on your needs
        return Date
    else:
        return String


def create_table_from_dataframe(engine, table_name: str, df: pd.DataFrame):
    """
    Create a table in the PostgreSQL database based on the DataFrame's schema.
    """
    metadata = MetaData()
    columns = []
    for column_name, dtype in zip(df.columns, df.dtypes):
        col_type = map_dtype(dtype)
        # You can add more constraints here (e.g., primary_key, nullable)
        columns.append(Column(column_name, col_type))
    Table(table_name, metadata, *columns)
    metadata.create_all(engine)  # Creates the table
    logger.info(f"\tTable '{table_name}' created successfully.")


def sum_premiums(policy: Policy):
    return sum(
        [p.amount
         for p in policy.premiums.filter(validity_to__isnull=True, is_photo_fee=False).only("amount")
         ]
    )


def find_product_name(product_info: dict, claim: Claim, items: QuerySet, services: QuerySet):
    if claim.status in (Claim.STATUS_ENTERED, Claim.STATUS_REJECTED):
        return None
    for item in items:
        if item["product_id"]:
            return product_info[item["product_id"]]
    for service in services:
        if service["product_id"]:
            return product_info[service["product_id"]]
    return None


def count_detail_detail(claim_details: QuerySet):
    total = 0
    approved = 0
    rejected = 0
    for claim_detail in claim_details:
        total += 1
        if claim_detail["status"] == ClaimDetail.STATUS_PASSED:
            approved += 1
        elif claim_detail["status"] == ClaimDetail.STATUS_REJECTED:
            rejected += 1
    return total, approved, rejected


def fetch_product_code_from_bill_code(bill_code: str):
    splits = bill_code.split("-")
    return splits[1]


def write_data_to_superset(dataframe: DataFrame, table_name: str, credentials: dict):
    logger.info(f"\tWriting data to Superset - table {table_name}")
    user = credentials["user"]
    password = credentials["password"]
    host = credentials["host"]
    name = credentials["name"]
    engine = create_engine(f"postgresql://{user}:{password}@{host}:5432/{name}")
    create_table_from_dataframe(engine, table_name=table_name, df=dataframe)
    dataframe.to_sql(table_name,
                     con=engine,
                     if_exists="replace",
                     index_label="id")


def process_enrollment_data(mapping_hfs: dict, superset_credentials: dict):
    logger.info(f"*** Fetching enrollment data with batches of {PAGE_SIZE_FETCH_POLICIES} lines ***")
    policies = (Policy.objects.filter(validity_to__isnull=True)
                              .select_related("product", "family__head_insuree")
                              .prefetch_related("premiums")
                              .order_by("id"))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABELS_FOR_ENROLLMENT)  # Write header

        paginator = Paginator(policies, PAGE_SIZE_FETCH_POLICIES)
        for page_number in paginator.page_range:
            page = paginator.page(page_number)
            logger.info(f"\tProcessing batch policies {page_number}")

            data = []
            for policy in page.object_list:
                product = policy.product
                insuree = policy.family.head_insuree
                hf = mapping_hfs.get(policy.officer_id, None)
                data.append([
                    # Mind the order here, based on the order in LABELS_FOR_ENROLLMENT
                    hf["name"] if hf else UNKNOWN,  # Corresponds to LABEL_ENROLLMENT_HF
                    hf["lga"] if hf else UNKNOWN,  # Corresponds to LABEL_ENROLLMENT_LGA
                    f"{product.code} - {product.name}",  # Corresponds to LABEL_ENROLLMENT_PRODUCT
                    policy.enroll_date,  # Corresponds to LABEL_ENROLLMENT_DATE
                    insuree.age(policy.enroll_date),  # Corresponds to LABEL_ENROLLMENT_AGE
                    GENDERS.get(insuree.gender_id, UNKNOWN),  # Corresponds to LABEL_ENROLLMENT_GENDER
                    "Formal" if insuree.is_formal_sector else "Informal",  # Corresponds to LABEL_ENROLLMENT_SECTOR
                    sum_premiums(policy),  # Corresponds to LABEL_ENROLLMENT_PAYMENT_AMOUNT
                    POLICY_STATUSES.get(policy.status, UNKNOWN),  # Corresponds to LABEL_ENROLLMENT_POLICY_STATUS
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_ENROLLMENTS,
                               credentials=superset_credentials)
        logger.info(f"*** Enrollment data successfully uploaded! ***")


def process_ecrvs_data(mapping_villages: dict,  superset_credentials: dict):
    logger.info(f"*** Fetching population data with batches of {PAGE_SIZE_FETCH_POPULATION} lines ***")
    insurees = (Insuree.objects.filter(validity_to__isnull=True)
                               .select_related("family")
                               .prefetch_related("insuree_policies")
                               .order_by("id"))
    ips = set((InsureePolicy.objects.filter(validity_to__isnull=True, insuree__in=insurees).values_list("insuree_id", flat=True)))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABELS_FOR_ECRVS)  # Write header

        paginator = Paginator(insurees, PAGE_SIZE_FETCH_POPULATION)
        for page_number in paginator.page_range:
            page = paginator.page(page_number)
            logger.info(f"\tProcessing batch insurees {page_number}")

            data = []
            for insuree in page.object_list:
                data.append([
                    # Mind the order here, based on the order in LABELS_FOR_ECRVS
                    mapping_villages.get(insuree.family.location_id, UNKNOWN),  # Corresponds to LABEL_ECRVS_LGA
                    insuree.age(),  # Corresponds to LABEL_ECRVS_AGE
                    GENDERS.get(insuree.gender_id, UNKNOWN),  # Corresponds to LABEL_ECRVS_GENDER
                    insuree.validity_from.date(),  # Corresponds to LABEL_ECRVS_DATE
                    insuree.id in ips  # Corresponds to LABEL_ECRVS_ALREADY_ENROLLED
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_POPULATION,
                               credentials=superset_credentials)
        logger.info(f"*** Enrollment data successfully uploaded! ***")


def process_enrollment_payment_data(mapping_hfs: dict, product_info: dict, superset_credentials: dict):
    logger.info(f"*** Fetching enrollment payment data with batches of {PAGE_SIZE_FETCH_PAYMENTS} lines ***")
    premiums = (Premium.objects.filter(validity_to__isnull=True)
                               .select_related("policy", "policy__family__head_insuree")
                               .order_by("id"))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABELS_FOR_ENROLLMENT_PAYMENTS)  # Write header

        paginator = Paginator(premiums, PAGE_SIZE_FETCH_PAYMENTS)
        for page_number in paginator.page_range:
            page = paginator.page(page_number)
            logger.info(f"\tProcessing batch enrollment payments {page_number}")

            data = []
            for premium in page.object_list:
                policy = premium.policy
                insuree = policy.family.head_insuree
                hf = mapping_hfs.get(policy.officer_id, None)
                data.append([
                    # Mind the order here, based on the order in LABELS_FOR_ENROLLMENT_PAYMENTS
                    hf["lga"] if hf else UNKNOWN,  # LABEL_ENROLLMENT_PAYMENT_LGA,
                    hf["name"] if hf else UNKNOWN,  # LABEL_ENROLLMENT_PAYMENT_HF,
                    premium.pay_date,  # LABEL_ENROLLMENT_PAYMENT_DATE,
                    PAYMENT_TYPES.get(premium.pay_type, UNKNOWN),  # LABEL_ENROLLMENT_PAYMENT_TYPE,
                    product_info[policy.product_id],  # LABEL_ENROLLMENT_PAYMENT_PRODUCT,
                    premium.amount,  # LABEL_ENROLLMENT_PAYMENT_AMOUNT,
                    "Formal" if insuree.is_formal_sector else "Informal",  # LABEL_ENROLLMENT_PAYMENT_SECTOR,
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_PAYMENTS,
                               credentials=superset_credentials)
        logger.info(f"*** Enrollment payments data successfully uploaded! ***")



def process_general_claim_data(hf_information: dict, product_info: dict, icd_info: dict, superset_credentials: dict):
    logger.info(f"*** Fetching general claim data with batches of {PAGE_SIZE_FETCH_GENERAL_CLAIMS} lines ***")

    claims = (Claim.objects.filter(validity_to__isnull=True)
                           .select_related("batch_run", "insuree", "admin")
                           .prefetch_related("items", "services")
                           .order_by("id"))

    items = (ClaimItem.objects.filter(validity_to__isnull=True)
                              .values("claim_id", "status", "product_id"))

    services = (ClaimService.objects.filter(validity_to__isnull=True)
                                    .values("claim_id", "status", "product_id"))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABELS_FOR_GENERAL_CLAIMS)  # Write header

        paginator = Paginator(claims, PAGE_SIZE_FETCH_GENERAL_CLAIMS)
        for page_number in paginator.page_range:
            page = paginator.page(page_number)
            logger.info(f"\tProcessing batch general claims {page_number}")

            data = []
            for claim in page.object_list:
                insuree = claim.insuree
                claim_admin = claim.admin
                claim_items = items.filter(claim_id=claim.id)
                claim_services = services.filter(claim_id=claim.id)
                # items = claim.items.filter(validity_to__isnull=True).only("status", "product_id")
                # services = claim.services.filter(validity_to__isnull=True).only("status", "product_id")
                product_name = find_product_name(product_info, claim, claim_items, claim_services)
                total_items, approved_items, rejected_items = count_detail_detail(claim_items)
                total_services, approved_services, rejected_services = count_detail_detail(claim_services)
                data.append([
                    # Mind the order here, based on the order in LABELS_FOR_ECRVS
                    hf_information[claim.health_facility_id]["name"],  # Corresponds to LABEL_CLAIM_GENERAL_HF
                    hf_information[claim.health_facility_id]["lga"],  # Corresponds to LABEL_CLAIM_GENERAL_HF_LGA
                    f"{claim_admin.other_names} {claim_admin.last_name}",  # Corresponds to LABEL_CLAIM_GENERAL_ADMIN
                    CLAIM_STATUSES.get(claim.status, UNKNOWN),  # Corresponds to LABEL_CLAIM_GENERAL_STATUS
                    icd_info[claim.icd_id],  # Corresponds to LABEL_CLAIM_GENERAL_ICD
                    GENDERS.get(insuree.gender_id, UNKNOWN),  # Corresponds to LABEL_CLAIM_GENERAL_GENDER
                    insuree.age(),  # Corresponds to LABEL_CLAIM_GENERAL_AGE
                    product_name,  # Corresponds to LABEL_CLAIM_GENERAL_PRODUCT
                    REJECTION_REASONS.get(claim.rejection_reason, UNKNOWN),  # Corresponds to LABEL_CLAIM_GENERAL_REJECTION
                    claim.date_claimed if claim.date_claimed else None,  # Corresponds to LABEL_CLAIM_GENERAL_CLAIMED_DATE
                    claim.submit_stamp.date() if claim.submit_stamp else None,  # Corresponds to LABEL_CLAIM_GENERAL_SUBMISSION_DATE
                    claim.process_stamp.date() if claim.process_stamp else None,  # Corresponds to LABEL_CLAIM_GENERAL_PROCESSED_DATE
                    claim.batch_run.run_date.date() if claim.batch_run else None,  # Corresponds to LABEL_CLAIM_GENERAL_BILL_DATE

                    claim.claimed,  # Corresponds to LABEL_CLAIM_GENERAL_AMOUNT_REQUESTED
                    claim.approved if claim.approved else 0.00,  # Corresponds to LABEL_CLAIM_GENERAL_AMOUNT_APPROVED
                    claim.remunerated if claim.remunerated else 0.00,  # Corresponds to LABEL_CLAIM_GENERAL_AMOUNT_PAID

                    total_items,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_ITEMS
                    rejected_items,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_ITEMS_REJECTED
                    approved_items,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_ITEMS_APPROVED

                    total_services,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_SERVICES
                    rejected_services,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_SERVICES_REJECTED
                    approved_services,  # Corresponds to LABEL_CLAIM_GENERAL_NUMBER_SERVICES_APPROVED
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_CLAIM_GENERAL,
                               credentials=superset_credentials)
        logger.info(f"*** General claim data successfully uploaded! ***")


def process_claim_details_data(hf_information: dict, icd_info: dict, superset_credentials: dict):
    logger.info(f"*** Fetching claim details data with batches of {PAGE_SIZE_FETCH_CLAIM_DETAILS} lines ***")

    items = (ClaimItem.objects.filter(validity_to__isnull=True)
                              .select_related("claim", "item")
                              .order_by("id"))

    services = (ClaimService.objects.filter(validity_to__isnull=True)
                                    .select_related("claim", "service")
                                    .order_by("id"))

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABEL_FOR_CLAIM_DETAILS)  # Write header

        # Doing first Items
        items_paginator = Paginator(items, PAGE_SIZE_FETCH_CLAIM_DETAILS)
        for page_number in items_paginator.page_range:
            page = items_paginator.page(page_number)
            logger.info(f"\tProcessing batch claim details - items {page_number}")

            data = []
            for item in page.object_list:
                claim = item.claim
                data.append([
                    # Mind the order here, based on the order in LABEL_FOR_CLAIM_DETAILS
                    item.item.name,  # LABEL_CLAIM_DETAILS_NAME,
                    "Item", # LABEL_CLAIM_DETAILS_TYPE,
                    CLAIM_DETAIL_STATUSES.get(item.status, UNKNOWN),  # LABEL_CLAIM_DETAILS_STATUS,
                    REJECTION_REASONS.get(item.rejection_reason, UNKNOWN),  # LABEL_CLAIM_DETAILS_REJECTION,
                    claim.id,  # LABEL_CLAIM_DETAILS_CLAIM_ID,
                    CLAIM_STATUSES.get(claim.status, UNKNOWN),  # LABEL_CLAIM_DETAILS_CLAIM_STATUS,
                    icd_info[claim.icd_id],  # LABEL_CLAIM_DETAILS_CLAIM_ICD,
                    hf_information[claim.health_facility_id]["name"],  # Corresponds to LABEL_CLAIM_DETAILS_HF
                    hf_information[claim.health_facility_id]["lga"],  # Corresponds to LABEL_CLAIM_DETAILS_HF_LGA
                ])

            # Writing
            writer.writerows(data)

        # Then, instead of uploading, doing Services
        services_paginator = Paginator(services, PAGE_SIZE_FETCH_CLAIM_DETAILS)
        for page_number in services_paginator.page_range:
            page = services_paginator.page(page_number)
            logger.info(f"\tProcessing batch claim details - services {page_number}")

            data = []
            for service in page.object_list:
                claim = service.claim
                data.append([
                    # Mind the order here, based on the order in LABEL_FOR_CLAIM_DETAILS
                    service.service.name,  # LABEL_CLAIM_DETAILS_NAME,
                    "Service", # LABEL_CLAIM_DETAILS_TYPE,
                    CLAIM_DETAIL_STATUSES.get(service.status, UNKNOWN),  # LABEL_CLAIM_DETAILS_STATUS,
                    REJECTION_REASONS.get(service.rejection_reason, UNKNOWN),  # LABEL_CLAIM_DETAILS_REJECTION,
                    claim.id,  # LABEL_CLAIM_DETAILS_CLAIM_ID,
                    CLAIM_STATUSES.get(claim.status, UNKNOWN),  # LABEL_CLAIM_DETAILS_CLAIM_STATUS,
                    icd_info[claim.icd_id],  # LABEL_CLAIM_DETAILS_CLAIM_ICD,
                    hf_information[claim.health_facility_id]["name"],  # Corresponds to LABEL_CLAIM_DETAILS_HF
                    hf_information[claim.health_facility_id]["lga"],  # Corresponds to LABEL_CLAIM_DETAILS_HF_LGA
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_CLAIM_DETAILS,
                               credentials=superset_credentials)
        logger.info(f"*** Claim details data successfully uploaded! ***")


def process_bill_data(hf_information: dict, superset_credentials: dict):
    logger.info(f"*** Fetching bill data with batches of {PAGE_SIZE_FETCH_BILLS} lines ***")

    bills = Bill.objects.filter(is_deleted=False).order_by("id")

    BATCH_RUN_INFORMATION = {}
    batch_runs = BatchRun.objects.filter(validity_to__isnull=True).only("id", "run_year", "run_month")
    for br in batch_runs:
        BATCH_RUN_INFORMATION[br.id] = {
            "year": br.run_year,
            "month": br.run_month,
        }

    PRODUCT_INFORMATION = {}
    products = Product.objects.filter(validity_to__isnull=True).only("code", "name")
    for product in products:
        PRODUCT_INFORMATION[product.code] = product.name

    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        writer = csv.writer(temp_file)
        writer.writerow(LABELS_FOR_BILLS)  # Write header

        paginator = Paginator(bills, PAGE_SIZE_FETCH_BILLS)
        for page_number in paginator.page_range:
            page = paginator.page(page_number)
            logger.info(f"\tProcessing batch bill {page_number}")

            data = []
            for bill in page.object_list:
                hf_id = int(bill.thirdparty_id)
                batch_run_id = int(bill.subject_id)
                product_code = fetch_product_code_from_bill_code(bill.code)
                data.append([
                    # Mind the order here, based on the order in LABEL_FOR_CLAIM_DETAILS
                    hf_information[hf_id]["name"],  # LABEL_BILL_HF,
                    hf_information[hf_id]["lga"],  # LABEL_BILL_HF_LGA,
                    bill.amount_net,  # LABEL_BILL_AMOUNT,
                    bill.date_bill,  # LABEL_BILL_CREATION_DATE,
                    BATCH_RUN_INFORMATION[batch_run_id]["month"],  # LABEL_BILL_MONTH,
                    BATCH_RUN_INFORMATION[batch_run_id]["year"],  # LABEL_BILL_YEAR,
                    PRODUCT_INFORMATION[product_code],  # LABEL_BILL_PRODUCT,
                ])

            # Writing
            writer.writerows(data)

        # Finishing writing to CSV
        temp_file.flush()
        temp_file.seek(0)

        df = pd.read_csv(temp_file.name)
        write_data_to_superset(df,
                               SUPERSET_TABLE_BILLS,
                               credentials=superset_credentials)
        logger.info(f"*** Bill data successfully uploaded! ***")


class Command(BaseCommand):
    help = "Exports data to superset"

    def handle(self, *args, **options):
        logger.info("Preparing a new CSV export for superset")

        logger.info("*** Preparing Superset connection information ***")
        superset_credentials = {
            "user": os.environ["SUPERSET_USER"],
            "password": os.environ["SUPERSET_PASSWORD"],
            "host": os.environ["SUPERSET_HOST"],
            "name": os.environ["SUPERSET_DATABASE"],
        }

        LGA_INFORMATION = {}
        lgas = Location.objects.filter(validity_to__isnull=True, type="D")
        for lga in lgas:
            LGA_INFORMATION[lga.id] = lga.name

        # Preparing a mapping to avoid fetching all data from the DB later on + because the foreign key is stored as int and not FK
        logger.info("*** Preparing User HF information ***")
        users = InteractiveUser.objects.filter(validity_to__isnull=True, health_facility_id__isnull=False)
        MAPPING_USERS_TO_HFS = {}
        for user in users:
            hf = HealthFacility.objects.get(id=user.health_facility_id)
            MAPPING_USERS_TO_HFS[user.id] = {
                "id": hf.id,
                "name": hf.name,
                "lga": LGA_INFORMATION[hf.location_id],
            }

        # Preparing a mapping to avoid fetching all data from the DB later on
        MAPPING_VILLAGES_TO_DISTRICTS = {}
        villages = Location.objects.filter(validity_to__isnull=True, type="V").select_related("parent")
        for village in villages:
            MAPPING_VILLAGES_TO_DISTRICTS[village.id] = LGA_INFORMATION[village.parent.parent_id]

        # Preparing once HF data in order to avoid fetching it from the DB later on
        HF_INFORMATION = {}
        hfs = HealthFacility.objects.filter(validity_to__isnull=True).select_related("location")
        for hf in hfs:
            HF_INFORMATION[hf.id] = {
                "name": hf.name,
                "lga": LGA_INFORMATION[hf.location_id],
            }

        PRODUCT_INFO = {}
        products = Product.objects.filter(validity_to__isnull=True)
        for product in products:
            PRODUCT_INFO[product.id] = product.name

        ICD_INFORMATION = {}
        icds = Diagnosis.objects.filter(validity_to__isnull=True)
        for icd in icds:
            ICD_INFORMATION[icd.id] = icd.name

        process_enrollment_data(MAPPING_USERS_TO_HFS, superset_credentials)
        process_ecrvs_data(MAPPING_VILLAGES_TO_DISTRICTS, superset_credentials)
        process_enrollment_payment_data(MAPPING_USERS_TO_HFS, PRODUCT_INFO, superset_credentials)
        process_general_claim_data(HF_INFORMATION, PRODUCT_INFO, ICD_INFORMATION, superset_credentials)
        process_claim_details_data(HF_INFORMATION, ICD_INFORMATION, superset_credentials)
        process_bill_data(HF_INFORMATION, superset_credentials)

        logger.info("Data upload to Superset is done!")
