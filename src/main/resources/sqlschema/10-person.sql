-- -*- mode: sql; sql-product: ms; -*-

CREATE TABLE PERSON (
    ID                                  NVARCHAR(4000) NOT NULL PRIMARY KEY,
    REV                                 NVARCHAR(34) NOT NULL,
    "NAME"                              NVARCHAR(4000),
    FIRST_NAME                          NVARCHAR(4000),
    LAST_NAME                           NVARCHAR(4000),
    COMMON_NAME                         NVARCHAR(4000),
    DISPLAY_NAME                        NVARCHAR(4000),
    NOTES                               NVARCHAR(4000),
    SEX                                 NVARCHAR(4000),
    PHONE                               NVARCHAR(4000),
    ALTERNATE_PHONE                     NVARCHAR(4000),
    PHONE_OWNER                         NVARCHAR(4000),
    /* No FK: Could reference clinic, health_center, or
       district_hospital. */
    PARENT                              NVARCHAR(4000),
    "ROLE"                              NVARCHAR(4000),
    REPORTED_DATE                       datetime2(3),
    EXTERNAL_ID                         NVARCHAR(4000),
    FACILITY_ID                         int,
    PATIENT_ID                          int,
    LINK_FACILITY                       NVARCHAR(4000),
    COHORT_NUMBER                       NVARCHAR(4000),
    DATE_OF_GRADUATION                  date,
    /* This pulls from date_of_birth, dob_iso, and
       ephemeral_dob->dob_iso. */
    DATE_OF_BIRTH                       date,

    /* Skipping the other age attributes for now, as they are a mess.
     * person_age_in_months is inclusive of years (i.e., a 3-year-old
     * would be between 36 and 48). But person_age_months can be
     * inclusive or exclusive. How to tell which? Just take it mod 12? */

    PREGNANT_AT_REGISTRATION            bit,
    FP_ELIGIBLE                         bit,
    DELIVERED_IN_FACILITY               bit,
    EXCLUSIVELY_BREASTFED               bit,
    SLEPT_UNDER_TREATED_NET             bit,
    /* Pulls from relationship_to_primary_caregiver and
     * relationship_primary_caregiver. */
    RELATIONSHIP_TO_PRIMARY_CAREGIVER   VARCHAR(10),
    RELATIONSHIP_PC_OTHER               VARCHAR(4000),
    CANT_PAY_NOW                        bit,
    FP_G_EDD                            date,
    FP_G_LMP_METHOD                     NVARCHAR(4000),
    FP_G_LMP_APPROX                     integer,
    FP_CURRENT_METHOD                   NVARCHAR(4000),
    FP_WHY_STOP                         NVARCHAR(4000),
    FP_DAYS_SINCE_LMP                   FLOAT,
    FP_G_LMP_CALENDAR                   date,
    FP_BREASTFEEDING                    bit,
    FP_NEW_FP_METHOD                    NVARCHAR(4000),
    FP_TAKE_ON_SWITCH                   bit,
    FP_USED_MODERN_FP                   bit,
    FP_USING_MODERN_FP                  bit,
    FP_IN_THE_HOUSEHOLD                 bit,
    FP_SPOUSE_INCLUDED_HES              bit,
    FP_BREASTFEEDING_DURATION           int,
    FP_MOST_RECENT_METHOD               NVARCHAR(4000),
    FP_PREGNANCY_TEST                   NVARCHAR(4000),
    FP_CONFIRMED_PREGNANCY              bit,
    /* Pulls from fp_screening.g_fp_method_administered and just
     * fp_method_administered. */
    FP_METHOD_ADMINISTERED              NVARCHAR(4000),
    /* Pulls from fp_screening.g_lmp_date_8601 and just
     * lmp_date. */    
    LMP_DATE                            date,
    ON_PILL                             bit,
    ON_INJECTION                        bit,
    NEWLY_ON_LAM                        bit,
    NEWLY_ON_PILL                       bit,
    NEWLY_ON_INJECTION                  bit,
    ELIGIBLE_WOMAN                      bit,
    PARTNER_REFUSE                      bit,
    UNPROTECTED_SEX                     int,
    HAS_RISK_FACTORS                    bit,
    IS_REFERRAL_CASE                    bit,
    FEAR_SIDE_EFFECTS                   bit,
    /* Pulls from fp_screening.reason_not_switch and just
    reason_not_switch (they should be identical). */
    REASON_NOT_SWITCH                   NVARCHAR(4000),
    NEED_MORE_INFORMATION               bit,
    MOTHER_ATTENDED_ANC                 bit,
    COMMUNITY_UNIT                      NVARCHAR(4000),
    MARITAL_STATUS                      NVARCHAR(4000),
    IS_IN_EMNCH_PILOT                   BIT,
    IS_IN_DISPERSIBLE_AMOXICILLIN_PILOT BIT,
    IS_IN_ECD_PILOT                     BIT,
    IS_IN_RBF_PILOT                     BIT,
    IS_IN_CDBS_PILOT                    BIT,
    IS_IN_FP_PILOT                      BIT,
    MOTHER_HIV_STATUS                   NVARCHAR(4000),
    TOTAL_SCORE                         int,
    NATIONAL_ID_NUMBER                  NVARCHAR(4000)
);

CREATE TABLE PERSON_FP_RISK_FACTORS (
    PERSON_ID                           NVARCHAR(4000)
        NOT NULL REFERENCES PERSON (ID),
    RISK_FACTOR                         NVARCHAR(4000) NOT NULL,
    PRIMARY KEY (PERSON_ID, RISK_FACTOR)
);
