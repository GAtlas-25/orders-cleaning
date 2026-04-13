import streamlit as st
import pandas as pd
import numpy as np
import io

# -----------------------------------------------------------
# PAGE CONFIG
# -----------------------------------------------------------
st.set_page_config(
    page_title="SAP Order Cleaning Tool",
    page_icon="📦",
    layout="wide"
)

# -----------------------------------------------------------
# LOAD REFERENCE FILE
# -----------------------------------------------------------
LTL_QTY_PATH = "LTL_qty_updated.xlsx"

@st.cache_data
def load_ltl_qty():
    return pd.read_excel(LTL_QTY_PATH)

# -----------------------------------------------------------
# HELPERS
# -----------------------------------------------------------
def make_row_key(df):
    return (
        df['Purchase order no.'].astype(str).fillna('') + '|' +
        df['Status'].astype(str).fillna('') + '|' +
        df['Orig'].astype(str).fillna('')
    )

def to_excel_bytes(df, sheet_name="Sheet1"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()

def get_approved_rows(df_with_checks):
    if df_with_checks is None or df_with_checks.empty or 'Approve' not in df_with_checks.columns:
        return pd.DataFrame()

    approved = df_with_checks[df_with_checks['Approve'] == True].copy()
    if approved.empty:
        return approved

    approved = approved.drop(columns=['Approve'])
    return approved

# -----------------------------------------------------------
# STEP 1 - PROCESS SAP ORDER EXPORT
# -----------------------------------------------------------
def process_order_export(files, ltl_qty_df):
    dfs = []
    for file in files:
        df = pd.read_excel(file)
        dfs.append(df)

    df_order_export = pd.concat(dfs, ignore_index=True)

    # Filter out RDC orders
    df_order_export = df_order_export[
        ~df_order_export['Name 1'].astype(str).str.contains('RDC', na=False)
    ]

    # Merge with LTL reference
    df_orders = pd.merge(
        df_order_export,
        ltl_qty_df[['SAP Code', 'LTL Qty', 'Case_Pallet', 'Orig']],
        left_on='Material',
        right_on='SAP Code',
        how='left'
    )

    df_LTL = df_orders.copy()

    # Convert weight to pounds
    df_LTL['Gross weight'] = df_LTL['Gross weight'] * 2.20462

    # Column to count lines per PO
    df_LTL['Lines_PO'] = 1

    columns = [
        'Purchase order no.',
        'Sales document',
        'Material',
        'Order Quantity',
        'Gross weight',
        'Case_Pallet',
        'LTL Qty',
        'Orig',
        'Batch',
        'Storage Location',
        'Lines_PO'
    ]

    df_LTL_clean = df_LTL[columns].copy()

    df_LTL_clean['Status'] = np.where(df_LTL_clean['Orig'].isna(), 'Not found', 'Found')
    df_LTL_clean['Orig'] = df_LTL_clean['Orig'].fillna('Not found')

    # If Material is sample (starts with 5), Orig is blank and Status is Found - Sample
    df_LTL_clean['Status'] = np.where(
        df_LTL_clean['Material'].astype(str).str.startswith('5'),
        'Found - Sample',
        df_LTL_clean['Status']
    )
    df_LTL_clean['Orig'] = np.where(
        df_LTL_clean['Status'] == 'Found - Sample',
        '',
        df_LTL_clean['Orig']
    )

    # Clean key columns before flags
    df_LTL_clean['Batch'] = df_LTL_clean['Batch'].astype(str).str.strip()

    # Normalize empties
    df_LTL_clean['Purchase order no.'] = df_LTL_clean['Purchase order no.'].replace(
        ['', ' ', 'NaN', 'nan', 'None'],
        pd.NA
    )
    df_LTL_clean['Batch'] = df_LTL_clean['Batch'].replace(
        ['', ' ', 'NaN', 'nan', 'None'],
        pd.NA
    )

    # Create flags for conditions
    df_LTL_clean['Missing_PO'] = (
        df_LTL_clean['Purchase order no.'].isna() |
        (df_LTL_clean['Purchase order no.'].astype(str).str.strip() == '')
    )

    df_LTL_clean['Missing_Batch'] = (
        df_LTL_clean['Batch'].isna() |
        (df_LTL_clean['Batch'].astype(str).str.strip() == '')
    )

    df_LTL_clean['Storage_2509'] = df_LTL_clean['Storage Location'] == 2509

    # Grouping logic
    df_LTL_grouped = (
        df_LTL_clean
        .groupby(['Purchase order no.', 'Status', 'Orig'], as_index=False)
        .agg({
            'Order Quantity': 'sum',
            'Gross weight': 'sum',
            'Case_Pallet': 'min',
            'LTL Qty': 'min',
            'Lines_PO': 'sum',
            'Missing_PO': 'max',
            'Missing_Batch': 'max',
            'Storage_2509': 'max',
            **{
                col: 'first'
                for col in df_LTL_clean.columns
                if col not in [
                    'Purchase order no.',
                    'Order Quantity',
                    'Gross weight',
                    'Case_Pallet',
                    'LTL Qty',
                    'Orig',
                    'Lines_PO',
                    'Missing_PO',
                    'Missing_Batch',
                    'Storage_2509'
                ]
            }
        })
    )

    df_LTL_grouped = df_LTL_grouped.sort_values(['Purchase order no.', 'Orig']).reset_index(drop=True)

    for col in ['Purchase order no.', 'Batch', 'LTL Qty']:
        df_LTL_grouped[col] = df_LTL_grouped[col].replace(['', ' ', 'NaN', 'nan', 'None'], pd.NA)

    # -------------------------------------------------------
    # Build initial FINAL and REVIEW tables
    # -------------------------------------------------------

    # LTL final candidates
    df_LTL_final = df_LTL_grouped[
        (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
        (
            (df_LTL_grouped['LTL Qty'].isna()) &
            (df_LTL_grouped['Status'] == 'Found')
        )
    ].copy()

    # LTL review
    df_LTL_errors = df_LTL_grouped[
        (
            (
                df_LTL_grouped['Missing_PO'] &
                (
                    (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] != 'Found - Sample'))
                )
            )
            |
            (
                df_LTL_grouped['Missing_Batch'] &
                (
                    (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] != 'Found - Sample'))
                )
            )
            |
            (
                df_LTL_grouped['Storage_2509'] &
                (
                    (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] != 'Found - Sample'))
                )
            )
            | 
            (
                (df_LTL_grouped['Purchase order no.'].astype(str).str.contains('_', na=False)) &
                (
                    (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] != 'Found - Sample'))
                )
            )
        )
    ].copy()

    # Parcel final candidates
    df_parcel_final = df_LTL_grouped[
        (
            (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) &
            (df_LTL_grouped['LTL Qty'].isna() == False)
        ) |
        (df_LTL_grouped['Material'].astype(str).str.startswith('5'))
    ].copy()

    # Parcel review
    df_parcel_errors = df_LTL_grouped[
        (
            (
                df_LTL_grouped['Missing_PO'] &
                (
                    (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] == 'Found - Sample'))
                )
            )
            |
            (
                df_LTL_grouped['Missing_Batch'] &
                (
                    (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] == 'Found - Sample'))
                )
            )
            |
            (
                (df_LTL_grouped['Storage_2509'] & df_LTL_grouped['Orig']=='NJ') &
                (
                    (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] == 'Found - Sample'))
                )
            )
            |
            (
                (df_LTL_grouped['Purchase order no.'].astype(str).str.contains('_', na=False)) &
                (
                    (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) |
                    (df_LTL_grouped['LTL Qty'].isna() & (df_LTL_grouped['Status'] == 'Found - Sample'))
                )
            )
        )
    ].copy()

    # -------------------------------------------------------
    # Remove review rows from final tables using row_key
    # -------------------------------------------------------
    df_LTL_final['row_key'] = make_row_key(df_LTL_final)
    df_LTL_errors['row_key'] = make_row_key(df_LTL_errors)
    df_LTL_final = df_LTL_final[~df_LTL_final['row_key'].isin(df_LTL_errors['row_key'])].copy()

    df_parcel_final['row_key'] = make_row_key(df_parcel_final)
    df_parcel_errors['row_key'] = make_row_key(df_parcel_errors)
    df_parcel_final = df_parcel_final[~df_parcel_final['row_key'].isin(df_parcel_errors['row_key'])].copy()

    # Final cleaning for output
    df_LTL_final = df_LTL_final.drop(columns=['LTL Qty', 'Batch', 'Missing_PO', 'Missing_Batch', 'Storage_2509', 'row_key'])
    df_parcel_final = df_parcel_final.drop(columns=['LTL Qty', 'Case_Pallet', 'Batch', 'Missing_PO', 'Missing_Batch', 'Storage_2509', 'row_key'])

    # Keep flags in review tables so CS can understand why rows need review
    df_LTL_errors = df_LTL_errors.drop(columns=['LTL Qty', 'Case_Pallet', 'Batch', 'row_key','Status'])
    df_parcel_errors = df_parcel_errors.drop(columns=['LTL Qty', 'Case_Pallet', 'Batch', 'row_key','Status'])

    df_LTL_final['Pallet_qty'] = np.ceil(
        df_LTL_final['Order Quantity'] / df_LTL_final['Case_Pallet']
    )

    return df_LTL_final, df_LTL_errors, df_parcel_final, df_parcel_errors

# -----------------------------------------------------------
# STEP 2 - BUILD FINAL PARCEL EXPORT
# -----------------------------------------------------------
def process_parcel_export(df_parcel_final, dn_file, chub_file):
    dn_df = pd.read_excel(dn_file)

    dn_cols = [
        'Delivery',
        'Material',
        'Material Description',
        'Batch',
        'Delivery quantity',
        'Sales unit',
        'Gross Weight',
        'Weight unit',
        'Sales document',
        'Delivery Date',
        'Picking Date',
        'Pland Gds Mvmnt Date',
        'Act. Gds Mvmnt Date',
        'Created by',
        'Bill-to party',
        'Receipt recipient',
        'Sold-to party',
        'Name sold-to party',
        'Ship-to party',
        'Name ship-to party',
        'Country Key',
        'Region',
        'Description'
    ]

    missing_dn_cols = [col for col in dn_cols if col not in dn_df.columns]
    if missing_dn_cols:
        raise ValueError(f"DN file is missing these columns: {missing_dn_cols}")

    dn_df = dn_df[dn_cols].copy()

    df_filtered = dn_df[
        dn_df['Receipt recipient'].astype(str).str.contains('Home Depot', case=False, na=False)
    ].reset_index(drop=True)

    df_filtered['Delivery'] = df_filtered['Delivery'].astype(str).str.strip()
    df_filtered['Sales document'] = df_filtered['Sales document'].astype(str).str.strip()

    df_parcel_final = df_parcel_final.copy()
    df_parcel_final['Sales document'] = df_parcel_final['Sales document'].astype(str).str.strip()

    merged_parcel_df = pd.merge(
        df_parcel_final,
        df_filtered[['Sales document', 'Delivery', 'Name ship-to party', 'Country Key', 'Region', 'Description']],
        on='Sales document',
        how='left'
    )

    merged_parcel_df = merged_parcel_df.drop_duplicates()
    merged_parcel_df = merged_parcel_df.fillna('')

    df_chub = pd.read_csv(
        chub_file,
        skiprows=4,
        encoding='utf-8',
        engine='python',
        dtype={'ShipToPostalCode': str}
    )
    df_chub.columns = df_chub.columns.str.strip().str.replace(r"\s+", "", regex=True)

    df_chub['ShipToPostalCode'] = (
        df_chub['ShipToPostalCode']
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
        .str.zfill(5)
    )

    required_chub_cols = [
        'PONumber', 'ShipToName', 'ShipToAddress1', 'ShipToAddress2',
        'ShipToCity', 'ShipToState', 'ShipToPostalCode',
        'ShipToDayPhone', 'Status', 'ShippingCode'
    ]
    missing_chub_cols = [col for col in required_chub_cols if col not in df_chub.columns]
    if missing_chub_cols:
        raise ValueError(f"CHUB file is missing these columns: {missing_chub_cols}")

    df_chub['HD_Store'] = df_chub['ShipToAddress1'].astype(str).str.extract(r'Store #(\d{3,})')

    df_chub['ShipToAddress'] = np.where(
        df_chub['ShipToAddress1'].astype(str).str.contains('THD', na=False),
        df_chub['ShipToAddress2'],
        df_chub['ShipToAddress1']
    )

    df_chub['Delivery Store'] = np.where(
        df_chub['HD_Store'].notna(),
        'THD Ship to Store ' + df_chub['HD_Store'].astype(str),
        'Home Depot Customer'
    )

    df_chub['HD_Store'] = df_chub['HD_Store'].fillna('')

    col_to_keep = [
        'PONumber', 'ShipToName', 'ShipToAddress', 'ShipToCity',
        'ShipToState', 'ShipToPostalCode', 'Delivery Store',
        'ShipToDayPhone', 'Status', 'ShippingCode'
    ]
    df_chub_filtered = df_chub[col_to_keep].copy()

    for col in df_chub_filtered.columns:
        df_chub_filtered[col] = df_chub_filtered[col].astype(str).str.strip()

    merged_parcel_df['Purchase order no.'] = merged_parcel_df['Purchase order no.'].astype(str).str.strip()

    parcel_df_export = pd.merge(
        merged_parcel_df,
        df_chub_filtered,
        left_on='Purchase order no.',
        right_on='PONumber',
        how='left'
    )

    parcel_df_export = parcel_df_export.drop_duplicates()

    # Add SAP Carrier Code
    parcel_df_export['SAP_Carrier_Code'] = '33120'
    # Add UPS account number
    parcel_df_export['UPS_account'] = '71WV93'

    cols_to_drop = ['PONumber', 'Region', 'Name ship-to party', 'Status_x', 'Status_y','Country Key','Description','Storage Location','Material']
    cols_existing_to_drop = [col for col in cols_to_drop if col in parcel_df_export.columns]
    parcel_df_export = parcel_df_export.drop(columns=cols_existing_to_drop)

    # Clean Column Names and orders for final Export
    # Split ShipToName safely into First Name and Last Name
    name_split = (
        parcel_df_export['ShipToName']
        .fillna('')
        .astype(str)
        .str.strip()
        .str.split(r'\s+', n=1, expand=True)
    )

    parcel_df_export['First Name'] = name_split[0].fillna('')
    parcel_df_export['Last Name'] = name_split[1].fillna('') if 1 in name_split.columns else ''

    # Rename Columns to match POM names
    parcel_df_export = parcel_df_export.rename(columns={
        'ShipToAddress':'Address',
        'ShipToCity':'City',
        'ShipToState':'State',
        'ShipToPostalCode':'Zip Code',
        'Delivery Store':'Business Name',
        'ShipToDayPhone':'Phone Number'})

    # Reorder Columns Appearance
    parcel_df_export = parcel_df_export[['Purchase order no.','Orig','Order Quantity','Gross weight','Lines_PO','Sales document','Delivery',
                                         'SAP_Carrier_Code','UPS_account','Business Name','First Name','Last Name','Phone Number','Address',
                                         'Zip Code','State','City']]

    return parcel_df_export

# -----------------------------------------------------------
# SESSION STATE
# -----------------------------------------------------------
if 'df_LTL_auto_final' not in st.session_state:
    st.session_state.df_LTL_auto_final = None

if 'df_LTL_errors' not in st.session_state:
    st.session_state.df_LTL_errors = None

if 'df_LTL_final' not in st.session_state:
    st.session_state.df_LTL_final = None

if 'df_parcel_auto_final' not in st.session_state:
    st.session_state.df_parcel_auto_final = None

if 'df_parcel_errors' not in st.session_state:
    st.session_state.df_parcel_errors = None

if 'df_parcel_final' not in st.session_state:
    st.session_state.df_parcel_final = None

if 'ltl_review_table' not in st.session_state:
    st.session_state.ltl_review_table = None

if 'parcel_review_table' not in st.session_state:
    st.session_state.parcel_review_table = None

if 'parcel_df_export' not in st.session_state:
    st.session_state.parcel_df_export = None

# -----------------------------------------------------------
# HEADER
# -----------------------------------------------------------
st.title("📦 SAP Order Cleaning Tool")
st.caption("Clean SAP order files and generate final parcel output for Customer Service.")

with st.expander("How this tool works"):
    st.markdown("""
    **Step 1**
    - Upload one or more SAP Order Export files
    - Select one workflow:
      - **LTL**
      - **Parcel**
    - Only the selected workflow is shown full page
    - Checked review rows are added back into the final table
    - Approved rows are removed from the review table

    **Step 2**
    - Uses the approved **Parcel Final** table
    - Upload:
      - **DN Excel file**
      - **search_CHUB CSV**
    - The tool creates the **final Parcel export**
    """)

st.markdown("---")

# -----------------------------------------------------------
# LOAD LTL REFERENCE
# -----------------------------------------------------------
try:
    ltl_qty_df = load_ltl_qty()
    st.success("Reference file loaded: LTL_qty_updated.xlsx")
except Exception as e:
    st.error(f"❌ Error loading LTL reference file: {e}")
    st.stop()

# -----------------------------------------------------------
# STEP 1 - FILE UPLOAD + MODE SELECTION
# -----------------------------------------------------------
st.subheader("Step 1 · Upload SAP Order Export files")

uploaded_files = st.file_uploader(
    "Upload SAP Order Export Excel file(s)",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
    key="sap_files"
)

if uploaded_files:
    process_mode = st.radio(
        "Choose order type",
        ["LTL", "Parcel"],
        horizontal=True,
        key="process_mode"
    )

    process_selected = st.button(f"Process {process_mode}", use_container_width=True)

    if process_selected:
        try:
            df_LTL_auto_final, df_LTL_errors, df_parcel_auto_final, df_parcel_errors = process_order_export(
                uploaded_files,
                ltl_qty_df
            )

            st.session_state.df_LTL_auto_final = df_LTL_auto_final
            st.session_state.df_LTL_errors = df_LTL_errors
            st.session_state.df_LTL_final = df_LTL_auto_final.copy()

            st.session_state.df_parcel_auto_final = df_parcel_auto_final
            st.session_state.df_parcel_errors = df_parcel_errors
            st.session_state.df_parcel_final = df_parcel_auto_final.copy()

            st.session_state.ltl_review_table = df_LTL_errors.copy()
            st.session_state.ltl_review_table['Approve'] = False

            st.session_state.parcel_review_table = df_parcel_errors.copy()
            st.session_state.parcel_review_table['Approve'] = False

            st.session_state.parcel_df_export = None

            st.success(f"{process_mode} data processed successfully.")
        except Exception as e:
            st.error(f"❌ Error processing SAP files: {e}")
else:
    st.info("Upload SAP Order Export Excel files to begin.")

# -----------------------------------------------------------
# STEP 1 - SHOW ONLY SELECTED WORKFLOW
# -----------------------------------------------------------
if uploaded_files and st.session_state.get("process_mode") == "LTL":
    if (
        st.session_state.ltl_review_table is not None and
        st.session_state.df_LTL_final is not None
    ):
        st.markdown("---")
        st.subheader("Step 1 · LTL Review")

        tab1, tab2 = st.tabs(["LTL Review", "LTL Final"])

        with tab1:
            edited_ltl = st.data_editor(
                st.session_state.ltl_review_table,
                use_container_width=True,
                hide_index=True,
                key="ltl_review_editor"
            )

            st.session_state.ltl_review_table = edited_ltl.copy()

            if st.button("Add approved rows to LTL Final", use_container_width=True, key="approve_ltl"):
                approved_ltl = get_approved_rows(st.session_state.ltl_review_table)

                if approved_ltl.empty:
                    st.warning("No LTL rows were approved.")
                else:
                    current_final = st.session_state.df_LTL_final.copy()

                    for col in current_final.columns:
                        if col not in approved_ltl.columns:
                            approved_ltl[col] = np.nan

                    approved_ltl = approved_ltl[current_final.columns]

                    # APPEND BACK THE APPROVED REVIEW ROWS TO LTL FINAL
                    st.session_state.df_LTL_final = (
                        pd.concat([current_final, approved_ltl], ignore_index=True)
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )

                    # Remove approved rows from LTL review table
                    st.session_state.ltl_review_table = (
                        st.session_state.ltl_review_table[
                            st.session_state.ltl_review_table['Approve'] != True
                        ]
                        .copy()
                        .reset_index(drop=True)
                    )

                    if 'Approve' in st.session_state.ltl_review_table.columns:
                        st.session_state.ltl_review_table['Approve'] = False

                    st.success(f"Added {len(approved_ltl)} row(s) to LTL Final.")

            ltl_review_download = st.session_state.ltl_review_table.copy()
            if 'Approve' in ltl_review_download.columns:
                ltl_review_download = ltl_review_download.drop(columns=['Approve'])

            st.download_button(
                "⬇️ Download LTL Review",
                data=to_excel_bytes(ltl_review_download, "LTL_Review"),
                file_name="LTL_Review.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_ltl_review"
            )

        with tab2:
            st.dataframe(st.session_state.df_LTL_final, use_container_width=True)

            st.download_button(
                "⬇️ Download LTL Final",
                data=to_excel_bytes(st.session_state.df_LTL_final, "LTL_Final"),
                file_name="LTL_Final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_ltl_final"
            )

if uploaded_files and st.session_state.get("process_mode") == "Parcel":
    if (
        st.session_state.parcel_review_table is not None and
        st.session_state.df_parcel_final is not None
    ):
        st.markdown("---")
        st.subheader("Step 1 · Parcel Review")

        tab1, tab2 = st.tabs(["Parcel Review", "Parcel Final"])

        with tab1:
            edited_parcel = st.data_editor(
                st.session_state.parcel_review_table,
                use_container_width=True,
                hide_index=True,
                key="parcel_review_editor"
            )

            st.session_state.parcel_review_table = edited_parcel.copy()

            if st.button("Add approved rows to Parcel Final", use_container_width=True, key="approve_parcel"):
                approved_parcel = get_approved_rows(st.session_state.parcel_review_table)

                if approved_parcel.empty:
                    st.warning("No Parcel rows were approved.")
                else:
                    current_final = st.session_state.df_parcel_final.copy()

                    for col in current_final.columns:
                        if col not in approved_parcel.columns:
                            approved_parcel[col] = np.nan

                    approved_parcel = approved_parcel[current_final.columns]

                    # APPEND BACK THE APPROVED REVIEW ROWS TO PARCEL FINAL
                    st.session_state.df_parcel_final = (
                        pd.concat([current_final, approved_parcel], ignore_index=True)
                        .drop_duplicates()
                        .reset_index(drop=True)
                    )

                    # Remove approved rows from Parcel review table
                    st.session_state.parcel_review_table = (
                        st.session_state.parcel_review_table[
                            st.session_state.parcel_review_table['Approve'] != True
                        ]
                        .copy()
                        .reset_index(drop=True)
                    )

                    if 'Approve' in st.session_state.parcel_review_table.columns:
                        st.session_state.parcel_review_table['Approve'] = False

                    st.success(f"Added {len(approved_parcel)} row(s) to Parcel Final.")

            parcel_review_download = st.session_state.parcel_review_table.copy()
            if 'Approve' in parcel_review_download.columns:
                parcel_review_download = parcel_review_download.drop(columns=['Approve'])

            st.download_button(
                "⬇️ Download Parcel Review",
                data=to_excel_bytes(parcel_review_download, "Parcel_Review"),
                file_name="Parcel_Review.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_parcel_review"
            )

        with tab2:
            st.dataframe(st.session_state.df_parcel_final, use_container_width=True)

            st.download_button(
                "⬇️ Download Parcel Final",
                data=to_excel_bytes(st.session_state.df_parcel_final, "Parcel_Final"),
                file_name="Parcel_Final.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_parcel_final"
            )

st.markdown("---")

# -----------------------------------------------------------
# STEP 2 UI
# -----------------------------------------------------------
st.subheader("Step 2 · Build Final Parcel Export")

if st.session_state.get("process_mode") != "Parcel" or st.session_state.df_parcel_final is None:
    st.info("Select Parcel and complete Step 1 to unlock the final parcel export.")
else:
    col_a, col_b = st.columns(2)

    with col_a:
        dn_file = st.file_uploader(
            "Upload DN Excel file",
            type=["xlsx", "xls"],
            key="dn_file"
        )

    with col_b:
        chub_file = st.file_uploader(
            "Upload search_CHUB CSV file",
            type=["csv"],
            key="chub_file"
        )

    build_final = st.button("Build Final Parcel Export", use_container_width=True)

    if dn_file is not None and chub_file is not None and build_final:
        try:
            parcel_df_export = process_parcel_export(
                st.session_state.df_parcel_final,
                dn_file,
                chub_file
            )

            st.session_state.parcel_df_export = parcel_df_export
            st.success("Final Parcel export created successfully.")

        except Exception as e:
            st.error(f"❌ Error creating final parcel export: {e}")

    elif build_final and (dn_file is None or chub_file is None):
        st.warning("Please upload both the DN file and the search_CHUB CSV file.")

# -----------------------------------------------------------
# FINAL OUTPUT
# -----------------------------------------------------------
if st.session_state.parcel_df_export is not None:
    st.markdown("---")
    st.subheader("Final Parcel Export")

    st.dataframe(st.session_state.parcel_df_export, use_container_width=True)

    st.download_button(
        "⬇️ Download Final Parcel Export",
        data=to_excel_bytes(st.session_state.parcel_df_export, "Parcel_Final_Output"),
        file_name="Parcel_Final_Export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )
