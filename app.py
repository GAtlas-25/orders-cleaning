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

    columns = [
        'Purchase order no.',
        'Sales document',
        'Material',
        'Order Quantity',
        'Gross weight',
        'Case_Pallet',
        'LTL Qty',
        'Orig'
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

    df_LTL_grouped = (
        df_LTL_clean
        .groupby(['Purchase order no.', 'Status', 'Orig'], as_index=False)
        .agg({
            'Order Quantity': 'sum',
            'Gross weight': 'sum',
            'Case_Pallet': 'min',
            'LTL Qty': 'min',
            **{
                col: 'first'
                for col in df_LTL_clean.columns
                if col not in [
                    'Purchase order no.',
                    'Order Quantity',
                    'Gross weight',
                    'Case_Pallet',
                    'LTL Qty',
                    'Orig'
                ]
            }
        })
    )

    df_LTL_grouped = df_LTL_grouped.sort_values(['Purchase order no.', 'Orig']).reset_index(drop=True)

    df_LTL_final = df_LTL_grouped[
        (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
        ((df_LTL_grouped['LTL Qty'].isna()) & (df_LTL_grouped['Status'] == 'Found'))
    ].copy()

    df_parcel_final = df_LTL_grouped[
        (
            (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) &
            (df_LTL_grouped['LTL Qty'].notna())
        ) |
        (df_LTL_grouped['Material'].astype(str).str.startswith('5'))
    ].copy()

    df_LTL_final = df_LTL_final.drop(columns=['LTL Qty'])
    df_parcel_final = df_parcel_final.drop(columns=['LTL Qty', 'Case_Pallet'])

    df_LTL_final['Pallet_qty'] = np.ceil(
        df_LTL_final['Order Quantity'] / df_LTL_final['Case_Pallet']
    )

    return df_LTL_final, df_parcel_final

# -----------------------------------------------------------
# STEP 2 - BUILD FINAL PARCEL EXPORT
# -----------------------------------------------------------
def process_parcel_export(df_parcel_final, dn_file, chub_file):
    # -------------------------------
    # DN FILE
    # -------------------------------
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

    # -------------------------------
    # MERGE WITH PARCEL DATA
    # -------------------------------
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

    # -------------------------------
    # CHUB FILE
    # -------------------------------
    df_chub = pd.read_csv(
        chub_file,
        skiprows=4,
        encoding='utf-8',
        engine='python',
        dtype={'ShipToPostalCode': str}
    )
    
    # Ensure ZIP is string, strip, and pad with leading zeros
    df_chub['ShipToPostalCode'] = (
        df_chub['ShipToPostalCode']
        .astype(str)
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)  # removes Excel float artifact like 12345.0
        .str.zfill(5)
    )

    df_chub.columns = df_chub.columns.str.strip().str.replace(r"\s+", "", regex=True)

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
        ''
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

    cols_to_drop = ['PONumber', 'Region', 'Name ship-to party', 'Status_x', 'Status_y']
    cols_existing_to_drop = [col for col in cols_to_drop if col in parcel_df_export.columns]
    parcel_df_export = parcel_df_export.drop(columns=cols_existing_to_drop)

    parcel_df_export['Delivery Store'] = parcel_df_export['Delivery Store'].fillna('')

    return parcel_df_export

# -----------------------------------------------------------
# DOWNLOAD HELPER
# -----------------------------------------------------------
def to_excel_bytes(df, sheet_name="Sheet1"):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()

# -----------------------------------------------------------
# SESSION STATE
# -----------------------------------------------------------
if 'df_LTL_final' not in st.session_state:
    st.session_state.df_LTL_final = None

if 'df_parcel_final' not in st.session_state:
    st.session_state.df_parcel_final = None

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
    - The tool creates:
      - **LTL output**
      - **Parcel intermediate output**

    **Step 2**
    - Upload:
      - **DN Excel file**
      - **search_CHUB CSV**
    - The tool creates the **final Parcel export**

    **Recommended flow**
    1. Process SAP files
    2. Review previews
    3. Upload DN + CHUB files
    4. Download final parcel file
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
# STEP 1 UI
# -----------------------------------------------------------
st.subheader("Step 1 · Process SAP Order Export files")

uploaded_files = st.file_uploader(
    "Upload SAP Order Export Excel file(s)",
    type=["xlsx", "xls"],
    accept_multiple_files=True,
    key="sap_files"
)

col1, col2 = st.columns([1, 3])

with col1:
    process_step1 = st.button("Process SAP Files", use_container_width=True)

if uploaded_files and process_step1:
    try:
        df_LTL_final, df_parcel_final = process_order_export(uploaded_files, ltl_qty_df)

        st.session_state.df_LTL_final = df_LTL_final
        st.session_state.df_parcel_final = df_parcel_final
        st.session_state.parcel_df_export = None

        st.success("Step 1 completed successfully.")
    except Exception as e:
        st.error(f"❌ Error processing SAP files: {e}")

if st.session_state.df_LTL_final is not None and st.session_state.df_parcel_final is not None:
    tab1, tab2 = st.tabs(["LTL Preview", "Parcel Preview"])

    with tab1:
        st.dataframe(st.session_state.df_LTL_final, use_container_width=True)
        st.download_button(
            "⬇️ Download Cleaned LTL File",
            data=to_excel_bytes(st.session_state.df_LTL_final, "LTL_Output"),
            file_name="LTL_Cleaned.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with tab2:
        st.dataframe(st.session_state.df_parcel_final, use_container_width=True)
        st.download_button(
            "⬇️ Download Intermediate Parcel File",
            data=to_excel_bytes(st.session_state.df_parcel_final, "Parcel_Output"),
            file_name="Parcel_Cleaned.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

st.markdown("---")

# -----------------------------------------------------------
# STEP 2 UI
# -----------------------------------------------------------
st.subheader("Step 2 · Build Final Parcel Export")

if st.session_state.df_parcel_final is None:
    st.info("Complete Step 1 first to unlock the final parcel export.")
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
