import streamlit as st
import pandas as pd
import numpy as np
import io

# -----------------------------------------------------------
# Load LTL Qty file (pre-loaded in your system)
# -----------------------------------------------------------
LTL_QTY_PATH = "LTL_qty_updated.xlsx"

@st.cache_data
def load_ltl_qty():
    return pd.read_excel(LTL_QTY_PATH)

# -----------------------------------------------------------
# Process uploaded SAP Export files
# -----------------------------------------------------------
def process_order_export(files, ltl_qty_df):

    # Read and combine uploaded Excel files
    dfs = []
    for file in files:
        df = pd.read_excel(file)
        dfs.append(df)

    df_order_export = pd.concat(dfs, ignore_index=True)

    # Filter out RDC orders
    df_order_export = df_order_export[~df_order_export['Name 1'].str.contains('RDC')]

    # Merge with LTL Qty file
    df_orders = pd.merge(
        df_order_export,
        ltl_qty_df[['SAP Code', 'LTL Qty', 'Case_Pallet','Orig']],
        left_on='Material',
        right_on='SAP Code',
        how='left'
    )

    df_LTL = df_orders.copy()

    # Filter LTL orders -- needs to be done after grouping by PO
    #df_LTL = df_orders[
        #(df_orders['Order Quantity'] >= df_orders['LTL Qty']) |
        #(df_orders['LTL Qty'].isna() == True)
    #]

    # Convert weight to Pounds
    df_LTL['Gross weight'] = df_LTL['Gross weight'] * 2.20462

    # Base columns for cleanup
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
    df_LTL_clean['DN'] = ""

    # if orig null new columns status with found yes or no
    df_LTL_clean['Status'] = np.where(df_LTL_clean['Orig'].isna(), 'Not found', 'Found')
    # if orig is na change it with Not found - so don't disappear from the pivot table
    df_LTL_clean['Orig'] = df_LTL_clean['Orig'].fillna('Not found')

    # Grouping logic - for TN group by unique POs (group lines)
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

    # Filter LTL orders
    df_LTL_final = df_LTL_grouped[
        (df_LTL_grouped['Order Quantity'] >= df_LTL_grouped['LTL Qty']) |
        ((df_LTL_grouped['LTL Qty'].isna() == True) & (df_LTL_grouped['Status'] == 'Found')) # these are the 24x48s that always ship LTL
    ]
    df_parcel_final = df_LTL_grouped[
        (df_LTL_grouped['Order Quantity'] < df_LTL_grouped['LTL Qty']) &
        (df_LTL_grouped['LTL Qty'].isna() == False) | (df_LTL_grouped['Material'].astype(str).str.startswith('5')) # samples always go in parcel
    ]

    # Drop LTL Qty columns
    df_LTL_final = df_LTL_final.drop(columns=['LTL Qty'])
    df_parcel_final = df_parcel_final.drop(columns=['LTL Qty'])
    df_parcel_final = df_parcel_final.drop(columns=['Case_Pallet'])

    # Pallet quantity for LTL
    df_LTL_final['Pallet_qty'] = np.ceil(
        df_LTL_final['Order Quantity'] / df_LTL_final['Case_Pallet']
    )

    return df_LTL_final, df_parcel_final


# -----------------------------------------------------------
# UI – Streamlit App
# -----------------------------------------------------------
st.set_page_config(page_title="SAP Order Cleaner", layout="centered")

st.title("📦 SAP Order Cleaning Tool")
st.write("Upload SAP Order Export file(s) and automatically generate the cleaned SAP grouping sheet.")

st.markdown("---")

# Load LTL Qty file
try:
    ltl_qty_df = load_ltl_qty()
    st.success("LTL Qty file loaded successfully.")
except Exception as e:
    st.error(f"❌ Error loading LTL_qty.xlsx: {e}")
    st.stop()

# Upload area
uploaded_files = st.file_uploader(
    "📤 Upload SAP Order Export Excel file(s)",
    type=["xlsx", "xls"],
    accept_multiple_files=True
)

if uploaded_files:

    if st.button("▶️ Process Files"):
        try:
            df_LTL_final, df_parcel_final = process_order_export(uploaded_files, ltl_qty_df)

            st.success("Processing completed!")

            st.subheader("LTL output preview")
            st.dataframe(df_LTL_final.head(50))

            st.subheader("Parcel output preview")
            st.dataframe(df_parcel_final.head(50))

            # Prepare LTL file for download
            ltl_buffer = io.BytesIO()
            with pd.ExcelWriter(ltl_buffer, engine="xlsxwriter") as writer:
                df_LTL_final.to_excel(writer, index=False, sheet_name="LTL_Output")
            ltl_data = ltl_buffer.getvalue()

            st.download_button(
                "⬇️ Download Cleaned LTL File",
                data=ltl_data,
                file_name="LTL_Cleaned.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Prepare Parcel file for download
            parcel_buffer = io.BytesIO()
            with pd.ExcelWriter(parcel_buffer, engine="xlsxwriter") as writer:
                df_parcel_final.to_excel(writer, index=False, sheet_name="Parcel_Output")
            parcel_data = parcel_buffer.getvalue()

            st.download_button(
                "⬇️ Download Parcel File",
                data=parcel_data,
                file_name="Parcel_Cleaned.xlsx",
                mime="application/vnd.openxmlformats-officedocument-spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"❌ Error processing files: {e}")

else:
    st.info("Upload SAP Order Export Excel files to begin.")

