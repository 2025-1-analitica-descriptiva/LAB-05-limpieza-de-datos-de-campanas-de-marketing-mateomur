"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import os
    import pandas as pd
    import zipfile
    from io import TextIOWrapper

    input_dir = "files/input"
    output_dir = "files/output"

    # Crear carpeta de salida si no existe
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Leer todos los archivos .csv.zip y combinarlos en un único DataFrame
    dataframes = []

    for archivo in sorted(os.listdir(input_dir)):
        if archivo.endswith(".csv.zip"):
            zip_path = os.path.join(input_dir, archivo)
            with zipfile.ZipFile(zip_path, "r") as z:
                for name in z.namelist():
                    with z.open(name) as f:
                        df = pd.read_csv(TextIOWrapper(f, encoding="utf-8"), sep=",", skipinitialspace=True)
                        dataframes.append(df)

    df_full = pd.concat(dataframes, ignore_index=True)

    # ---------------------- client.csv ----------------------
    df_client = df_full[[
        "client_id", "age", "job", "marital", "education", "credit_default", "mortgage"
    ]].copy()

    df_client["job"] = df_client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    df_client["education"] = df_client["education"].str.replace(".", "_", regex=False)
    df_client["education"] = df_client["education"].replace("unknown", pd.NA)
    df_client["credit_default"] = df_client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    df_client["mortgage"] = df_client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    # Guardar client.csv
    df_client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # ---------------------- campaign.csv ----------------------
    df_campaign = df_full[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    # Mapear meses a números
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }
    df_campaign["month_num"] = df_campaign["month"].map(month_map)

    # Crear columna de fecha con nombre correcto
    df_campaign["last_contact_date"] = (
        "2022-" + df_campaign["month_num"] + "-" + df_campaign["day"].astype(str).str.zfill(2)
    )

    df_campaign = df_campaign[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "last_contact_date"
    ]]

    # Guardar campaign.csv
    df_campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # ---------------------- economics.csv ----------------------
    df_econ = df_full[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Guardar economics.csv
    df_econ.to_csv(os.path.join(output_dir, "economics.csv"), index=False)



if __name__ == "__main__":
    clean_campaign_data()
