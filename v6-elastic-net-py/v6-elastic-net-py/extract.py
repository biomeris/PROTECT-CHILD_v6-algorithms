"""
Funciones de DATA EXTRACTION (vantage6 v5 "Uluru").

En v5 la extraccion de datos es un paso de sesion separado: lee de la base de
datos del nodo y crea un dataframe que se reutiliza en el compute.

Para CSV (matriz de metilacion: CpGs + outcome + patient_id) basta el built-in
`read_csv` de vantage6, que se re-exporta en __init__.py — no hace falta una
funcion custom. Las columnas no usadas (p.ej. patient_id) se ignoran en compute.

Si en produccion hace falta logica especifica (filtrar columnas, query SQL,
SPARQL...), añadir aqui una funcion propia:

    from vantage6.algorithm.decorator.action import data_extraction

    @data_extraction
    def read_methylation(connection_details: dict, drop_columns=None):
        df = pd.read_csv(connection_details["uri"])
        if drop_columns:
            df = df.drop(columns=drop_columns)
        return df
"""
# TODO Añadir funciones de data extraction custom aqui si se necesitan.
