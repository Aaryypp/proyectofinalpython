import os
from datetime import datetime
import pandas as pd
import dagster as dg
from dagster import asset, job, op, asset_check, AssetCheckResult


@dg.asset
def assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...

# -----------------------------
# OPERACIONES
# -----------------------------

@op
def cargar_datos():
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    dfw = pd.read_csv(url)
    # Filtrar solo Spain y Ecuador
    df = dfw[dfw["country"].isin(["Spain", "Ecuador"])].copy()
    df["date"] = pd.to_datetime(df["date"])
    return df

@op
def leer_datos():
    ##leer datos y mostrar estos tal como son 
    url = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"
    dfw = pd.read_csv(url)
    print(dfw)
   

@op
def realizar_perfilado(df: pd.DataFrame):
    # Métricas
    min_new_cases = df["new_cases"].min()
    max_new_cases = df["new_cases"].max()
    pct_missing_new_cases = round(df["new_cases"].isna().mean() * 100, 2)
    pct_missing_people_vaccinated = round(df["people_vaccinated"].isna().mean() * 100, 2) \
        if "people_vaccinated" in df.columns else None
    fecha_min = df["date"].min()
    fecha_max = df["date"].max()

    # Columnas y tipos concatenadas
    columnas_tipos = "--".join([f"{col} ({typ})" for col, typ in zip(df.columns, df.dtypes.astype(str))])
    

    # Tabla en un solo registro
    tabla_perfilado_unico = pd.DataFrame([{
        "min_new_cases": f"{min_new_cases:,.0f}",
        "max_new_cases": f"{max_new_cases:,.0f}",
        "pct_missing_new_cases": f"{pct_missing_new_cases:.2f}",
        "pct_missing_people_vaccinated": f"{pct_missing_people_vaccinated:.2f}",
        "fecha_min": fecha_min.strftime("%Y-%m-%d"),
        "fecha_max": fecha_max.strftime("%Y-%m-%d"),
        "columnas_tipos_datos": columnas_tipos
    }])

    # Guardar CSV
    tabla_perfilado_unico.to_csv("tabla_perfilado.csv", index=False)
    return "tabla_perfilado.csv guardado ✅"





@job
def perfilado_covid_job():
    df = cargar_datos()
    realizar_perfilado(df)


def leer_datos() -> pd.DataFrame:
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    return pd.read_csv(url, parse_dates=["date"])


# ─────────────────────────────
# Asset: datos crudos
# ─────────────────────────────
@dg.asset(automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"))
def datos_crudos() -> pd.DataFrame:
    return leer_datos()

# ─────────────────────────────
# Asset: datos para procesar
# ─────────────────────────────
@dg.asset(automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"))
def datos_procesar() -> pd.DataFrame:
    return leer_datos()

@dg.asset
def datos_filtrados_ecuador_spain(datos_procesar: pd.DataFrame) -> pd.DataFrame:
    return datos_procesar[datos_procesar["location"].isin(["Ecuador", "Spain"])]   


@dg.asset
def datos_limpios_ecuador_spain(datos_filtrados_ecuador_spain: pd.DataFrame) -> pd.DataFrame:
    # Eliminar nulos
    df_limpio = datos_filtrados_ecuador_spain.dropna(subset=["new_cases", "people_vaccinated"])
    
    # Guardar CSV directamente en la raíz del proyecto
    output_path = "datos_limpios_ecuador_spain.csv"
    df_limpio.to_csv(output_path, index=False)
    
    return df_limpio


@dg.asset
def datos_esenciales_ecuador_spain(datos_limpios_ecuador_spain: pd.DataFrame) -> pd.DataFrame:
    """
    Selecciona columnas esenciales y elimina duplicados si existen.

    Estrategia:
    - Se seleccionan las columnas clave para el análisis: location, date, new_cases, people_vaccinated, population.
    - Se eliminan duplicados basados en la combinación de estas columnas, asumiendo que cada registro debe ser único
      por país, fecha, métricas epidemiológicas y población.
    """
    columnas_esenciales = ["location", "date", "new_cases", "people_vaccinated", "population"]
    df_esencial = datos_limpios_ecuador_spain[columnas_esenciales].copy()

    # Eliminar duplicados basados en todas las columnas seleccionadas
    df_esencial = df_esencial.drop_duplicates(subset=columnas_esenciales)

    return df_esencial



@asset_check(asset="datos_esenciales_ecuador_spain")
def check_duplicados_datos_esenciales_ecuador_spain(datos_esenciales_ecuador_spain: pd.DataFrame) -> AssetCheckResult:
    """
    Verifica si existen duplicados en los datos esenciales.
    Siempre guarda un CSV limpio en la raíz del proyecto, con o sin duplicados.
    """
    columnas_clave = ["location", "date", "new_cases", "people_vaccinated", "population"]

    # Detectar duplicados
    duplicados = datos_esenciales_ecuador_spain.duplicated(subset=columnas_clave)
    num_duplicados = duplicados.sum()

    # Eliminar duplicados si existen
    df_sin_duplicados = datos_esenciales_ecuador_spain.drop_duplicates(subset=columnas_clave)

    # Guardar CSV limpio en la raíz
    output_path = "datos_esenciales_ecuador_spain_sin_duplicados.csv"
    df_sin_duplicados.to_csv(output_path, index=False)

    return AssetCheckResult(
        passed=bool(num_duplicados == 0),
        metadata={
            "duplicados_detectados": int(num_duplicados),
            "csv_generado": "Sí",
            "ruta_csv": output_path
        }
    )

@dg.asset
def metrica_incidencia_7d(datos_esenciales_ecuador_spain: pd.DataFrame) -> pd.DataFrame:
    df = datos_esenciales_ecuador_spain.copy()
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100_000

    # Promedio móvil de 7 días por país
    df.sort_values(["location", "date"], inplace=True)
    df["incidencia_7d"] = df.groupby("location")["incidencia_diaria"].transform(lambda x: x.rolling(7).mean())

    resultado = df[["date", "location", "incidencia_7d"]].dropna()
    resultado.rename(columns={"date": "fecha", "location": "país"}, inplace=True)

    return resultado


@dg.asset
def metrica_factor_crec_7d(datos_esenciales_ecuador_spain: pd.DataFrame) -> pd.DataFrame:
    df = datos_esenciales_ecuador_spain.copy()
    df.sort_values(["location", "date"], inplace=True)

    # Suma de casos por ventana de 7 días
    df["casos_7d"] = df.groupby("location")["new_cases"].transform(lambda x: x.rolling(7).sum())
    df["casos_7d_prev"] = df.groupby("location")["new_cases"].transform(lambda x: x.shift(7).rolling(7).sum())
    df["factor_crec_7d"] = df["casos_7d"] / df["casos_7d_prev"]

    resultado = df[["date", "location", "casos_7d", "factor_crec_7d"]].dropna()
    resultado.rename(columns={"date": "semana_fin", "location": "país", "casos_7d": "casos_semana"}, inplace=True)

    return resultado


@dg.asset_check(asset="metrica_incidencia_7d")
def check_incidencia_7d_valores_validos(metrica_incidencia_7d: pd.DataFrame) -> AssetCheckResult:
    fuera_rango = metrica_incidencia_7d[
        (metrica_incidencia_7d["incidencia_7d"] < 0) | (metrica_incidencia_7d["incidencia_7d"] > 2000)
    ]

    archivo = "incidencia_7d_fuera_rango.csv"
    if not fuera_rango.empty:
        fuera_rango.to_csv(archivo, index=False)

    return AssetCheckResult(
        passed=fuera_rango.empty,
        metadata={
            "filas_fuera_rango": len(fuera_rango),
            "archivo": archivo if not fuera_rango.empty else "N/A"
        }
    )

@dg.asset
def reporte_excel_covid(
    datos_esenciales_ecuador_spain: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> None:
    import pandas as pd

    with pd.ExcelWriter("reporte_covid_ecuador_spain.xlsx") as writer:
        datos_esenciales_ecuador_spain.to_excel(writer, sheet_name="Datos Procesados", index=False)
        metrica_incidencia_7d.to_excel(writer, sheet_name="Incidencia 7d", index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name="Factor Crecimiento 7d", index=False)

# ─────────────────────────────
# Asset Check: validar columnas clave no nulas
# ─────────────────────────────
@dg.asset_check(
    asset=datos_crudos,
    name="columnas_clave_no_nulas",   # 👈 forzamos el nombre del check
    description="Valida que las columnas clave location, date y population no tengan nulos",
    blocking=True
)
def validacion_columnas_clave(
    context: dg.AssetCheckExecutionContext,
    datos_crudos: pd.DataFrame
) -> dg.AssetCheckResult:

    columnas_clave = ["location", "date", "population"]

    # Filtrar filas con nulos en esas columnas
    nulos = datos_crudos[datos_crudos[columnas_clave].isnull().any(axis=1)]

    # Guardar CSV con los índices que tienen nulos
    if not nulos.empty:
        archivo = "columnas_clave_nulas.csv"
        nulos[["location", "date", "population"]].to_csv(archivo, index=True)
        context.log.info(f"❌ Se encontraron {len(nulos)} filas con nulos. Guardado en {archivo}.")
        estado = "ERROR"
        notas = "Se encontraron nulos en location, date o population"
    else:
        context.log.info("✅ No se encontraron nulos en location, date o population.")
        estado = "OK"
        notas = "Sin problemas detectados"

    # Crear resumen tipo DataFrame
    resumen = pd.DataFrame([{
        "nombre_regla": "columnas_clave_no_nulas",
        "estado": estado,
        "filas_afectadas": len(nulos),
        "notas": notas
    }])

    context.log.info("\n📊 Resumen de check:\n" + resumen.to_string(index=False))

    return dg.AssetCheckResult(
        passed=nulos.empty,
        check_name="columnas_clave_no_nulas",
        severity=dg.AssetCheckSeverity.ERROR if not nulos.empty else dg.AssetCheckSeverity.WARN,
        metadata={
            "filas_afectadas": len(nulos),
            "archivo": "columnas_clave_nulas.csv" if not nulos.empty else "N/A",
            "resumen": resumen.to_dict(orient="records")
        }
    )


# ─────────────────────────────
# Asset Check 2: validar total_deaths no nulo ni negativo
# ─────────────────────────────
@dg.asset_check(
    asset=datos_crudos,
    name="total_deaths_no_negativos",
    description="Valida que total_deaths no sea nulo ni negativo",
    blocking=True
)
def validacion_total_deaths(
    context: dg.AssetCheckExecutionContext,
    datos_crudos: pd.DataFrame
) -> dg.AssetCheckResult:
    # Filtrar filas con total_deaths nulo o negativo
    invalido = datos_crudos[
        (datos_crudos["total_deaths"].isnull()) | (datos_crudos["total_deaths"] < 0)
    ]

    # Guardar CSV con las filas inválidas
    if not invalido.empty:
        archivo = "total_deaths_invalidos.csv"
        invalido[["location", "date", "total_deaths"]].to_csv(archivo, index=True)
        context.log.info(f"❌ Se encontraron {len(invalido)} filas inválidas en total_deaths. Guardado en {archivo}.")
        estado = "ERROR"
        notas = "Existen nulos o valores negativos en total_deaths"
    else:
        context.log.info("✅ No se encontraron valores inválidos en total_deaths.")
        estado = "OK"
        notas = "Sin problemas detectados"

    # Crear un pequeño DataFrame de resumen
    resumen = pd.DataFrame([{
        "nombre_regla": "total_deaths_no_negativos",
        "estado": estado,
        "filas_afectadas": len(invalido),
        "notas": notas
    }])

    
    context.log.info("\n📊 Resumen de check:\n" + resumen.to_string(index=False))
    return dg.AssetCheckResult(
        passed=invalido.empty,
        check_name="total_deaths_no_negativos",
        severity=dg.AssetCheckSeverity.WARN if not invalido.empty else dg.AssetCheckSeverity.NONE,
        metadata={
            "filas_afectadas": len(invalido),
            "archivo": "total_deaths_invalidos.csv" if not invalido.empty else "N/A",
            "resumen": resumen.to_dict(orient="records")  # <-- También lo dejo en metadata
        }
    )

# ─────────────────────────────
# Job para correr asset + checks
# ─────────────────────────────
lectura_chequeos = dg.define_asset_job(
    "lectura_chequeos",
    selection=[datos_crudos],  # ejecuta el asset y sus checks asociados
)


procesar_datos = dg.define_asset_job(
    "procesar_datos",
    selection=dg.AssetSelection.keys(
        "datos_procesar",
        "datos_filtrados_ecuador_spain",
        "datos_limpios_ecuador_spain",  
        "datos_esenciales_ecuador_spain",
        "metrica_incidencia_7d",
        "metrica_factor_crec_7d",
        "reporte_excel_covid"
    )
)

from dagster import Definitions

defs = Definitions(
    assets=[
        datos_crudos,
        datos_procesar,
        datos_filtrados_ecuador_spain,
        datos_limpios_ecuador_spain,
        datos_esenciales_ecuador_spain,
        metrica_incidencia_7d,
        metrica_factor_crec_7d,
        reporte_excel_covid
    ],
    asset_checks=[
        validacion_columnas_clave,
        validacion_total_deaths,
        check_duplicados_datos_esenciales_ecuador_spain,
        check_incidencia_7d_valores_validos
    ],
    jobs=[
        lectura_chequeos,
        procesar_datos
    ]
)
