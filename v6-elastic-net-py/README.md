# v6-elastic-net-py

Federated **Elastic Net** for feature selection on high-dimensional DNA-methylation
data (`p >> n`), for the Horizon Europe **PROTECT-CHILD** project (vantage6 v5).

## Propósito clínico

Seleccionar, sin mover datos entre hospitales, qué sitios CpG de metilación
predicen el outcome (p.ej. rechazo vs tolerancia de trasplante). Reduce miles de
marcadores a una lista corta interpretable.

## Cómo funciona (federación exacta)

Elastic Net se resuelve por coordinate descent, que solo necesita los
estadísticos suficientes `G = XᵀX` (Gram) y `b = Xᵀy`. Ambos se **suman** entre
nodos (`G = ΣG_k`, `b = Σb_k`), así que el resultado federado es **idéntico** al
centralizado. Dos rondas:

1. `partial_moments` → media/std/ymean globales (para estandarizar igual en todos).
2. `partial_gram` → `G_k`, `b_k` por nodo → el `central` agrega y resuelve.

> Validado: federado == centralizado (sklearn), diferencia ~1e-8.

## Funciones

| Función | Tipo | Rol |
|---|---|---|
| `central` | central | orquesta 2 rondas, agrega, resuelve, devuelve coeficientes + CpGs |
| `partial_moments` | federated | momentos locales (ronda 1) |
| `partial_gram` | federated | estadísticos suficientes locales (ronda 2) |

## Input

Una tabla por nodo, en cualquiera de estos formatos (se detecta solo):

- **Ancho**: una fila por paciente, columnas de CpGs (`cg...`) con beta-values
  + la columna outcome (+ la columna de cohorte si se usa). Otras columnas se
  ignoran.
- **Largo** (salida de `v6-preprocessIDAT-py`): una fila por (muestra, sonda) con
  `probe_id`, `sample_column` (default `sample_label`), `value_column` (default
  `beta`) y las columnas por muestra outcome (+ cohorte). Se pivota en el nodo
  solo para las `features` pedidas.

Los nombres de columna (outcome, cohorte, muestra, valor) se pasan como
argumentos, así que el algoritmo no depende de cómo se integre el modelo de
datos. Las muestras sin outcome (o sin cohorte, si se usa) se excluyen.

**Outcome**: numérico (p.ej. `age`, regresión), o de texto con `positive_value`:
y = 1 si el valor es `positive_value`, 0 si no. Para comparar dos cohortes de la
salida de `v6-extractionIDAT-py`: `outcome_column="cohort"`,
`cohort_column="cohort"`, `cohorts=["lipid", "highcardio"]`,
`positive_value="highcardio"`.

**CpGs disponibles en todos los hospitales**: tras el control de calidad cada
hospital conserva un conjunto de sondas algo distinto (una sonda se elimina si
falla en alguna muestra del hospital). En la ronda 1 cada nodo indica qué
features tiene sin valores ausentes; el modelo usa solo las disponibles en todos
los nodos válidos y el resultado las lista en `excluded_features` (con en
cuántos nodos estaban), en vez de descartar el hospital entero.

**Selección previa**: las `features` deben venir pre-seleccionadas (como mucho
2,000), p.ej. los top DMPs de `v6-dmp-dmr-py` (presentes en todos los
hospitales: columna `n_nodes`).

## Centrado: hospitales y cohortes

| `center_by` | Qué hace | Equivale a |
|---|---|---|
| `hospital` (default) | cada nodo centra X e y con sus propias medias | un intercepto no penalizado por hospital: elimina batch entre hospitales |
| `hospital_cohort` | centra dentro de cada (hospital, cohorte) | un intercepto por hospital × cohorte |
| `global` | media/std global (comportamiento original) | un único intercepto |

La equivalencia es exacta (Frisch-Waugh-Lovell) y el test lo comprueba contra
sklearn. La escala es la desviación típica intra-grupo agregada. **Ojo**:
`hospital_cohort` elimina también las diferencias entre cohortes; no usarlo si el
outcome depende de la cohorte que se quiere estudiar. Con `hospital_cohort`, los
grupos (hospital, cohorte) con menos de `min_samples` muestras se descartan.

`cohorts` permite ajustar el modelo solo con algunas cohortes (p.ej.
`["cohort_A"]`).

## Argumentos (función `central`)

| Argumento | Tipo | Default | Descripción |
|---|---|---|---|
| `features` | json (lista) | — | CpGs candidatas (máx. 2,000); o bien `candidates_task_id` |
| `candidates_task_id` | int | — | tarea DMP/DMR (top DMPs presentes en todos los hospitales) o Beta y M por cohorte (mayor Δβ entre las cohortes elegidas, cobertura completa) de la que se toman las candidatas |
| `n_candidates` | int | 100 | cuántas candidatas tomar de esa tarea |
| `outcome_column` | column | — | outcome numérico, o de texto con `positive_value` |
| `positive_value` | string | — | outcome = 1 si el valor es este (p.ej. una cohorte). Vacío con dos cohortes: la segunda es la positiva; vacío con tres o más: un modelo por cohorte frente a las demás (one-vs-rest) y la unión de las CpGs |
| `alpha` | float | 0.01 | fuerza de regularización |
| `l1_ratio` | float | 0.5 | balance L1/L2 (1=Lasso, 0=Ridge) |
| `max_iter` | int | 1000 | iteraciones máx de coordinate descent |
| `tol` | float | 1e-8 | umbral de convergencia |
| `min_samples` | int | 5 | privacy guard: nodos con menos muestras se saltan |
| `warn_gram_gb` | float | 1.0 | avisa si la matriz Gram p×p supera este tamaño |
| `center_by` | string | `hospital` | `hospital`, `hospital_cohort` o `global` (ver arriba) |
| `cohort_column` | column | — | columna de cohorte (para `hospital_cohort` o `cohorts`) |
| `cohorts` | string_list | — | cohortes a incluir; vacío = todas |
| `sample_column` | string | `sample_label` | columna de muestra (solo formato largo) |
| `value_column` | string | `beta` | columna de valores (solo formato largo) |
| `organizations_to_include` | lista de ids | todas | hospitales a usar; vacío = todos |

El resultado incluye `input`: de dónde vienen las candidatas (una lista dada, o la tarea, su nombre y su análisis). En la interfaz web, el formulario pregunta primero si usar el resultado de una tarea anterior o subir un fichero JSON. Si HQ rechaza crear las subtareas (p.ej. el dataframe no existe en un hospital), la tarea se detiene con el mensaje de HQ en vez de esperar.

## Output

```json
{
  "selected": [{"feature": "cg...", "coefficient": 0.12}, ...],
  "selected_cpgs": ["cg...", ...],
  "coefficients": [...],
  "features": [...],
  "n_features_requested": 1000,
  "excluded_features": [{"feature": "cg...", "n_nodes_available": 2}],
  "n_total": 28,
  "n_nodes": 3,
  "n_groups": 3,
  "nodes_skipped": 0,
  "skipped": [],
  "privacy_warnings": ["..."],
  "center_by": "hospital",
  "cohorts": ["lipid", "highcardio"],
  "outcome": {"column": "cohort", "positive_value": "highcardio"},
  "hyperparams": {"alpha": 0.01, "l1_ratio": 0.5}
}
```

`selected` ordena las CpGs seleccionadas de mayor a menor |coeficiente|
(coeficientes sobre X estandarizada). `features`/`coefficients` son las CpGs
usadas (disponibles en todos los nodos) y todos sus coeficientes. `skipped` da
el motivo de cada nodo que no participó.

## Privacidad

Solo salen agregados (sumas, matriz Gram), nunca filas de pacientes. Guard K
(`min_samples`, default 5), aplicado también por grupo (hospital, cohorte). Si un
nodo tiene más features que pacientes (p >= n), la matriz Gram contiene mucha
información de cada paciente: el nodo avisa; usar menos features que pacientes
por nodo. Ver `docs/privacy.md`.

## Limitación de escala

La matriz Gram es `p×p`. Para `p` grande (~850K CpGs) es inviable (~TB) → usar
pre-screening federado para reducir a ~miles de features antes de ejecutar. Cada
nodo la envía en binario (float64, base64): 1,000 features ≈ 11 MB, 2,000 ≈ 43
MB por nodo. El límite de 2,000 features mantiene el resultado lejos del límite
de 1 GB por resultado de vantage6.

## Limitaciones abiertas

- **Privacidad con p ≥ n**: con más features que pacientes por nodo la Gram
  revela casi los datos de cada paciente (salvo una rotación). El resultado lo
  avisa en `privacy_warnings`; pendiente de decisión del proyecto (ver
  `docs/privacy.md`).
- **`alpha` fijo, sin validación cruzada**: qué CpGs se seleccionan depende de
  `alpha`.
- **Selección y evaluación con los mismos datos**: si las CpGs vienen de DMP/DMR
  y luego se entrena y evalúa un clasificador (SVM) con las mismas muestras, la
  precisión se sobreestima; hace falta reservar muestras.
- Con un outcome 0/1 es un modelo lineal (mínimos cuadrados): sirve para
  seleccionar CpGs, no para predecir probabilidades.

## Test local

```bash
pip install -e .
python test/test.py   # federado == centralizado con MockNetwork, outcome de cohorte,
                      # CpGs ausentes en un hospital, límites y cliente real
```

## Build & deploy

```bash
docker build -t adoviguera/v6-elastic-net-py:1.0.0 .
docker push adoviguera/v6-elastic-net-py:1.0.0
```
