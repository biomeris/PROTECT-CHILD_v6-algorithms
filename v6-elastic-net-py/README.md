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
datos. Las muestras con algún valor ausente en las features/outcome se excluyen.

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
| `features` | column_list | — | columnas CpG a usar como predictores |
| `outcome_column` | column | — | columna outcome binaria |
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

## Output

```json
{
  "coefficients": [...],
  "features": [...],
  "selected_cpgs": ["cg...", ...],
  "n_total": 200,
  "n_nodes": 4,
  "n_groups": 4,
  "nodes_skipped": 0,
  "center_by": "hospital",
  "cohorts": null,
  "hyperparams": {"alpha": 0.01, "l1_ratio": 0.5}
}
```

## Privacidad

Solo salen agregados (sumas, matriz Gram), nunca filas de pacientes. Guard K
(`min_samples`, default 5), aplicado también por grupo (hospital, cohorte). Si un
nodo tiene más features que pacientes (p >= n), la matriz Gram contiene mucha
información de cada paciente: el nodo avisa; usar menos features que pacientes
por nodo. Ver `docs/privacy.md`.

## Limitación de escala

La matriz Gram es `p×p`. Para `p` grande (~850K CpGs) es inviable (~TB) → usar
pre-screening federado para reducir a ~miles de features antes de ejecutar. El
algoritmo avisa del coste en memoria al arrancar.

## Test local

```bash
pip install -e .
python test/test.py   # valida federado == centralizado con MockNetwork
```

## Build & deploy

```bash
docker build -t adoviguera/v6-elastic-net-py:1.0.0 .
docker push adoviguera/v6-elastic-net-py:1.0.0
```
