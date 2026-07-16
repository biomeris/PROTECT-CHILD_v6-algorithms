# Privacy — Federated Elastic Net (v6-elastic-net-py)

## Qué sale de cada nodo

El algoritmo NUNCA devuelve filas de pacientes. Solo agregados:

- **Ronda 1 (`partial_moments`)**: por nodo, `n` (nº muestras), `Σx` y `Σx²` por
  columna, y `Σy`. Son sumas sobre todos los pacientes del nodo.
- **Ronda 2 (`partial_gram`)**: por nodo, la matriz de Gram `G_k = Xsᵀ Xs`
  (p×p) y el vector `b_k = Xsᵀ yc` (p). Cada entrada es una suma de productos
  sobre los pacientes del nodo (estadísticos de segundo orden).

El coordinador (`central`) solo suma estos agregados; no recibe datos crudos.

## Guards implementados

- **K mínimo (`min_samples`, default 5)**: si un nodo tiene menos de K muestras
  válidas (tras `dropna`), devuelve `{"error": ...}` y no aporta estadísticos.
  Evita exponer agregados de grupos diminutos.
- **Solo agregados JSON-serializables**: nunca se serializan registros
  individuales.

## Riesgos residuales (a evaluar antes de producción)

- **Reconstrucción desde la Gram**: `G = XᵀX` es esencialmente la matriz de
  covarianza no normalizada. Con `n` muy pequeño respecto a `p`, un observador
  con acceso a los agregados podría, en teoría, inferir información sobre los
  datos. Mitigaciones posibles:
  - subir `min_samples` (K) por encima de 5 según el acuerdo del consorcio;
  - añadir ruido calibrado (privacidad diferencial) a `G_k`/`b_k` antes de
    enviarlos (los papers de referencia lo soportan);
  - limitar el nº de features (pre-screening) para reducir la superficie.
- **Tamaño / escala**: con p grande (~850K CpGs) la Gram es p×p (~TB) — además
  de inviable, agranda la superficie de exposición. Recomendado pre-screening
  federado para reducir a ~miles de features antes de ejecutar.

## Pendiente

- [ ] Fijar el umbral K definitivo con el consorcio PROTECT-CHILD.
- [ ] Decidir si se exige ruido DP sobre los estadísticos.
- [ ] Revisión de privacidad formal antes de publicar en el algorithm store.
