# v6-classification-metrics

Shared code to calculate classification statistics used by multiple classification algorithms.

This module provides a collection of utility functions for evaluating binary classification models. It computes common performance metrics derived from the confusion matrix and supports ROC-AUC calculation when probability scores are available.

## Installation

### uv
How to include it into UV of other algorithms when needed:

[Example](https://github.com/IKNL/idea4rc-vantage6-algorithms/blob/main/v6-sessions/pyproject.toml)

> [!IMPORTANT]
> Note the double reference in both `dependencies` and the `uv.tools` sections.

### Dockerfile
Also the Dockerfile should be modified accordingly to include the package. See [here](https://github.com/IKNL/idea4rc-vantage6-algorithms/blob/main/docker/sessions.Dockerfile) an example.


---

## Overview

The module includes functions for:

- Extracting confusion matrix counts
- Computing classification metrics
- Calculating ROC-AUC
- Handling common edge cases safely
- Generating a complete evaluation report in a single call

---

## Functions

### `confusion_counts(y_true, y_pred, labels=(0, 1))`

Computes the confusion matrix and returns the individual counts.

**Parameters**

- `y_true`: Ground-truth binary labels.
- `y_pred`: Predicted binary labels.
- `labels`: Tuple containing `(negative_class, positive_class)`.

Default:

```python
(0, 1)
```

**Returns**

```python
(tp, tn, fp, fn)
```

Where:

- `tp`: True Positives
- `tn`: True Negatives
- `fp`: False Positives
- `fn`: False Negatives

**Example**

```python
tp, tn, fp, fn = confusion_counts(
    y_true,
    y_pred,
    labels=(0, 1)
)
```

Using custom labels:

```python
tp, tn, fp, fn = confusion_counts(
    y_true,
    y_pred,
    labels=("No", "Yes")
)
```

> **Important:** The labels must be provided in the order `(negative_class, positive_class)`.

---

### `sensitivity(tp, fn)`

Computes sensitivity, also known as **recall** or **true positive rate (TPR)**.

**Formula**

```text
TP / (TP + FN)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `specificity(tn, fp)`

Computes specificity, also known as **true negative rate (TNR)**.

**Formula**

```text
TN / (TN + FP)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `precision(tp, fp)`

Computes precision.

**Formula**

```text
TP / (TP + FP)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `positive_predictive_value(tp, fp)`

Computes the Positive Predictive Value (PPV).

**Formula**

```text
TP / (TP + FP)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `negative_predictive_value(tn, fn)`

Computes the Negative Predictive Value (NPV).

**Formula**

```text
TN / (TN + FN)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `accuracy(tp, tn, fp, fn)`

Computes overall classification accuracy.

**Formula**

```text
(TP + TN) / (TP + TN + FP + FN)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `f1_score(tp, fp, fn)`

Computes the F1-score, which is the harmonic mean of precision and recall.

**Formula**

```text
2 × (Precision × Recall) / (Precision + Recall)
```

Returns:

```python
0.0
```

when the denominator is zero.

---

### `auc_roc(y_true, y_score)`

Computes the Area Under the Receiver Operating Characteristic Curve (ROC-AUC).

**Parameters**

- `y_true`: Ground-truth binary labels.
- `y_score`: Predicted probabilities or confidence scores for the positive class.

**Returns**

- `float`: ROC-AUC score.

Returns:

```python
0.0
```

if ROC-AUC cannot be computed (for example, if only one class is present in `y_true`).

**Example**

```python
auc = auc_roc(y_true, y_probabilities)
```

---

### `binary_classification_metrics(y_true, y_pred, y_score=None, labels=(0, 1))`

Computes a complete set of evaluation metrics for a binary classification model.

**Parameters**

- `y_true`: Ground-truth binary labels.
- `y_pred`: Predicted binary labels.
- `y_score` (optional): Predicted probabilities or decision scores for the positive class.
- `labels`: Tuple containing `(negative_class, positive_class)`.

**Returns**

A dictionary containing:

```python
{
    "accuracy": ...,
    "sensitivity": ...,
    "specificity": ...,
    "precision": ...,
    "positive_predictive_value": ...,
    "negative_predictive_value": ...,
    "f1_score": ...,
    "roc_auc": ...  # only if y_score is provided
}
```

---

## Example Usage

### Standard binary labels

```python
metrics = binary_classification_metrics(
    y_true=[0, 1, 1, 0, 1],
    y_pred=[0, 1, 0, 0, 1],
    y_score=[0.10, 0.95, 0.40, 0.20, 0.87]
)
```

### Custom binary labels

```python
metrics = binary_classification_metrics(
    y_true=["No", "Yes", "Yes", "No"],
    y_pred=["No", "Yes", "No", "No"],
    labels=("No", "Yes")
)
```

---

## Edge Case Handling

The module safely handles several common edge cases.

### Division by zero

Metrics return `0.0` when their denominator is zero.

Examples:

- Precision when `TP + FP = 0`
- Sensitivity when `TP + FN = 0`
- Specificity when `TN + FP = 0`
- NPV when `TN + FN = 0`
- Accuracy when no samples are available

### Single-class datasets

The confusion matrix is constructed using the provided `labels`.

Example:

```python
y_true = [0, 0, 0, 0]
y_pred = [0, 0, 0, 0]

metrics = binary_classification_metrics(
    y_true,
    y_pred,
    labels=(0, 1)
)
```

In this situation:

```text
TP = 0
TN = 4
FP = 0
FN = 0
```

and all metrics are computed without errors.

### ROC-AUC limitations

ROC-AUC is undefined when only one class is present in the ground-truth labels.

In these cases, the function returns:

```python
0.0
```

instead of raising an exception.

---
