from sklearn.metrics import confusion_matrix, roc_auc_score, roc_curve


def confusion_counts(y_true, y_pred, labels=(0, 1)):
    """
    Compute the confusion matrix and return its four components.

    Returns:
        tp (int): True positives.
        tn (int): True negatives.
        fp (int): False positives.
        fn (int): False negatives.
    """
    tn, fp, fn, tp = confusion_matrix(
        y_true=y_true, y_pred=y_pred, labels=labels
    ).ravel()
    return int(tp), int(tn), int(fp), int(fn)


def sensitivity(tp, fn):
    """Compute sensitivity (recall or true positive rate)."""
    return float(tp / (tp + fn)) if (tp + fn) != 0 else 0.0


def specificity(tn, fp):
    """Compute specificity (true negative rate)."""
    return float(tn / (tn + fp)) if (tn + fp) != 0 else 0.0


def precision(tp, fp):
    """Compute precision."""
    return float(tp / (tp + fp)) if (tp + fp) != 0 else 0.0


def positive_predictive_value(tp, fp):
    """Compute the positive predictive value (PPV)."""
    return float(tp / (tp + fp)) if (tp + fp) != 0 else 0.0


def negative_predictive_value(tn, fn):
    """Compute the negative predictive value (NPV)."""
    return float(tn / (tn + fn)) if (tn + fn) != 0 else 0.0


def accuracy(tp, tn, fp, fn):
    """Compute the overall classification accuracy."""
    return float((tp + tn) / (tp + tn + fp + fn)) if (tp + tn + fp + fn) != 0 else 0.0


def f1_score(tp, fp, fn):
    """
    Compute the F1-score as the harmonic mean of
    precision and sensitivity.
    """
    p = precision(tp, fp)
    r = sensitivity(tp, fn)
    return float(2 * p * r / (p + r)) if (p + r) != 0 else 0.0


def roc_curve_data(y_true, y_score):
    """
    Compute ROC curve coordinates.

    Args:
        y_true: Ground-truth binary labels.
        y_score: Predicted probabilities or decision scores
                 for the positive class.

    Returns:
        tuple:
            fpr (array): False Positive Rates.
            tpr (array): True Positive Rates.
            thresholds (array): Decision thresholds.
    """
    try:
        fpr, tpr, thresholds = roc_curve(y_true, y_score)
        return fpr, tpr, thresholds
    except ValueError:
        return [], [], []


def auc_roc(y_true, y_score):
    """
    Compute the Area Under the Receiver Operating
    Characteristic Curve (ROC-AUC).

    Args:
        y_true: Ground-truth binary labels.
        y_score: Predicted probabilities or decision scores
                 for the positive class.
    """
    try:
        return roc_auc_score(y_true, y_score)
    except ValueError:
        return 0.0


def binary_classification_metrics(y_true, y_pred, y_score=None, labels=(0, 1)):
    """
    Compute a collection of standard evaluation metrics for
    binary classification models.

    Args:
        y_true: Ground-truth binary labels.
        y_pred: Predicted binary labels.
        y_score: Predicted probabilities or decision scores
                 for the positive class (optional).

    Returns:
        dict: Dictionary containing the computed metrics.
    """

    # Extract the confusion matrix counts
    tp, tn, fp, fn = confusion_counts(y_true, y_pred, labels)

    # Compute metrics derived from the confusion matrix
    metrics = {
        "accuracy": accuracy(tp, tn, fp, fn),
        "sensitivity": sensitivity(tp, fn),
        "specificity": specificity(tn, fp),
        "precision": precision(tp, fp),
        "positive_predictive_value": positive_predictive_value(tp, fp),
        "negative_predictive_value": negative_predictive_value(tn, fn),
        "f1_score": f1_score(tp, fp, fn),
    }

    # Compute ROC-AUC only if predicted probabilities are provided
    if y_score is not None:
        metrics["roc_auc"] = auc_roc(y_true, y_score)

        fpr, tpr, thresholds = roc_curve_data(y_true, y_score)

        metrics["roc_curve"] = {
            "fpr": fpr.tolist(),
            "tpr": tpr.tolist(),
            "thresholds": thresholds.tolist(),
        }

    return metrics
