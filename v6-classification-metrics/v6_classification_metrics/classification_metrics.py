from sklearn.metrics import confusion_matrix, roc_auc_score


def confusion_counts(y_true, y_pred):
    """
    Compute the confusion matrix and return its four components.

    Returns:
        tp (int): True positives.
        tn (int): True negatives.
        fp (int): False positives.
        fn (int): False negatives.
    """
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    return tp, tn, fp, fn


def sensitivity(tp, fn):
    """Compute sensitivity (recall or true positive rate)."""
    return tp / (tp + fn)


def specificity(tn, fp):
    """Compute specificity (true negative rate)."""
    return tn / (tn + fp)


def precision(tp, fp):
    """Compute precision."""
    return tp / (tp + fp)


def positive_predictive_value(tp, fp):
    """Compute the positive predictive value (PPV)."""
    return tp / (tp + fp)


def negative_predictive_value(tn, fn):
    """Compute the negative predictive value (NPV)."""
    return tn / (tn + fn)


def accuracy(tp, tn, fp, fn):
    """Compute the overall classification accuracy."""
    return (tp + tn) / (tp + tn + fp + fn)


def f1_score(tp, fp, fn):
    """
    Compute the F1-score as the harmonic mean of
    precision and sensitivity.
    """
    p = precision(tp, fp)
    r = sensitivity(tp, fn)
    return 2 * p * r / (p + r)


def auc_roc(y_true, y_score):
    """
    Compute the Area Under the Receiver Operating
    Characteristic Curve (ROC-AUC).

    Args:
        y_true: Ground-truth binary labels.
        y_score: Predicted probabilities or decision scores
                 for the positive class.
    """
    return roc_auc_score(y_true, y_score)


def binary_classification_metrics(y_true, y_pred, y_score=None):
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
    tp, tn, fp, fn = confusion_counts(y_true, y_pred)

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

    return metrics
