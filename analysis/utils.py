def get_date_weights(dates):
    days_diff = (dates.max() - dates.min()).days
    weights = (days_diff - (dates.max() - dates).dt.days) / days_diff

    return weights
