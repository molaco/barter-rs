use chrono::{Datelike, NaiveDate};

/// Generate an inclusive sequence of dates from `start` to `end`.
pub fn date_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = start;
    while current <= end {
        dates.push(current);
        current = current.succ_opt().unwrap_or(current);
        if current == start {
            // Overflow protection
            break;
        }
    }
    dates
}

/// Choose monthly archives for complete months, daily for partial months.
///
/// Given an inclusive date range `[start, end]`, returns:
/// - `complete_months`: `Vec<(year, month)>` for months entirely within the range.
/// - `remaining_days`: `Vec<NaiveDate>` for days in partial months at the boundaries.
///
/// A month is "complete" if both its first and last day fall within `[start, end]`.
pub fn partition_date_range(
    start: NaiveDate,
    end: NaiveDate,
) -> (Vec<(i32, u32)>, Vec<NaiveDate>) {
    if start > end {
        return (Vec::new(), Vec::new());
    }

    let mut complete_months = Vec::new();
    let mut remaining_days = Vec::new();

    let mut current = start;
    while current <= end {
        let year = current.year();
        let month = current.month();
        // month is 1..=12 from Datelike::month(), so day=1 is always valid
        let first_of_month = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let last_of_month = last_day_of_month(year, month);

        if first_of_month >= start && last_of_month <= end {
            // This entire month is within range -- use monthly archive
            complete_months.push((year, month));
            // Advance past this month
            current = if month == 12 {
                match NaiveDate::from_ymd_opt(year + 1, 1, 1) {
                    Some(d) => d,
                    None => break,
                }
            } else {
                // month is 1..=11 here (12 handled above), so month+1 is valid
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
            };
        } else {
            // Partial month -- add individual days
            let month_end = last_of_month.min(end);
            while current <= month_end {
                remaining_days.push(current);
                current = match current.succ_opt() {
                    Some(d) => d,
                    None => break,
                };
            }
        }
    }

    (complete_months, remaining_days)
}

/// Return the last day of the given month.
///
/// # Panics
///
/// Panics if `month` is not 1..=12 or if `year` overflows the calendar.
pub fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    // Next month's 1st always exists for valid year/month, and pred_opt
    // of any date > Jan 1 of MIN_YEAR is always Some.
    if month == 12 {
        // Jan 1 of next year is valid for any reasonable year
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .expect("year+1 overflows NaiveDate range")
            .pred_opt()
            .expect("Jan 1 always has a predecessor")
    } else {
        // month+1 is 2..=12, always valid
        NaiveDate::from_ymd_opt(year, month + 1, 1)
            .expect("month+1 is 2..=12, always valid")
            .pred_opt()
            .expect("any date after Jan 1 has a predecessor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // date_range tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_date_range_single_day() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let dates = date_range(d, d);
        assert_eq!(dates, vec![d]);
    }

    #[test]
    fn test_date_range_multiple_days() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let dates = date_range(start, end);
        assert_eq!(
            dates,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            ]
        );
    }

    #[test]
    fn test_date_range_empty_when_start_after_end() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let dates = date_range(start, end);
        assert!(dates.is_empty());
    }

    // -----------------------------------------------------------------------
    // partition_date_range tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_single_complete_month() {
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 3)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_partial_month_start() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 2)]);
        assert_eq!(days.len(), 17); // Jan 15..=31 = 17 days
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(days[16], NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
    }

    #[test]
    fn test_partition_partial_month_end() {
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 3)]);
        assert_eq!(days.len(), 15); // Apr 1..=15 = 15 days
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 4, 1).unwrap());
        assert_eq!(days[14], NaiveDate::from_ymd_opt(2024, 4, 15).unwrap());
    }

    #[test]
    fn test_partition_multiple_complete_months() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 1), (2024, 2), (2024, 3)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_no_complete_months() {
        let start = NaiveDate::from_ymd_opt(2024, 5, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 5, 20).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert!(months.is_empty());
        assert_eq!(days.len(), 11); // May 10..=20
    }

    #[test]
    fn test_partition_cross_year_boundary() {
        let start = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 12), (2025, 1)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_single_day() {
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let (months, days) = partition_date_range(d, d);
        assert!(months.is_empty());
        assert_eq!(days, vec![d]);
    }

    #[test]
    fn test_partition_empty_when_start_after_end() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert!(months.is_empty());
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_both_partial_months() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 20).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 2)]);
        assert_eq!(days.len(), 22);
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 1, 20).unwrap());
        assert_eq!(days[11], NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
        assert_eq!(days[12], NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(days[21], NaiveDate::from_ymd_opt(2024, 3, 10).unwrap());
    }

    // -----------------------------------------------------------------------
    // last_day_of_month tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_last_day_of_month_regular() {
        assert_eq!(
            last_day_of_month(2024, 1),
            NaiveDate::from_ymd_opt(2024, 1, 31).unwrap()
        );
        assert_eq!(
            last_day_of_month(2024, 4),
            NaiveDate::from_ymd_opt(2024, 4, 30).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_february_leap() {
        assert_eq!(
            last_day_of_month(2024, 2),
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_february_non_leap() {
        assert_eq!(
            last_day_of_month(2025, 2),
            NaiveDate::from_ymd_opt(2025, 2, 28).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_december() {
        assert_eq!(
            last_day_of_month(2024, 12),
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }
}
