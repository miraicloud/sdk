//! UTC times as unix ms, written like JavaScript's `toISOString` without
//! milliseconds (`2030-01-01T00:00:00Z`).

/// The current time, unix ms.
pub(crate) fn now_ms() -> i64 {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's
/// `days_from_civil`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// `YYYY-MM-DDTHH:MM:SSZ` (milliseconds dropped); years outside 0–9999 as
/// `±YYYYYY`, like JavaScript.
pub(crate) fn iso(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let year = if (0..=9999).contains(&y) {
        format!("{y:04}")
    } else if y < 0 {
        format!("-{:06}", -y)
    } else {
        format!("+{y:06}")
    };
    format!(
        "{year}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

pub(crate) enum ParseError {
    /// Not `YYYY-MM-DDTHH:MM:SSZ`.
    Format,
    /// The right shape, but no such time.
    Date,
}

/// Unix ms of `YYYY-MM-DDTHH:MM:SSZ`. Out-of-range days and `24:00:00` are
/// carried over (the caller re-renders to check canonical form).
pub(crate) fn parse_iso(s: &str) -> Result<i64, ParseError> {
    let b = s.as_bytes();
    let shape = b.len() == 20
        && b.iter().enumerate().all(|(i, &c)| match i {
            4 | 7 => c == b'-',
            10 => c == b'T',
            13 | 16 => c == b':',
            19 => c == b'Z',
            _ => c.is_ascii_digit(),
        });
    if !shape {
        return Err(ParseError::Format);
    }
    let n = |r: std::ops::Range<usize>| s[r].parse::<i64>().expect("digits");
    let (y, mo, d, h, mi, se) = (n(0..4), n(5..7), n(8..10), n(11..13), n(14..16), n(17..19));
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 24 || mi > 59 || se > 59 {
        return Err(ParseError::Date);
    }
    if h == 24 && (mi != 0 || se != 0) {
        return Err(ParseError::Date);
    }
    Ok((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se) * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for (text, ms) in [
            ("1970-01-01T00:00:00Z", 0),
            ("2030-01-01T00:00:00Z", 1_893_456_000_000),
            ("2029-06-01T12:30:15Z", 1_875_011_415_000),
            ("2000-02-29T23:59:59Z", 951_868_799_000),
            ("0000-01-01T00:00:00Z", -62_167_219_200_000),
        ] {
            assert_eq!(parse_iso(text).ok(), Some(ms), "{text}");
            assert_eq!(iso(ms), text);
        }
        assert_eq!(iso(1_893_456_000_999), "2030-01-01T00:00:00Z");
        assert_eq!(iso(-1), "1969-12-31T23:59:59Z");
        assert!(parse_iso("2030-13-01T00:00:00Z").is_err());
        assert!(parse_iso("2030-01-01T00:00:00.000Z").is_err());
        // Carried over, then refused by the canonical check.
        assert_eq!(
            iso(parse_iso("2030-02-30T00:00:00Z").ok().unwrap()),
            "2030-03-02T00:00:00Z"
        );
    }
}
