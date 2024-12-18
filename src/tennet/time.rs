use chrono::{TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;
use regex::Regex;

pub fn create_tennet_time_stamp (utc: DateTime<Utc>) -> String {
    Amsterdam
        .from_utc_datetime(&utc.naive_utc())
        .format("%d-%m-%Y %H:%M:%S")
        .to_string()
}

pub fn parse_tennet_time_stamp (time_string: &str) -> DateTime<Utc> {

    let re = Regex::new(r"([0-9]+)-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2})").unwrap();

    let caps = re.captures(time_string).unwrap();

    let year = caps.get(1).unwrap().as_str().parse::<i32>().unwrap();
    let month = caps.get(2).unwrap().as_str().parse::<u32>().unwrap();
    let day = caps.get(3).unwrap().as_str().parse::<u32>().unwrap();
    let hour = caps.get(4).unwrap().as_str().parse::<u32>().unwrap();
    let min = caps.get(5).unwrap().as_str().parse::<u32>().unwrap();

    // println!("{year}-{month}-{day}T{hour}:{min}");

    // handle Ambiguous local time
    let amsterdam_time = Amsterdam.with_ymd_and_hms(year, month, day, hour, min, 0).unwrap();
    let utc = amsterdam_time.to_utc();

    return utc;
}