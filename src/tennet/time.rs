use chrono::{offset::LocalResult, DateTime, TimeZone, Utc};
use chrono_tz::Europe::Amsterdam;
use chrono_tz::Tz;

pub fn create_tennet_time_stamp (utc: DateTime<Utc>) -> String {
    Amsterdam
        .from_utc_datetime(&utc.naive_utc())
        .format("%d-%m-%Y %H:%M:%S")
        .to_string()
}

pub fn parse_tennet_time_stamp (time_string: &str) -> LocalResult<DateTime<Tz>> {

    let split = time_string.splitn(2, "-").collect::<Vec<&str>>();

    let year = split[0].parse::<i32>().unwrap();
    let month = split[1].get(0..2).unwrap().parse::<u32>().unwrap();
    let day = split[1].get(3..5).unwrap().parse::<u32>().unwrap();
    let hour = split[1].get(6..8).unwrap().parse::<u32>().unwrap();
    let min = split[1].get(9..11).unwrap().parse::<u32>().unwrap();
    let sec = split[1].get(12..14).unwrap().parse::<u32>().unwrap();

    Amsterdam.with_ymd_and_hms(year, month, day, hour, min, sec)
}