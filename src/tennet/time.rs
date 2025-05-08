use chrono::{offset::LocalResult, Date, DateTime, TimeZone, Utc};
use chrono_tz::Europe::Amsterdam;
use chrono_tz::Tz;
use regex::Regex;

pub fn create_tennet_time_stamp (utc: DateTime<Utc>) -> String {
    Amsterdam
        .from_utc_datetime(&utc.naive_utc())
        .format("%d-%m-%Y %H:%M:%S")
        .to_string()
}

pub fn parse_tennet_time_stamp (time_string: &str) -> LocalResult<DateTime<Tz>> {

    // let re = Regex::new(r"([0-9]+)-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2})").unwrap();

    // let caps = re.captures(time_string).unwrap();

    // let year = caps.get(1).unwrap().as_str().parse::<i32>().unwrap();
    // let month = caps.get(2).unwrap().as_str().parse::<u32>().unwrap();
    // let day = caps.get(3).unwrap().as_str().parse::<u32>().unwrap();
    // let hour = caps.get(4).unwrap().as_str().parse::<u32>().unwrap();
    // let min = caps.get(5).unwrap().as_str().parse::<u32>().unwrap();

    let split = time_string.splitn(2, "-").collect::<Vec<&str>>();

    let year = split[0].parse::<i32>().unwrap();
    let month = split[1].get(0..2).unwrap().parse::<u32>().unwrap();
    let day = split[1].get(3..5).unwrap().parse::<u32>().unwrap();
    let hour = split[1].get(6..8).unwrap().parse::<u32>().unwrap();
    let min = split[1].get(9..11).unwrap().parse::<u32>().unwrap();

    // println!("{:?}", time_string);
    // println!("{year}-{month}-{day}T{hour}:{min}");

    let amsterdam_time = Amsterdam.with_ymd_and_hms(year, month, day, hour, min, 0);

    return amsterdam_time;
}