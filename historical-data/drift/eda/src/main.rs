use helpers::twap::Twap;
use itertools::iproduct;
use plotters::prelude::*;
use polars::prelude::*;
use rayon::prelude::*;
use sp_runtime::FixedU128;

// -------------------------------------------------------------------------------------------------
//                                             Constants
// -------------------------------------------------------------------------------------------------

const SECOND: u64 = 1000; // 1 second, in millis
const MINUTE: u64 = 60 * SECOND;
const HOUR: u64 = 60 * MINUTE;
const DAY: u64 = 24 * HOUR;

const PERIOD: u64 = 1 * DAY; // Default period for twap

// -------------------------------------------------------------------------------------------------
//                                          Data Processing
// -------------------------------------------------------------------------------------------------

fn main() {
    iproduct!(1..=12_u64, [HOUR, DAY], 0..=12_u64)
        .collect::<Vec<(u64, u64, u64)>>()
        .par_iter()
        .for_each(|(interval, time, dataset)| {
        let period = interval * time;
        let period_formated = {
            let mut s = format!("{interval}");
            if *time == HOUR {
                s.push_str(" Hour")
            } else {
                s.push_str(" Day")
            };
            if *interval != 1 {
                s.push('s')
            }
            s
        };
        let mut twap: Option<Twap<FixedU128, u64>> = None;
        let df = LazyCsvReader::new("data/assembled_dataset.csv".into())
            .has_header(true)
            .finish()
            .unwrap()
            .filter(col("market_index").eq(lit(*dataset)))
            .select(&[col("ts"), col("mark_price_before")])
            .with_column(
                col("ts")
                    .str()
                    .strptime(StrpTimeOptions {
                        date_dtype: DataType::Datetime(TimeUnit::Milliseconds, None),
                        fmt: Some("%Y-%m-%d %T%z".to_string()),
                        strict: true,
                        exact: true,
                    })
                    .dt()
                    .timestamp(TimeUnit::Milliseconds)
                    .alias("timestamp"),
            )
            .sort(
                "timestamp",
                SortOptions {
                    descending: false,
                    nulls_last: true,
                },
            )
            .with_column(
                as_struct(&[cols(vec!["timestamp", "mark_price_before"])])
                    .map(
                        move |data| {
                            Ok(Series::from_vec(
                                "parsed_twap",
                                data.iter()
                                    .map(move |i| match i {
                                        AnyValue::Struct(v, _) => {
                                            let (now, price) = match v[..] {
                                                [AnyValue::Int64(now), AnyValue::Int64(price)] =>
                                                    (now as u64, {
                                                        FixedU128::from_inner(
                                                            (price as u128)
                                                                .saturating_mul(10_u128.pow(12)),
                                                        )
                                                    }),
                                                _ => panic!(
                                                    "Could not extranct `now` and `price` values"
                                                ),
                                            };
                                            match twap {
                                                Some(ref mut t) => t
                                                    .accumulate(&price, now)
                                                    .unwrap_or_else(|_| {
                                                        panic!(
                                                            "Failed to accumulate twap, {now} {price}"
                                                        )
                                                    })
                                                    .to_float(),
                                                None => {
                                                    twap = Some(Twap::new(price, now, period));
                                                    twap.unwrap().get_twap().to_float()
                                                },
                                            }
                                        },
                                        _ => panic!("Failed to parse a struct field"),
                                    })
                                    .collect::<Vec<f64>>(),
                            ))
                        },
                        GetOutput::from_type(DataType::Float64),
                    )
                    .alias("twap"),
            )
            .with_column(
                col("mark_price_before")
                    .map(
                        |p| {
                            Ok(Series::from_vec(
                                "price",
                                p.iter()
                                    .map(|x| match x {
                                        AnyValue::Int64(i) => i as f64 / 10.0_f64.powf(6.),
                                        err => panic!("Failed to parse int: {err}"),
                                    })
                                    .collect::<Vec<f64>>(),
                            ))
                        },
                        GetOutput::from_type(DataType::Int64),
                    )
                    .alias("price"),
            )
            .collect()
            .unwrap();

        let x_lim_0 = df["timestamp"].min::<i64>().unwrap();
        let x_lim_1 = df["timestamp"].max::<i64>().unwrap();
        let y_lim_0 = df["price"]
            .min::<f64>()
            .unwrap()
            .min(df["twap"].min::<f64>().unwrap());
        let y_lim_1 = df["price"]
            .max::<f64>()
            .unwrap()
            .max(df["twap"].max::<f64>().unwrap());

        let file_name = format!("imgs/test_{dataset:02}_{period:010}.png");
        dbg!(&file_name);
        let root = BitMapBackend::new(&file_name, (3840, 2160)).into_drawing_area();
        root.fill(&WHITE).unwrap();
        let mut chart = ChartBuilder::on(&root)
            .margin(10)
            .caption(
                format!("Dataset = {dataset:02}, Twap Interval = {period_formated}"),
                ("sans-serif", 40),
            )
            .set_label_area_size(LabelAreaPosition::Left, 60)
            .set_label_area_size(LabelAreaPosition::Bottom, 40)
            .build_cartesian_2d(x_lim_0..x_lim_1, y_lim_0..y_lim_1)
            .unwrap();
        chart
            .configure_mesh()
            .disable_x_mesh()
            .disable_y_mesh()
            .x_labels(30)
            .max_light_lines(4)
            .y_desc("price/twap")
            .draw()
            .unwrap();

        let price_plot = df["timestamp"]
            .iter()
            .zip(df["price"].iter())
            .map(|(ts, price)| {
                (
                    match ts {
                        AnyValue::Int64(ts) => ts,
                        _ => panic!(),
                    },
                    match price {
                        AnyValue::Float64(price) => price,
                        _ => panic!(),
                    },
                )
            });
        let twap_plot = df["timestamp"]
            .iter()
            .zip(df["twap"].iter())
            .map(|(ts, price)| {
                (
                    match ts {
                        AnyValue::Int64(ts) => ts,
                        _ => panic!(),
                    },
                    match price {
                        AnyValue::Float64(price) => price,
                        _ => panic!(),
                    },
                )
            });
        chart
            .draw_series(LineSeries::new(price_plot, &BLUE))
            .unwrap();
        chart.draw_series(LineSeries::new(twap_plot, &RED)).unwrap();
        root.present().unwrap();

        // let out_file = std::fs::File::create(std::path::Path::new(
        //     format!("src/twap/tests/eda/data/twap_{dataset:02}_{period:010}.csv").as_str(),
        // ))
        // .unwrap();
        // CsvWriter::new(out_file)
        //     .has_header(true)
        //     .finish(&mut df.clone());
        })
}
