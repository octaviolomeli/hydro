use chrono::{DateTime, Timelike, Utc};
use hydro_lang::{live_collections::stream::NoOrder, prelude::*};

pub struct Queries;

#[derive(Clone)]
pub struct Auction {
    id: i64,
    item_name: String,
    description: String,
    initial_bid: i64,
    reserve: i64,
    date_time: DateTime<Utc>,
    expires: DateTime<Utc>,
    seller: i64,
    category: i64,
    extra: String
}

#[derive(Clone)]
pub struct Person {
    id: i64,
    name: String,
    email: String,
    credit_card: String,
    city: String,
    state: String,
    date_time: DateTime<Utc>,
    extra: String
}

#[derive(Clone)]
pub struct Bid {
    auction: i64,
    bidder: i64,
    price: i64,
    channel: String,
    url: String,
    date_time: DateTime<Utc>,
    extra: String
}

/*
    Queries from https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
*/

pub fn q1<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<Bid, Process<'a, Queries>, Bounded> {
    /*
        Convert each bid value from dollars to euros. Illustrates a simple transformation.
    */
    bid_stream.map(q!(|mut bid_obj| {
        bid_obj.price = (bid_obj.price as f64 * 0.908) as i64;
        bid_obj
    }))
}

pub fn q2<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64), Process<'a, Queries>, Bounded> {
    /*
        Find bids with specific auction ids and show their bid price.
    */
    bid_stream.filter_map(q!(|bid_obj| {
        if bid_obj.auction % 123 == 0 {
            Some((bid_obj.auction, bid_obj.price))
        } else {
            None
        }
    }))
}

pub fn q3<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Bounded>,
) -> Stream<(String, String, String, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Who is selling in OR, ID or CA in category 10, and for what auction ids?
        Illustrates an incremental join (using per-key state and timer) and filter.
    */

    let cat10_auctions = auction_stream.filter_map(q!(|a| {
        if a.category == 10 {
            Some((a.seller, a))
        } else {
            None
        }
    }));
    let or_id_ca_persons = person_stream.filter_map(q!(|p| {
        if p.state == "OR" || p.state == "CA" || p.state == "ID" {
            Some((p.id, p))
        } else {
            None
        }
    }));

    or_id_ca_persons.join(cat10_auctions).map(q!(|p_a| {
        let (p, a) = p_a.1;
        (p.name, p.city, p.state, a.id)
    }))

}

pub fn q4<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Select the average of the wining bid prices for all auctions in each category.
        Illustrates complex join and aggregation.
    */

    let formatted_auctions = auction_stream.map(q!(|a| (a.id, a)));
    let formatted_bids = bid_stream.map(q!(|b| (b.auction, b)));

    let joined = formatted_auctions.join(formatted_bids).filter_map(q!(|a_b| {
        let (a, b) = a_b.1;
        if a.date_time <= b.date_time && b.date_time <= a.expires {
            Some(((a.id, a.category), b.price))
        } else {
            None
        }
    }));

    let max_price_by_auction = joined.into_keyed().reduce_commutative(q!(|winning_a_bid, cur| {
        if *winning_a_bid < cur {
            *winning_a_bid = cur;
        }
    }));

    let aggregated_categories = max_price_by_auction.entries().map(q!(|elem| {
        let category = elem.0.1;
        let max_price = elem.1;
        (category, (max_price, 1))
    })).into_keyed();

    let summed_prices_by_category = aggregated_categories.reduce_commutative(q!(|acc_prices, x| {
        acc_prices.0 += x.0;
        acc_prices.1 += x.1;
    }));

    let average_winning_price_by_category = summed_prices_by_category.map(q!(|elem| {
        let summed_prices = elem.0;
        let num_prices = elem.1;
       summed_prices / num_prices
    }));

    average_winning_price_by_category.entries()

}

pub fn q10<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<Bid, Process<'a, Queries>, Bounded> {
    /*
        Convert each bid value from dollars to euros. Illustrates a simple transformation.
    */
    bid_stream.map(q!(|mut bid_obj| {
        bid_obj.price = (bid_obj.price as f64 * 0.908) as i64;
        bid_obj
    }))
}

pub fn q17<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>
) -> Stream<(i64, String, i64, i64, i64, i64, i64, i64, i64, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        How many bids on an auction made a day and what is the price?
        Illustrates an unbounded group aggregation.
    */
    let grouped_auction_day = bid_stream.map(q!(|bid| ((bid.auction, bid.date_time.format("%Y/%m/%d").to_string()), bid.price))).into_keyed();
    let aggregation = grouped_auction_day.fold_commutative(q!(|| (0, 0, 0, 0, 0, 0, 0, 0)), q!(|acc, bid_price| {
        acc.0 += 1;
        acc.1 += if bid_price < 10000 { 1 } else { 0 };
        acc.2 += if bid_price >= 10000 && bid_price < 1000000 { 1 } else { 0 };
        acc.3 += if bid_price >= 1000000 { 1 } else { 0 };
        acc.4 = if bid_price < acc.4 { bid_price } else { acc.4 };
        acc.5 = if bid_price > acc.4 { bid_price } else { acc.4 };
        acc.6 += bid_price;
        acc.7 += bid_price;
    }));
    let select = aggregation.entries().map(q!(|(auction_day, agg)| (
        auction_day.0,
        auction_day.1,
        agg.0,
        agg.1,
        agg.2,
        agg.3,
        agg.4,
        agg.5,
        agg.6 / agg.0,
        agg.7
    )));
    select
}


pub fn q14<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64, f64, String, DateTime<Utc>, String, i64), Process<'a, Queries>, Bounded> {
    /*
        Convert bid timestamp into types and find bids with specific price.
        Illustrates duplicate expressions and usage of user-defined-functions.
    */
    bid_stream.filter_map(q!(|bid| {
        if bid.price as f64 * 0.908 > 1000000.0 && bid.price as f64 * 0.908 < 50000000.0 {
            let mut bid_time_type = "otherTime";
            let bid_hour = bid.date_time.hour();
            if bid_hour >= 8 && bid_hour <= 18 {
                bid_time_type = "dayTime";
            } else if bid_hour <= 6 || bid_hour >= 20 {
                bid_time_type = "nightTime";
            }

            Some((
                bid.auction,
                bid.bidder,
                0.908 * bid.price as f64,
                bid_time_type.to_string(),
                bid.date_time,
                bid.extra.clone(),
                bid.extra.chars().filter(|c| *c == 'c').count() as i64
            ))
        } else {
            None
        }
    }))
}

pub fn q20<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64, i64, String, String, DateTime<Utc>, String, String, String, i64, i64, DateTime<Utc>, DateTime<Utc>, i64, i64, String), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Convert each bid value from dollars to euros. Illustrates a simple transformation.
    */
    let cat_10_auctions = auction_stream.filter_map(q!(|a| {
        if  a.category == 10 {
            Some((a.id, a))
        } else {
            None
        }
    }));

    let formatted_bids = bid_stream.map(q!(|b| (b.auction, b)));
    let join = cat_10_auctions.join(formatted_bids);
    let select = join.map(q!(|elem| {
        let (a, b) = elem.1;
        (
            b.auction, b.bidder, b.price, b.channel, b.url, b.date_time, b.extra,
            a.item_name, a.description, a.initial_bid, a.reserve, a.date_time, a.expires,
            a.seller, a.category, a.extra
        )
    }));

    select

}

// pub fn q21<'a>(
//     bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
// ) -> Stream<(i64, i64, i64, String, String), Process<'a, Queries>, Bounded> {
//     /*
//         Add a channel_id column to the bid table.
//         Illustrates a 'CASE WHEN' + 'REGEXP_EXTRACT' SQL.
//     */
//     bid_stream.map(q!(|bid_obj| {
//         let channel_id = match bid_obj.channel.to_lowercase().as_str() {
//             "apple" => "0",
//             "google" => "1",
//             "facebook" => "1",
//             "baidu" => "3",
//             _ => reg
//         };
//         (
//             bid_obj.auction,
//             bid_obj.bidder,
//             bid_obj.price,
//             bid_obj.channel,
//             channel_id.to_string()
//         )
//     }))
// }

pub fn q22<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64, i64, String, String, String, String), Process<'a, Queries>, Bounded> {
    /*
        What is the directory structure of the URL?
        Illustrates a SPLIT_INDEX SQL.
    */
    bid_stream.map(q!(|bid_obj| {
        let split_url = bid_obj.url.split("/").collect::<Vec<&str>>();
        (
            bid_obj.auction,
            bid_obj.bidder,
            bid_obj.price,
            bid_obj.channel,
            split_url[3].to_string(),
            split_url[4].to_string(),
            split_url[5].to_string()
        )
    }))
}