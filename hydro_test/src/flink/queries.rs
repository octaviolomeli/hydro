use chrono::{DateTime, Timelike, Utc};
use hydro_lang::{live_collections::stream::{NoOrder, TotalOrder}, prelude::*};

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

pub fn q6<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        What is the average selling price per seller for their last 10 closed auctions.
        Illustrates a specialized combiner
    */

    let prepared_auctions = auction_stream.map(q!(|a| (a.id, a)));
    let prepared_bids = bid_stream.map(q!(|b| (b.auction, b)));
    let join = prepared_auctions.join(prepared_bids);
    let subquery = join.filter_map(q!(|(_, (a, b))| {
        if a.date_time <= b.date_time && b.date_time <= a.expires {
            Some((-b.price, a.id, a.seller, b.date_time))
        } else {
            None
        }
    })).sort(); // Sort by price descending

    let groupby_idseller = subquery.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).map(q!(|(price, id, seller, date)| ((id, seller), (-price, date)))).into_keyed();
    let where_aggregation = groupby_idseller.scan(q!(|| 0), q!(|acc, data| {
            *acc += 1;
            if *acc == 1 {
                Some(data)
            } else {
                None
            }
        }));
    let from = where_aggregation.entries().map(q!(|((_, seller), (price, date))| (seller, (date, price)))).sort();
    
    // Calculating average of current row and last 10
    let groupby_seller = from.into_keyed()
        .scan(q!(|| Vec::<i64>::new()), q!(|buffer, (_, price)| {
            buffer.push(price);
            if buffer.len() > 10 {
                buffer.remove(0);
            }

            let sum: i64 = buffer.iter().sum();
            let avg = sum / buffer.len() as i64;
            Some(avg)
        })).entries();

    groupby_seller
    
}

pub fn q7<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<Bid, Process<'a, Queries>, Bounded, NoOrder> {
    /*
        What are the highest bids per period of 10 seconds?
    */
    
    let window_stream = bid_stream.clone().map(q!(|b| {
        let window_size = 10;
        let seconds = b.date_time.timestamp();
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((window_start, window_end), b.price)
    }));

    let aggregation = window_stream
        .into_keyed()
        .reduce_commutative(q!(|acc, price| {
            if *acc < price {
                *acc = price;
            }
    }));
    let prepared_join = aggregation.entries().map(q!(|(k, v)| (v, v)));
    let prepared_bids = bid_stream.map(q!(|b| (b.price, b)));
    let join = prepared_bids.join(prepared_join);
    let select = join.map(q!(|(_, (bid, _))| bid));
    select
    
}

pub fn q8<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Bounded>
) -> Stream<(i64, String, i64, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Select people who have entered the system and created auctions in the last period of 10 seconds.
    */
    
    let person_window_stream = person_stream.clone().map(q!(|p| {
        let window_size = 10;
        let seconds = p.date_time.timestamp();
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((p.id, p.name, window_start, window_end), ())
    }));

    let auction_window_stream = auction_stream.clone().map(q!(|a| {
        let window_size = 10;
        let seconds = a.date_time.timestamp();
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((a.seller, window_start, window_end), ())
    }));

    let auction_aggregation = auction_window_stream
        .into_keyed()
        .reduce_commutative(q!(|acc, _| {}));

    let person_aggregation = person_window_stream
        .into_keyed()
        .reduce_commutative(q!(|acc, _| {}));

    let prepared_a_join = auction_aggregation.entries().map(q!(|(k, _)| (k, k)));
    let prepared_p_join = person_aggregation.entries().map(q!(|(k, _)| ((k.0, k.2, k.3), k)));
    let join = prepared_a_join.join(prepared_p_join);
    let select = join.map(q!(|(k, v)| v.1));
    select
    
}

pub fn q9<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>
) -> Stream<(i64, String, i64, DateTime<Utc>), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Find the winning bid for each auction.
    */

    let prepared_auction = auction_stream.map(q!(|a| (a.id, a)));
    let prepared_bidders = bid_stream.map(q!(|b| (b.auction, b)));
    let join = prepared_auction.join(prepared_bidders);
    let datetime_filter = join.filter_map(q!(|(_, (auc, bid))| {
        if bid.date_time >= auc.date_time && bid.date_time <= auc.expires {
            Some((-bid.price, auc.date_time, auc.id, auc.item_name))
        } else {
            None
        }
    }));

    let sort_price_groupby_id = datetime_filter.sort()
        .map(q!(|(price, date, id, item)| (id, (id, item, -price, date))))
        .assume_ordering::<TotalOrder>(nondet!(/** Sorted */))
        .into_keyed();

    // Within each group, assign a row number to the entries
    let aggregation = sort_price_groupby_id.assume_ordering::<TotalOrder>(nondet!(/** Sorted */))
        .scan(q!(|| 0), q!(|acc, data| {
            *acc += 1;
            Some((*acc, data))
        }));
    
    let from = aggregation.entries().filter_map(q!(|(_, (row_num, data))| {
        if row_num <= 1 {
            Some(data)
        } else {
            None
        }
    }));

    from
}

pub fn q11<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
) -> Stream<(i64, i32, DateTime<Utc>, DateTime<Utc>), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        How many bids did a user make in each session they were active? Illustrates session windows.
        Group bids by the same user into sessions with max session gap.
        Emit the number of bids per session.
    */
    let sorted_dates = bid_stream.map(q!(|bid| (bid.date_time, bid.bidder))).sort();
    let groupby_bid = sorted_dates.map(q!(|(date, bid)| (bid, date))).assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).into_keyed();
    let sessions = groupby_bid.scan(
        q!(|| None),
        q!(|state, current_time| {
            match state {
                None => {
                    *state = Some((current_time, current_time, 1));
                    None
                }
                Some((start, last, count)) => {
                    // Update current session or make new one
                    let elapsed = current_time - *last;

                    if elapsed.num_seconds() <= 10 {
                        *last = current_time;
                        *count += 1;
                        None
                    } else {
                        let result = Some((*start, *last, *count));
                        *state = Some((current_time, current_time, 1));
                        result
                    }
                }
            }
        }));

        let select = sessions.entries().map(q!(|(bidder, (start, end, count))| (bidder, count, start, end)));
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

pub fn q18<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>
) -> Stream<(DateTime<Utc>, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        What's a's last bid for bidder to auction?
    */
    let sorted_datetime = bid_stream.map(q!(|bid| (bid.date_time, bid.bidder, bid.auction, bid.price))).sort().map(q!(|(d, b, a, p)| ((b, a), (d, p))));
    let grouped_bidder_auction = sorted_datetime.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).into_keyed();

    // Within each group, assign a row number to the entries
    let aggregation = grouped_bidder_auction.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).scan(q!(|| 0), q!(|acc, (datetime, price)| {
        *acc += 1;
        Some((*acc, (datetime, price)))
    }));
    
    let from = aggregation.entries().filter_map(q!(|(_, (row_num, data))| {
        if row_num <= 1 {
            Some(data)
        } else {
            None
        }
    }));

    from
}

pub fn q19<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>
) -> Stream<(i64, i64, i64), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        What's the top price 10 bids of an auction?
        Illustrates a TOP-N query.
    */
    let sorted_price = bid_stream.map(q!(|bid| (-bid.price, bid.auction))).sort().map(q!(|(p, a)| (a, -p)));
    let grouped_bidder_auction = sorted_price.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).into_keyed();

    // Within each group, assign a row number to the entries
    let aggregation = grouped_bidder_auction.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).scan(q!(|| 0), q!(|acc, price| {
        *acc += 1;
        Some((*acc, price))
    }));
    
    let from = aggregation.entries().filter_map(q!(|(auction, (row_num, price))| {
        if row_num <= 10 {
            Some((row_num, auction, price))
        } else {
            None
        }
        
    }));

    from
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

pub fn q23<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Bounded>,
    bid_stream: Stream<Bid, Process<'a, Queries>, Bounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Bounded>
) -> Stream<(Auction, Bid, Person), Process<'a, Queries>, Bounded, NoOrder> {
    /*
        Find all bids made by a person who has also listed an item for auction
        Illustrates a multi-way join.
    */
    let prepared_persons = person_stream.map(q!(|p| (p.id, p)));
    let prepared_bidders = bid_stream.map(q!(|b| (b.bidder, b)));
    let prepared_auctions = auction_stream.map(q!(|a| (a.seller, a)));

    let joined_persons_bidders = prepared_persons.join(prepared_bidders).map(q!(|(_, (p, b))| (b.bidder, (p, b))));
    let join = joined_persons_bidders.join(prepared_auctions);
    let select = join.map(q!(|(_, ((p, b), a))| (a, b, p)));
    select
}