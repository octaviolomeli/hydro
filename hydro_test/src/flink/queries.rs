use hydro_lang::{live_collections::stream::{NoOrder, TotalOrder}, prelude::*, live_collections::keyed_singleton::BoundedValue};
use serde::{Deserialize, Serialize};

pub struct Queries;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Auction {
    pub id: i64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: i64,
    pub reserve: i64,
    pub date_time: i64,
    pub expires: i64,
    pub seller: i64,
    pub category: i64,
    pub extra: String
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: i64,
    pub extra: String
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Bid {
    pub auction: i64,
    pub bidder: i64,
    pub price: i64,
    pub channel: String,
    pub url: String,
    pub date_time: i64,
    pub extra: String
}

/*
    Queries from https://github.com/nexmark/nexmark/tree/master/nexmark-flink/src/main/resources/queries
*/

pub fn q1<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
) -> Stream<Bid, Process<'a, Queries>, Unbounded> {
    /*
        Convert each bid value from dollars to euros. Illustrates a simple transformation.
    */
    bid_stream.map(q!(|mut bid_obj| {
        bid_obj.price = (bid_obj.price as f64 * 0.908) as i64;
        bid_obj
    }))
}

pub fn q2<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
) -> Stream<(i64, i64), Process<'a, Queries>, Unbounded> {
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
    auction_stream: Stream<Auction, Process<'a, Queries>, Unbounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Unbounded>,
) -> Stream<(String, String, String, i64), Process<'a, Queries>, Unbounded, NoOrder> {
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
    auction_stream: Stream<Auction, Tick<Process<'a, Queries>>, Bounded>,
    bid_stream: Stream<Bid, Tick<Process<'a, Queries>>, Bounded>,
) -> KeyedSingleton<i64, i64, Process<'a, Queries>, Unbounded> {
    /*
        Select the average of the wining bid prices for all auctions in each category.
        Illustrates join and aggregation.
    */

    let formatted_auctions = auction_stream.map(q!(|a| (a.id, a)));
    let formatted_bids = bid_stream.map(q!(|b| (b.auction, b)));

    let join = formatted_auctions.join(formatted_bids).filter_map(q!(|a_b| {
        let (a, b) = a_b.1;
        if a.date_time <= b.date_time && b.date_time <= a.expires {
            Some(((a.id, a.category), b.price))
        } else {
            None
        }
    }));

    let max_price_by_auction = join.into_keyed().assume_ordering::<TotalOrder>(nondet!(/** TODO **/)).reduce(q!(|winning_a_bid, cur| {
        if *winning_a_bid < cur {
            *winning_a_bid = cur;
        }
    }));

    let aggregated_categories = max_price_by_auction.entries().all_ticks().map(q!(|elem| {
        let category = elem.0.1;
        let max_price = elem.1;
        (category, (max_price, 1))
    })).into_keyed();

    let summed_prices_by_category = aggregated_categories.assume_ordering::<TotalOrder>(nondet!(/** TODO **/)).reduce(q!(|acc_prices, x| {
        acc_prices.0 += x.0;
        acc_prices.1 += x.1;
    }));

    let average_winning_price_by_category = summed_prices_by_category.map(q!(|elem| {
        let summed_prices = elem.0;
        let num_prices = elem.1;
       summed_prices / num_prices
    }));

    average_winning_price_by_category

}

// fix
pub fn q6<'a>(
    auction_stream: Stream<Auction, Tick<Process<'a, Queries>>, Bounded>,
    bid_stream: Stream<Bid, Tick<Process<'a, Queries>>, Bounded>
) -> Stream<(i64, i64), Tick<Process<'a, Queries>>, Bounded, NoOrder> {
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

    let groupby_idseller = subquery
        .assume_ordering::<TotalOrder>(nondet!(/** Sorted */))
        .map(q!(|(price, id, seller, date)| ((id, seller), (-price, date))))
        .into_keyed();

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
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
) -> Stream<Bid, Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        What are the highest bids per period of 10 seconds?
    */
    let tick = bid_stream.clone().location().tick();

    let window_stream = bid_stream.clone().map(q!(|b| {
        let window_size = 10;
        let seconds = b.date_time;
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((window_start, window_end), b.price)
    }));

    let aggregation = window_stream
        .into_keyed()
        .reduce(q!(|acc, price| {
            if *acc < price {
                *acc = price;
            }
    }));
    let prepared_join = aggregation
        .snapshot(&tick, nondet!(/** test **/))
        .entries()
        .all_ticks()
        .map(q!(|(_, v)| (v, v)));

    let prepared_bids = bid_stream.map(q!(|b| (b.price, b)));
    let join = prepared_bids.join(prepared_join);
    let select = join.map(q!(|(_, (bid, _))| bid));
    select
    
}

pub fn q8<'a>(
    auction_stream: Stream<Auction, Process<'a, Queries>, Unbounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Unbounded>
) -> Stream<(i64, String, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Select people who have entered the system and created auctions in the last period.
    */
    let tick = auction_stream.clone().location().tick();
    
    let person_window_stream = person_stream.clone().map(q!(|p| {
        let window_size = 10;
        let seconds = p.date_time;
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((p.id, p.name, window_start, window_end), ())
    }));

    let auction_window_stream = auction_stream.clone().map(q!(|a| {
        let window_size = 10;
        let seconds = a.date_time;
        let window_start = seconds - (seconds % window_size);
        let window_end = window_start + window_size;

        ((a.seller, window_start, window_end), ())
    }));

    let auction_aggregation = auction_window_stream
        .into_keyed()
        .reduce(q!(|_, _| {}));

    let person_aggregation = person_window_stream
        .into_keyed()
        .reduce(q!(|_, _| {}));

    let prepared_a_join = auction_aggregation
        .snapshot(&tick, nondet!(/** test **/))
        .entries()
        .all_ticks()
        .map(q!(|(k, _)| (k, k)));

    let prepared_p_join = person_aggregation
        .snapshot(&tick, nondet!(/** test **/))
        .entries()
        .all_ticks()
        .map(q!(|(k, _)| ((k.0, k.2, k.3), k)));
    let join = prepared_a_join.join(prepared_p_join);
    let select = join.map(q!(|(_, v)| v.1));
    select
    
}

// fix
pub fn q9<'a>(
    auction_stream: Stream<Auction, Tick<Process<'a, Queries>>, Bounded>,
    bid_stream: Stream<Bid, Tick<Process<'a, Queries>>, Bounded>
) -> KeyedSingleton<i64, (i64, String, i64, i64), Tick<Process<'a, Queries>>, Bounded>{
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

    let sort_price_groupby_id = datetime_filter
        .sort()
        .map(q!(|(price, date, id, item)| (id, (id, item, -price, date))))
        .assume_ordering::<TotalOrder>(nondet!(/** Sorted */))
        .into_keyed();

    let aggregation = sort_price_groupby_id.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).first();
    
    aggregation
}

// fix
pub fn q11<'a>(
    bid_stream: Stream<Bid, Tick<Process<'a, Queries>>, Bounded>,
) -> Stream<(i64, i32, i64, i64), Tick<Process<'a, Queries>>, Bounded, NoOrder> {
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

                    if elapsed <= 10 {
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
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
) -> Stream<(i64, i64, f64, String, i64, String, i64), Process<'a, Queries>, Unbounded> {
    /*
        Convert bid timestamp into types and find bids with specific price.
        Illustrates duplicate expressions and usage of user-defined-functions.
    */
    bid_stream.filter_map(q!(|bid| {
        if bid.price as f64 * 0.908 > 1000000.0 && bid.price as f64 * 0.908 < 50000000.0 {
            let mut bid_time_type = "otherTime";
            let bid_hour = bid.date_time; // Using just seconds for testing
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
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>
) -> KeyedSingleton<(i64, i64), (i64, i64, i64, i64, i64, i32, i64, i64, i64, i64), Process<'a, Queries>, Unbounded>{
    /*
        How many bids on an auction made a day and what is the price?
        Illustrates an unbounded group aggregation.
    */
    let grouped_auction_day = bid_stream.map(q!(|bid| ((bid.auction, bid.date_time), bid.price))).into_keyed();
    let aggregation = grouped_auction_day.fold(q!(|| (0, 0, 0, 0, i64::MAX, i64::MIN, 0, 0)), q!(|acc, bid_price| {
        acc.0 += 1;
        acc.1 += if bid_price < 10000 { 1 } else { 0 };
        acc.2 += if bid_price >= 10000 && bid_price < 1000000 { 1 } else { 0 };
        acc.3 += if bid_price >= 1000000 { 1 } else { 0 };
        acc.4 = if bid_price < acc.4 { bid_price } else { acc.4 };
        acc.5 = if bid_price > acc.5 { bid_price } else { acc.5 };
        acc.6 += bid_price;
        acc.7 += bid_price;
    }));
    
    let select = aggregation.map_with_key(q!(|(key, data)| {
        ( key.0, key.1, data.0, data.1, data.2, data.3, data.4, data.5, data.6 / data.0, data.7 )
    }));

    select
}

pub fn q18<'a>(
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>
) -> KeyedSingleton<(i64, i64), (i64, i64, i64, i64, i32), Process<'a, Queries>, Unbounded>{
    /*
        What's a's last bid for bidder to auction?
    */
    let tick = bid_stream.clone().location().tick();

    let sorted_datetime = bid_stream.batch(&tick, nondet!(/** test **/))
        .map(q!(|bid| (bid.date_time, bid.bidder, bid.auction, bid.price)))
        .sort()
        .all_ticks()
        .map(q!(|(d, b, a, p)| ((b, a), (d, p))));

    let grouped_bidder_auction = sorted_datetime.assume_ordering::<TotalOrder>(nondet!(/** Sorted */)).into_keyed();

    // Within each group, assign a row number to the entries
    let aggregation = grouped_bidder_auction.assume_ordering::<TotalOrder>(nondet!(/** Sorted */))
    .scan(q!(|| 0), q!(|acc, (datetime, price)| {
        *acc += 1;
        Some((*acc, (datetime, price)))
    }));
    
    let from = aggregation.reduce(q!(|acc, (row_num, data)| {
        if row_num <= 1 {
            *acc = (acc.0 + row_num, data);
        }
    }))
    .map_with_key(q!(|((bidder, auction), (n, data))| {
        (bidder, auction, data.0, data.1, n)
    }));

    from
}

// fix
pub fn q19<'a>(
    bid_stream: Stream<Bid, Tick<Process<'a, Queries>>, Bounded>
) -> Stream<(i32, i64, i64), Tick<Process<'a, Queries>>, Bounded, NoOrder> {
    /*
        What's the top price 10 bids of an auction?
        Illustrates a TOP-N query.
    */

    let sorted_price = bid_stream.map(q!(|bid| (bid.price, bid.auction)))
        .sort()
        .map(q!(|(p, a)| (a, p)));

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
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
    auction_stream: Stream<Auction, Process<'a, Queries>, Unbounded>,
) -> Stream<(i64, i64, String, i64, i64, i64), Process<'a, Queries>, Unbounded, NoOrder> {
    /*
        Get bids with the corresponding auction information where category is 10.
        Illustrates a filter join.
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
            b.auction, b.date_time,
            a.item_name, a.date_time, a.seller, a.category
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
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
) -> Stream<(i64, i64, i64, String, String, String, String), Process<'a, Queries>, Unbounded> {
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
    auction_stream: Stream<Auction, Process<'a, Queries>, Unbounded>,
    bid_stream: Stream<Bid, Process<'a, Queries>, Unbounded>,
    person_stream: Stream<Person, Process<'a, Queries>, Unbounded>
) -> Stream<(Auction, Bid, Person), Process<'a, Queries>, Unbounded, NoOrder> {
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

pub fn bid(auction: i64, bidder: i64, price: i64, ts: i64, url: &str) -> Bid {
    Bid {
        auction,
        bidder,
        price,
        channel: "test".to_string(),
        url: url.to_string(),
        date_time: ts,
        extra: "abcdefgh".to_string(),
    }
}

pub fn auction(id: i64, seller: i64, category: i64, start: i64, end: i64) -> Auction {
    Auction {
        id,
        item_name: "item".to_string(),
        description: "desc".to_string(),
        initial_bid: 0,
        reserve: 0,
        date_time: start,
        expires: end,
        seller,
        category,
        extra: "".to_string(),
    }
}

pub fn person(id: i64, state: &str, ts: i64) -> Person {
    Person {
        id,
        name: format!("person{}", id),
        email: "test@test.com".to_string(),
        credit_card: "0000".to_string(),
        city: "city".to_string(),
        state: state.to_string(),
        date_time: ts,
        extra: "".to_string(),
    }
}

// Tests
#[cfg(test)]
mod tests {
    use hydro_lang::prelude::*;

    use super::*;

    #[test]
    fn test_q1() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q1(bid_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send(bid(0, 100, 4, 0, "a"));
            result_stream_port.assert_yields([bid(0, 100, 3, 0, "a")]).await;
        });
    }

    #[test]
    fn test_q2() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q2(bid_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(100, 100, 4, 0, "a"),
                bid(123, 100, 6, 0, "a"),
                bid(246, 100, 7, 0, "a")
            ]);
            result_stream_port.assert_yields([(123, 6), (246, 7)]).await;
        });
    }

    #[test]
    fn test_q3() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (personstream_in_port, person_stream) = process.sim_input();

        let result_stream = q3(auction_stream, person_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 15, 7, 0, 1),
                auction(2, 9, 10, 0, 12),
                (auction(3, 13, 10, 0, 10)),
                auction(4, 13, 10, 0, 10)
            ]);

            personstream_in_port.send_many([
                person(4, "PA", 100),
                person(9, "OR", 2),
                person(13, "CA", 5)
            ]);

            result_stream_port.assert_yields_unordered([
                ("person9".to_string(), "city".to_string(), "OR".to_string(), 2),
                ("person13".to_string(), "city".to_string(), "CA".to_string(), 3),
                ("person13".to_string(), "city".to_string(), "CA".to_string(), 4),
            ]).await;
        });
    }

    // #[test] not yet implemented: Reduce keyed with optional intermediates is not yet supported in simulator
    fn test_q4() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (bidstream_in_port, bid_stream) = process.sim_input();
        
        let auction_batch = auction_stream.batch(&tick, nondet!(/** test **/));
        let bid_batch = bid_stream.batch(&tick, nondet!(/** test **/));

        let result_stream = q4(auction_batch, bid_batch);
        let result_stream_port = result_stream.snapshot(&tick, nondet!(/** test **/)).entries().all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 1, 10, 0, 100),
                auction(2, 1, 10, 0, 100),
                auction(3, 1, 20, 0, 100),
                auction(4, 1, 20, 50, 60)
            ]);

            bidstream_in_port.send_many([
                bid(1, 101, 30, 10, "a"),
                bid(1, 102, 50, 20, "a"),
                bid(2, 103, 100, 10, "a"),
                bid(3, 104, 5, 10, "a"),
                bid(3, 105, 15, 20, "a"),
                bid(4, 106, 999, 10, "a")
            ]);
            
            result_stream_port.assert_yields_unordered([
                (10, 75),
                (20, 15)
            ]).await;
        });
    }

    // #[test] // broken
    fn test_q6() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q6(auction_stream.batch(&tick, nondet!(/** test */)), bid_stream.batch(&tick, nondet!(/** test */)));
        let result_stream_port = result_stream.all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 100, 10, 0, 100),
                auction(2, 100, 10, 0, 100),
                auction(2, 100, 10, 0, 100),
                auction(4, 200, 20, 0, 100),
                auction(5, 200, 20, 0, 100)
            ]);
            
            bidstream_in_port.send_many([
                bid(1, 10, 30, 10, "a"),
                bid(1, 11, 50, 20, "a"),
                bid(2, 12, 100, 10, "a"),
                bid(3, 13, 60, 10, "a"),
                bid(3, 14, 80, 20, "a"),
                bid(4, 15, 40, 10, "a"),
                bid(4, 16, 70, 20, "a"),
                bid(5, 17, 20, 10, "a")
            ]);

            result_stream_port.assert_yields_unordered([
                (100, 50),
                (100, 75),
                (100, 76),
                (200, 70),
                (200, 45)
            ]).await;
        });
    }
    
    // #[test] not yet implemented: Reduce keyed with optional intermediates is not yet supported in simulator
    fn test_q7() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q7(bid_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(0, 100, 20, 0, "a"),
                bid(0, 100, 1, 0, "a"),
                bid(0, 100, 65, 20, "a"),
                bid(0, 100, 30, 20, "a"),
                bid(0, 100, 75, 50, "a"),
                bid(0, 100, 40, 50, "a"),
                bid(0, 100, 1, 71, "a"),
            ]);

            result_stream_port.assert_contains_unordered([
                bid(0, 100, 20, 0, "a"),
                bid(0, 100, 65, 20, "a"),
                bid(0, 100, 75, 50, "a"),
                bid(0, 100, 1, 71, "a"),
            ]).await;
        });
    }

    // #[test] not yet implemented: Reduce keyed with optional intermediates is not yet supported in simulator
    fn test_q8() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (auctionstream_in_port, auction_stream) = process.sim_input();

        let (personstream_in_port, person_stream) = process.sim_input();

        let result_stream = q8(auction_stream, person_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(10, 1, 1, 7, 100),
                auction(11, 2, 1, 15, 100),
                auction(12, 2, 1, 16, 100),
                auction(13, 3, 1, 40, 100),
            ]);

            personstream_in_port.send_many([
                person(1, "CA", 5),
                person(2, "OR", 12),
                person(3, "WA", 25),
            ]);

            result_stream_port.assert_yields_unordered([
                (1, "person1".to_string(), 0, 10),
                (2, "person2".to_string(), 10, 20)
            ]).await;
        });
    }

    // #[test] // broken
    fn test_q9() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (bidstream_in_port, bid_stream) = process.sim_input();
        let batched_auctions = auction_stream.batch(&tick, nondet!(/** test **/));
        let batched_bids =  bid_stream.batch(&tick, nondet!(/** test **/));

        let result_stream = q9(batched_auctions, batched_bids);
        let result_stream_port = result_stream.values().all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 11, 10, 100, 110),
                auction(2, 22, 20, 200, 220)
            ]);

            bidstream_in_port.send_many([
                bid(1, 55, 20, 105, "a"),
                bid(1, 66, 100, 107, "a"),
                bid(2, 55, 400, 210, "a"),
                bid(2, 66, 1, 215, "a")
            ]);

            result_stream_port.assert_yields_unordered([
                (1, "item".to_string(), 100, 107),
                (2, "item".to_string(), 400, 210),
            ]).await;
        });
    }

    // #[test] // broken
    fn test_q11() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (bidstream_in_port, bid_stream) = process.sim_input();
        let bid_batch = bid_stream.batch(&tick, nondet!(/** test **/));

        let result_stream = q11(bid_batch);
        let result_stream_port = result_stream.all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(1, 1, 20, 0, "a"),
                bid(2, 1, 20, 9, "a"),
                bid(3, 2, 20, 10, "a"),
                bid(4, 2, 20, 15, "a"),
                bid(4, 2, 20, 35, "a"),
                bid(5, 1, 20, 31, "a"),
                bid(6, 3, 20, 35, "a"),
            ]);

            result_stream_port.assert_yields_unordered([
                (1, 2, 0, 9),
                (2, 2, 10, 15),
            ]).await;
        });

    }

    #[test]
    fn test_q14() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q14(bid_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(1, 11, 3000000, 9, "0/1/2/3/4/5"),
                bid(2, 22, 60000000, 24, "0/1/2/3/4/5"),
                bid(3, 33, 4000000, 21, "a/b/c/d/e/f"),
            ]);

            result_stream_port.assert_yields([
                (1, 11, 2724000.0, "dayTime".to_string(), 9,  "abcdefgh".to_string(), 1),
                (3, 33, 3632000.0, "nightTime".to_string(), 21,  "abcdefgh".to_string(), 1)
            ]).await;
        });
    }

    #[test]
    fn test_q17() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q17(bid_stream);
        let result_stream_port = result_stream.snapshot(&tick, nondet!(/** Test **/)).values().all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(1, 1, 10000, 50, "a"),
                bid(1, 2, 5000, 50, "a"),
                bid(1, 3, 3000000, 50, "a"),
            ]);

            result_stream_port.assert_contains_unordered([
                (1, 50, 3, 1, 1, 1, 5000, 3000000, 1005000, 3015000)
            ]).await;
        });
    }

    // #[test] // not yet implemented: Reduce keyed with optional intermediates is not yet supported in simulator
    fn test_q18() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q18(bid_stream);
        let result_stream_port = result_stream.snapshot(&tick, nondet!(/** test **/)).values().all_ticks().sim_output();

        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(1, 1, 20, 0, "a"),
                bid(1, 1, 50, 100, "a"),
                bid(2, 1, 20, 200, "a"),
                bid(3, 5, 20, 300, "a"),
                bid(3, 5, 26, 400, "a"),
            ]);

            result_stream_port.assert_contains_unordered([
                (1, 1, 100, 50, 1),
                (1, 2, 200, 20, 0),
                (5, 6, 400, 26, 0)
            ]).await;
        });
    }

    // #[test] // broken
    fn test_q19() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();
        let tick = process.tick();

        let (bidstream_in_port, bid_stream) = process.sim_input();
        let batched_bids = bid_stream.batch(&tick, nondet!(/** test **/));

        let result_stream = q19(batched_bids);
        let result_stream_port = result_stream.all_ticks().sim_output();
        
        flow.sim().exhaustive(async || {
            bidstream_in_port.send_many([
                bid(1, 11, 1000, 1, "0/1/2/3/4/5"),
                bid(1, 22, 2000, 2, "0/1/2/3/4/5"),
                bid(1, 33, 3000, 3, "a/b/c/d/e/f"),
                bid(1, 33, 4000, 4, "a/b/c/d/e/f"),
                bid(1, 33, 5000, 5, "a/b/c/d/e/f"),
                bid(1, 33, 6000, 21, "a/b/c/d/e/f"),
                bid(1, 33, 7000, 21, "a/b/c/d/e/f"),
                bid(1, 33, 8000, 21, "a/b/c/d/e/f"),
                bid(1, 33, 9000, 21, "a/b/c/d/e/f"),
                bid(1, 33, 10000, 21, "a/b/c/d/e/f"),
                bid(1, 33, 10500, 21, "a/b/c/d/e/f"),
                bid(2, 11, 11000, 9, "0/1/2/3/4/5"),
                bid(2, 22, 12000, 24, "0/1/2/3/4/5"),
                bid(2, 33, 13000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 14000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 15000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 16000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 17000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 18000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 19000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 20000, 21, "a/b/c/d/e/f"),
                bid(2, 33, 21000, 21, "a/b/c/d/e/f"),
            ]);

            result_stream_port.assert_yields_only_unordered([
                (1, 1, 10500),
                (2, 1, 10000),
                (3, 1, 9000),
                (4, 1, 8000),
                (5, 1, 7000),
                (6, 1, 6000),
                (7, 1, 5000),
                (8, 1, 4000),
                (9, 1, 3000),
                (10, 1, 2000),
                (1, 2, 21000),
                (2, 2, 20000),
                (3, 2, 19000),
                (4, 2, 18000),
                (5, 2, 17000),
                (6, 2, 16000),
                (7, 2, 15000),
                (8, 2, 14000),
                (9, 2, 13000),
                (10, 2, 12000),
            ]).await;
        });
    }

    #[test]
    fn test_q20() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q20(bid_stream, auction_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 20, 10, 0, 200),
                auction(2, 21, 11, 100, 200),
            ]);

            bidstream_in_port.send_many([
                bid(1, 11, 150, 35, "0/1/2/3/4/5"),
                bid(2, 12, 100, 35, "0/1/2/3/4/5"),
                bid(1, 23, 200, 45, "a/b/c/d/e/f"),
            ]);

            result_stream_port.assert_yields_unordered([
                (1, 35, "item".to_string(), 0, 20, 10),
                (1, 45, "item".to_string(), 0, 20, 10)
            ]).await;
        });
    }

    #[test]
    fn test_q22() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (bidstream_in_port, bid_stream) = process.sim_input();

        let result_stream = q22(bid_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {

            bidstream_in_port.send_many([
                bid(11, 1, 100, 35, "0/1/2/3/4/5"),
                bid(12, 2, 200, 45, "a/b/c/d/e/f"),
                bid(13, 3, 300, 55, "z/y/x/v/u/r"),
            ]);

            result_stream_port.assert_yields([
                (11, 1, 100, "test".to_string(), "3".to_string(), "4".to_string(), "5".to_string()),
                (12, 2, 200, "test".to_string(), "d".to_string(), "e".to_string(), "f".to_string()),
                (13, 3, 300, "test".to_string(), "v".to_string(), "u".to_string(), "r".to_string()),
            ]).await;
        });
    }

    #[test]
    fn test_q23() {
        let mut flow = FlowBuilder::new();
        let process: Process<'_, Queries> = flow.process();

        let (auctionstream_in_port, auction_stream) = process.sim_input();
        let (bidstream_in_port, bid_stream) = process.sim_input();
        let (personstream_in_port, person_stream) = process.sim_input();

        let result_stream = q23(auction_stream, bid_stream, person_stream);
        let result_stream_port = result_stream.sim_output();

        flow.sim().exhaustive(async || {
            auctionstream_in_port.send_many([
                auction(1, 1, 10, 0, 10),
                auction(2, 2, 11, 10, 20),
                auction(3, 1, 12, 20, 30),
                auction(4, 4, 13, 30, 40),
                auction(5, 3, 14, 40, 50)
            ]);

            bidstream_in_port.send_many([
                bid(11, 1, 100, 35, "a"),
                bid(12, 2, 200, 45, "a"),
                bid(13, 3, 300, 55, "a"),
                bid(14, 2, 400, 60, "a")
            ]);

            personstream_in_port.send_many([
                person(1, "CA", 1),
                person(2, "OR", 2),
                person(3, "PA", 3),
            ]);
            result_stream_port.assert_yields_unordered([
                (auction(1, 1, 10, 0, 10), bid(11, 1, 100, 35, "a"), person(1, "CA", 1)),
                (auction(3, 1, 12, 20, 30), bid(11, 1, 100, 35, "a"), person(1, "CA", 1)),
                (auction(2, 2, 11, 10, 20), bid(12, 2, 200, 45, "a"), person(2, "OR", 2)),
                (auction(2, 2, 11, 10, 20), bid(14, 2, 400, 60, "a"), person(2, "OR", 2)),
                (auction(5, 3, 14, 40, 50), bid(13, 3, 300, 55, "a"), person(3, "PA", 3))
            ]).await;
        });
    }

}