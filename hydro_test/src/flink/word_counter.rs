use hydro_lang::prelude::*;

pub struct WordServer;

pub fn word_counter_service<'a>(
    texts_stream: Stream<String, Process<'a, WordServer>>,
) -> KeyedStream<String, i32, Process<'a, WordServer>> {

    let atomic_tick = texts_stream.location().tick();
    let texts_stream_processing = texts_stream.atomic(&atomic_tick);
    let word_count = texts_stream_processing
        // Parse sentences into individual words
        .flat_map_ordered(q!(|sentence| {
            sentence
                .split_whitespace()
                .map(|w| w.to_string())
                .collect::<Vec<String>>()
        }))
        .map(q!(|word| (word, 1)))
        // Group each word occurrence together
        .into_keyed()
        // Sum the occurrences per word
        .reduce(q!(|acc, one| *acc += one));

    let response = sliced! {
        let count_snapshot = use::atomic(word_count, nondet!(/** atomicity guarantees consistency wrt new texts */));
        count_snapshot.into_keyed_stream()
    };

    response
}

#[cfg(test)]
mod tests {
    use hydro_lang::prelude::*;

    use super::*;

    #[test]
    fn test_word_counter() {
        let flow = FlowBuilder::new();
        let process = flow.process::<WordServer>();

        let (texts_in_port, texts_stream) = process.sim_input();
        let count_responses = word_counter_service(texts_stream);
        let count_out_port = count_responses.entries().sim_output();

        flow.sim().exhaustive(async || {
            texts_in_port.send("One Two Two Three Three Three".to_string());
            count_out_port.assert_yields_unordered([
                    ("One".to_string(), 1),
                    ("Two".to_string(), 2),
                    ("Three".to_string(), 3)
            ]).await;
        });
    }
}