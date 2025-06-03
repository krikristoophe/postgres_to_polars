use std::time::Instant;

use postgres_to_polars::{BinaryParam, Client, ClientOptions};

const USERNAME: &str = "POSTGRES_USER";
const PASSWORD: &str = "pgpassword";
const DATABASE: &str = "pg-database";

fn main() -> anyhow::Result<()> {
    let client = Client::new(ClientOptions::new(
        String::from(USERNAME),
        String::from(PASSWORD),
        String::from(DATABASE),
        String::from("127.0.0.1"),
        5432,
    ));

    client.connect()?;

    let t0 = Instant::now();

    let query = r#"select
        e.id,
        e.slot_id::text,
        e.user_id as "users.id",
        e.date,
        coalesce(special_events.special_types, '{}') as "events.special_event_types"
    from
        events e
        left join lateral (
            SELECT array_agg(set2.type) AS special_types
            FROM event_special_event_types eset
            JOIN special_event_types set2 ON eset.special_event_type_id = set2.id
            WHERE eset.event_id = e.id
	) special_events ON true
	join slots s on e.slot_id = s.id
	join rooms_professions rp on s.room_profession_id = rp.id
    where
        (
            e.date between date($1)
            and date($2)
        )
        and ($3 < 0 or e.user_id = $3)
        and e.status_type = 'WORK'
        and not e.is_archived
        and rp.profession_id = $4
    "#;

    let df = client.query(
        query,
        vec![
            Some(BinaryParam::Text(String::from("2024-01-01"))),
            Some(BinaryParam::Text(String::from("2025-12-30"))),
            Some(BinaryParam::Int4(-1)),
            Some(BinaryParam::Int4(24)),
        ],
    )?;

    let t1 = Instant::now();
    let elapsed = t1.duration_since(t0);

    println!("Elapsed time: {:?}", elapsed);
    println!("{}", df);
    Ok(())
}
