
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="cric_data" \
    -v $(pwd)/cric_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13


CREATE TABLE IF NOT EXISTS wickets (
    batter text NOT NULL,
    bowler text NOT NULL,
    non_striker text NOT NULL,
    runs jsonb,
    wickets jsonb,
    over_no int NOT NULL,
    ball_no int NOT NULL,
    match_id bigint NOT NULL,
    innings int NOT NULL,
    extras jsonb
);


CREATE TABLE IF NOT EXISTS runs (
    batter text NOT NULL,
    bowler text NOT NULL,
    non_striker text NOT NULL,
    runs jsonb,
    over_no int NOT NULL,
    ball_no int NOT NULL,
    match_id bigint NOT NULL,
    innings int NOT NULL,
    extras jsonb,
    replacements jsonb
);


CREATE TABLE IF NOT EXISTS match (
    id bigint NOT NULL,
    venue text,
    city text,
    toss jsonb,
    team_type text,
    season text,
    teams jsonb,
    officials jsonb,
    player_of_match jsonb,
    outcome jsonb,
    dates jsonb,
    match_type text,
    CONSTRAINT match_pkey PRIMARY KEY ( id )
);
