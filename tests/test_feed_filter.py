"""Filtered datasources: a database built with only the routes the user retains.

Like test_extraction, these drive the real pygtfs + sqlite path through the
entry points the integration uses (get_gtfs, rebuild_with_routes,
check_extracting) on a tiny feed built per test, and check the database that
comes out with sqlite3.

The feed has an agency, a station with a platform, and five routes:
- R1 rides P1 (platform of station ST) -> S2 on weekday service WK
- R2 rides S5 -> S2 on weekend service WE
- R3, never retained here, rides S2 -> S4, sharing stop S2 and service WK with
  R1, and also runs on XO, a service of its own only in calendar_dates
- R5 rides S2 -> S5 on OLD, a service that ended years ago
- R0 is in routes.txt, without a single trip
"""
from __future__ import annotations

import datetime
import os
import signal
import sqlite3
import zipfile
from pathlib import Path
from types import SimpleNamespace

from freezegun import freeze_time
from sqlalchemy import event

import ha_stub
from test_extraction import (
    GTFS_PATH,
    close,
    digest,
    failure,
    rows,
    start_hanging_extraction,
    wait_until_done,
)

gtfs_helper = ha_stub.load("gtfs_helper")
feed_filter = ha_stub.load("feed_filter")

NAME = "feed"
PALMBUS = Path(__file__).parent.parent / "tests_provider" / "fixtures" / "palmbus" / "static.zip"

FEED = {
    "agency.txt": (
        "agency_id,agency_name,agency_url,agency_timezone\n"
        "A1,Agency,https://example.com,Europe/Lisbon\n"),
    "stops.txt": (
        "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
        "ST,Station,38.70,-9.10,1,\n"
        "P1,Station,38.70,-9.10,0,ST\n"
        "S2,Two,38.71,-9.11,0,\n"
        "S4,Four,38.72,-9.12,0,\n"
        "S5,Five,38.73,-9.13,0,\n"),
    "routes.txt": (
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        "R0,A1,0,Line 0,3\n"
        "R1,A1,1,Line 1,3\n"
        "R2,A1,2,Line 2,3\n"
        "R3,A1,3,Line 3,3\n"
        "R5,A1,5,Line 5,3\n"),
    "trips.txt": (
        "route_id,service_id,trip_id,shape_id\n"
        "R1,WK,T1,SH1\n"
        "R2,WE,T2,\n"
        "R3,WK,T3,\n"
        "R3,XO,T3X,\n"
        "R5,OLD,T5,\n"),
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T1,08:00:00,08:00:00,P1,1\n"
        "T1,08:10:00,08:10:00,S2,2\n"
        "T2,09:00:00,09:00:00,S5,1\n"
        "T2,09:10:00,09:10:00,S2,2\n"
        "T3,10:00:00,10:00:00,S2,1\n"
        "T3,10:10:00,10:10:00,S4,2\n"
        "T3X,11:00:00,11:00:00,S2,1\n"
        "T3X,11:10:00,11:10:00,S4,2\n"
        "T5,12:00:00,12:00:00,S2,1\n"
        "T5,12:10:00,12:10:00,S5,2\n"),
    "calendar.txt": (
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
        "WK,1,1,1,1,1,0,0,20200101,20991231\n"
        "WE,0,0,0,0,0,1,1,20200101,20991231\n"
        "OLD,1,1,1,1,1,1,1,20100101,20101231\n"),
    "calendar_dates.txt": (
        "service_id,date,exception_type\n"
        "WK,20301225,2\n"
        "XO,20301225,1\n"),
    "frequencies.txt": (
        "trip_id,start_time,end_time,headway_secs\n"
        "T1,08:00:00,09:00:00,600\n"
        "T3,10:00:00,11:00:00,600\n"),
    "shapes.txt": (
        "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        "SH1,38.70,-9.10,1\n"),
    "feed_info.txt": (
        "feed_publisher_name,feed_publisher_url,feed_lang\n"
        "Publisher,https://example.com,pt\n"),
}


def write_feed(path, **replace):
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in {**FEED, **replace}.items():
            archive.writestr(name, content)


def data(**extra):
    return {"file": NAME, "url": "na", "extract_from": "zip", **extra}


def build(hass, gtfs_dir, routes, **replace):
    """Extract a new datasource retaining routes, as the config flow does."""
    write_feed(gtfs_dir / f"{NAME}.zip", **replace)
    rebuild(hass, routes)


def rebuild(hass, routes):
    assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, routes) == "started"
    wait_until_done(hass, NAME)


def ids(db_path, table, column):
    with sqlite3.connect(db_path) as conn:
        return {row[0] for row in conn.execute(f"SELECT {column} FROM {table}")}


def live(gtfs_dir):
    return gtfs_dir / f"{NAME}.sqlite"


def test_one_retained_route_keeps_only_what_it_needs(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])

    db = live(gtfs_dir)
    assert ids(db, "routes", "route_id") == {"R1"}
    assert ids(db, "trips", "trip_id") == {"T1"}
    assert ids(db, "stop_times", "trip_id") == {"T1"}
    # the platform's station comes along with it
    assert ids(db, "stops", "stop_id") == {"ST", "P1", "S2"}
    assert ids(db, "agency", "agency_id") == {"A1"}
    assert ids(db, "calendar", "service_id") == {"WK"}
    assert ids(db, "calendar_dates", "service_id") == {"WK"}
    assert ids(db, "frequencies", "trip_id") == {"T1"}
    assert rows(db, "feed_info") == 1
    assert rows(db, "shapes") == 0
    assert feed_filter.retained_routes(db) == ["R1"]
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.filter.json", f"{NAME}.sqlite", f"{NAME}.zip"]
    schedule = gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False)
    assert not isinstance(schedule, str)
    close(schedule)


def test_multiple_retained_routes_keep_the_data_of_each(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1", "R2"])

    db = live(gtfs_dir)
    assert ids(db, "routes", "route_id") == {"R1", "R2"}
    assert ids(db, "trips", "trip_id") == {"T1", "T2"}
    assert ids(db, "stops", "stop_id") == {"ST", "P1", "S2", "S5"}
    assert ids(db, "calendar", "service_id") == {"WK", "WE"}
    assert feed_filter.retained_routes(db) == ["R1", "R2"]


def test_stops_and_services_shared_with_excluded_routes_are_kept(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])

    db = live(gtfs_dir)
    # S2 and WK are R3's too: kept for R1
    assert "S2" in ids(db, "stops", "stop_id")
    assert "WK" in ids(db, "calendar", "service_id")
    # what only R3 uses is gone: its trips, its own stop and its own service
    assert not ids(db, "trips", "trip_id") & {"T3", "T3X"}
    assert not ids(db, "stop_times", "trip_id") & {"T3", "T3X"}
    assert "S4" not in ids(db, "stops", "stop_id")
    assert "XO" not in ids(db, "calendar_dates", "service_id")
    assert "T3" not in ids(db, "frequencies", "trip_id")


def test_route_without_current_trips_is_built(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R5"])

    db = live(gtfs_dir)
    assert ids(db, "trips", "trip_id") == {"T5"}
    assert ids(db, "calendar", "service_id") == {"OLD"}
    assert not (gtfs_dir / f"{NAME}.extract_failed").exists()


def test_route_without_trips_is_left_out_with_a_warning(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1", "R2"])
    db = live(gtfs_dir)
    # the operator drops R2 from the feed
    write_feed(gtfs_dir / f"{NAME}.zip", **{"trips.txt": FEED["trips.txt"].replace("R2,WE,T2,\n", "")})

    rebuild(hass, ["R1", "R2"])

    assert not (gtfs_dir / f"{NAME}.extract_failed").exists()
    assert ids(db, "routes", "route_id") == {"R1"}
    assert feed_filter.retained_routes(db) == ["R1"]
    # still requested: it comes back with a feed that has it again
    assert feed_filter.read_filter(str(gtfs_dir), NAME) == ["R1", "R2"]
    ir.async_create_issue.assert_called_once()
    args, kwargs = ir.async_create_issue.call_args
    assert args == (hass, "gtfs2", f"routes_missing_{NAME}")
    assert kwargs["translation_key"] == "routes_missing"
    assert kwargs["severity"] == ir.IssueSeverity.WARNING
    assert kwargs["translation_placeholders"] == {"datasource": NAME, "routes": "R2"}

    # no longer asking for R2 clears the warning, without rebuilding
    previous = digest(db)
    assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, ["R1"], only_if_changed=True) == "unchanged"
    assert digest(db) == previous
    assert feed_filter.read_filter(str(gtfs_dir), NAME) == ["R1"]
    ir.async_delete_issue.assert_called_with(hass, "gtfs2", f"routes_missing_{NAME}")
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.filter.json", f"{NAME}.sqlite", f"{NAME}.zip"]


def test_no_retained_route_with_trips_fails_and_keeps_the_live_db(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    db = live(gtfs_dir)
    previous = digest(db)

    rebuild(hass, ["R0"])

    assert digest(db) == previous
    assert feed_filter.retained_routes(db) == ["R1"]
    assert "R0" in failure(gtfs_dir, NAME)["error"]
    assert not (gtfs_dir / f"{NAME}.filtered").exists()
    assert not (gtfs_dir / f"{NAME}.sqlite_new").exists()
    _, kwargs = ir.async_create_issue.call_args
    assert "R0" in kwargs["translation_placeholders"]["error"]


def test_rebuild_is_refused_while_an_extraction_runs(hass, gtfs_dir, ir, monkeypatch):
    build(hass, gtfs_dir, ["R1"])
    pid = start_hanging_extraction(hass, gtfs_dir, monkeypatch)
    try:
        assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, ["R1", "R2"]) == "extracting"
        # the request the running extraction reads is left alone
        assert feed_filter.read_filter(str(gtfs_dir), NAME) == ["R1"]
    finally:
        os.kill(pid, signal.SIGKILL)
    wait_until_done(hass, NAME)


def test_saving_the_routes_the_database_holds_does_not_rebuild(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    db = live(gtfs_dir)
    previous = digest(db)

    assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, ["R1"], only_if_changed=True) == "unchanged"
    assert not gtfs_helper.check_extracting(hass, GTFS_PATH, NAME)
    assert digest(db) == previous

    # after a failed rebuild, saving the same routes tries again
    write_feed(gtfs_dir / f"{NAME}.zip", **{"stop_times.txt": FEED["stop_times.txt"].replace("08:10:00", "not a time")})
    rebuild(hass, ["R1"])
    write_feed(gtfs_dir / f"{NAME}.zip")
    assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, ["R1"], only_if_changed=True) == "started"
    wait_until_done(hass, NAME)
    assert not (gtfs_dir / f"{NAME}.extract_failed").exists()


def test_saving_a_full_datasource_unfiltered_does_not_rebuild(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip")
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False) == "extracting"
    wait_until_done(hass, NAME)
    previous = digest(live(gtfs_dir))

    assert gtfs_helper.rebuild_with_routes(hass, GTFS_PATH, NAME, [], only_if_changed=True) == "unchanged"
    assert digest(live(gtfs_dir)) == previous


def test_retained_routes_can_be_added_and_removed(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    db = live(gtfs_dir)

    rebuild(hass, ["R1", "R2"])
    assert ids(db, "routes", "route_id") == {"R1", "R2"}
    assert ids(db, "trips", "trip_id") == {"T1", "T2"}
    assert feed_filter.retained_routes(db) == ["R1", "R2"]

    rebuild(hass, ["R2"])
    assert ids(db, "routes", "route_id") == {"R2"}
    assert ids(db, "stops", "stop_id") == {"S2", "S5"}
    assert feed_filter.retained_routes(db) == ["R2"]

    # retaining nothing goes back to the full feed
    rebuild(hass, [])
    assert ids(db, "routes", "route_id") == {"R0", "R1", "R2", "R3", "R5"}
    assert feed_filter.retained_routes(db) is None
    assert not (gtfs_dir / f"{NAME}.filter.json").exists()


def test_failed_filtered_rebuild_keeps_the_live_db_and_no_temp_files(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    db = live(gtfs_dir)
    previous = digest(db)

    write_feed(gtfs_dir / f"{NAME}.zip", **{"stop_times.txt": FEED["stop_times.txt"].replace("08:10:00", "not a time")})
    rebuild(hass, ["R1", "R2"])

    assert "not a time" in failure(gtfs_dir, NAME)["error"]
    assert digest(db) == previous
    assert sorted(os.listdir(gtfs_dir)) == [
        f"{NAME}.extract_failed", f"{NAME}.filter.json", f"{NAME}.sqlite", f"{NAME}.zip"]


def test_interrupted_filtered_rebuild_keeps_the_live_db_and_no_temp_files(hass, gtfs_dir, ir, monkeypatch):
    build(hass, gtfs_dir, ["R1"])
    db = live(gtfs_dir)
    previous = digest(db)

    feed_filter.write_filter(str(gtfs_dir), NAME, ["R1", "R2"])
    pid = start_hanging_extraction(hass, gtfs_dir, monkeypatch)
    # the filtered feed is written before pygtfs loads it
    assert (gtfs_dir / f"{NAME}.filtered").is_dir()
    os.kill(pid, signal.SIGKILL)
    wait_until_done(hass, NAME)

    assert digest(db) == previous
    assert "interrupted" in failure(gtfs_dir, NAME)["error"]
    assert sorted(os.listdir(gtfs_dir)) == [
        f"{NAME}.extract_failed", f"{NAME}.filter.json", f"{NAME}.sqlite", f"{NAME}.zip"]


def test_full_feed_datasource_is_unchanged(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip")

    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False) == "extracting"
    wait_until_done(hass, NAME)

    db = live(gtfs_dir)
    assert ids(db, "routes", "route_id") == {"R0", "R1", "R2", "R3", "R5"}
    assert rows(db, "stop_times") == 10
    assert feed_filter.retained_routes(db) is None
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.sqlite", f"{NAME}.zip"]


def test_datasource_labels_name_the_retained_routes(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / "full.zip")
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(file="full"), False) == "extracting"
    wait_until_done(hass, "full")
    build(hass, gtfs_dir, ["R1", "R2"])

    # a filtered setup left at its routes, and what an older version's
    # interrupted zip rewrite left
    write_feed(gtfs_dir / "abandoned.zip")
    write_feed(gtfs_dir / "old_temp.zip")

    assert gtfs_helper._datasource_labels(str(gtfs_dir)) == {
        "full": "full",
        NAME: f"{NAME} (filtered: 2 routes - R1, R2)",
    }
    assert gtfs_helper._datasource_labels(str(gtfs_dir), include_unbuilt=True) == {
        "full": "full",
        NAME: f"{NAME} (filtered: 2 routes - R1, R2)",
        "abandoned": "abandoned (not built)",
    }


def test_filtered_setup_always_downloads_the_feed(hass, gtfs_dir, ir, monkeypatch, tmp_path):
    # left by a setup of the same name that never got past its routes
    write_feed(gtfs_dir / f"{NAME}.zip", **{"stops.txt": FEED["stops.txt"].replace("S2,Two", "S2,Stale")})
    fresh = tmp_path / "fresh.zip"
    write_feed(fresh)
    monkeypatch.setattr(gtfs_helper.requests, "get", lambda *args, **kwargs: SimpleNamespace(
        content=fresh.read_bytes(), raise_for_status=lambda: None))

    assert gtfs_helper.fetch_source(hass, GTFS_PATH, data(extract_from="url")) is None

    assert digest(gtfs_dir / f"{NAME}.zip") == digest(fresh)
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.zip"]


def test_url_update_keeps_the_datasource_filtered(hass, gtfs_dir, ir, monkeypatch, tmp_path):
    build(hass, gtfs_dir, ["R1"])
    newer = tmp_path / "newer.zip"
    write_feed(newer, **{"stops.txt": FEED["stops.txt"].replace("S2,Two", "S2,Renamed")})
    monkeypatch.setattr(gtfs_helper.requests, "get", lambda *args, **kwargs: SimpleNamespace(
        content=newer.read_bytes(), raise_for_status=lambda: None))

    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(extract_from="url"), True) == "extracting"
    wait_until_done(hass, NAME)

    db = live(gtfs_dir)
    assert ids(db, "routes", "route_id") == {"R1"}
    assert ids(db, "stops", "stop_name") == {"Station", "Renamed"}


def test_remove_datasource_removes_the_filter(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    (gtfs_dir / f"{NAME}.filtered").mkdir()
    for suffix in (".filter.json.tmp", ".routes_missing", ".routes_missing.tmp"):
        (gtfs_dir / f"{NAME}{suffix}").write_text("left over")

    assert gtfs_helper.remove_datasource(hass, GTFS_PATH, NAME, True) == "removed"

    assert os.listdir(gtfs_dir) == []


def test_departures_match_the_full_feed(hass, gtfs_dir, ir):
    """Every ride of Palm Bus line B departs as it does from the full feed.

    Line B shares Gare SNCF de Cannes with the lines left out.
    """
    for name, routes in (("full", []), ("filtered", ["B"])):
        (gtfs_dir / f"{name}.zip").write_bytes(PALMBUS.read_bytes())
        feed_filter.write_filter(str(gtfs_dir), name, routes)
        assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(file=name), False) == "extracting"
        wait_until_done(hass, name)
    assert ids(gtfs_dir / "filtered.sqlite", "routes", "route_id") == {"B"}
    assert ids(gtfs_dir / "full.sqlite", "routes", "route_id") == {"21", "22", "A", "B"}

    with sqlite3.connect(gtfs_dir / "full.sqlite") as conn:
        pairs = conn.execute("""
            SELECT DISTINCT o.stop_id, d.stop_id FROM trips t
            INNER JOIN stop_times o ON o.trip_id = t.trip_id
            INNER JOIN stop_times d ON d.trip_id = t.trip_id AND d.stop_sequence > o.stop_sequence
            WHERE t.route_id = 'B' AND o.stop_sequence = (
                SELECT min(stop_sequence) FROM stop_times WHERE trip_id = t.trip_id)
            ORDER BY 1, 2""").fetchall()
    assert pairs

    schedules = {name: gtfs_helper.get_gtfs(hass, GTFS_PATH, data(file=name), False)
                 for name in ("full", "filtered")}
    try:
        for schedule in schedules.values():
            _freeze_sqlite_now(schedule.engine)
        found = 0
        with freeze_time("2026-09-02 06:00:00"):
            for origin, destination in pairs:
                full, filtered = (
                    gtfs_helper._fetch_departure_rows("3", origin, destination, schedules[name], route="B")
                    for name in ("full", "filtered"))
                assert filtered == full, (origin, destination)
                found += len(full[0])
        assert found
    finally:
        for schedule in schedules.values():
            close(schedule)


def _freeze_sqlite_now(engine):
    """Have SQLite's 'now' answer with freezegun's clock (see tests_provider/fixture_db.py)."""
    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def substitute_now(conn, cursor, statement, parameters, context, executemany):
        if "'now'" in statement:
            frozen = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            statement = statement.replace("'now'", f"'{frozen}'")
        return statement, parameters


def test_feed_routes_lists_the_routes_of_the_zip(tmp_path):
    write_feed(tmp_path / "feed.zip")

    assert feed_filter.feed_routes(tmp_path / "feed.zip") == [
        ("R0", "R0: (0 - Line 0) Agency"),
        ("R1", "R1: (1 - Line 1) Agency"),
        ("R2", "R2: (2 - Line 2) Agency"),
        ("R3", "R3: (3 - Line 3) Agency"),
        ("R5", "R5: (5 - Line 5) Agency"),
    ]


def test_route_labels_parse_back_to_their_route():
    label = feed_filter.route_label("[A2L1N]4001_0", "4001", None, "Carris Metropolitana")

    assert label == "[A2L1N]4001_0: (4001) Carris Metropolitana"
    assert gtfs_helper.parse_route_option(f"3##{label}") == ("3", "[A2L1N]4001_0")
    assert feed_filter.route_id_of("[A2L1N]4001_0") == "[A2L1N]4001_0"


def test_routes_in_use_names_sensors_on_dropped_routes():
    def entry(title, **entry_data):
        return SimpleNamespace(title=title, data={"file": NAME, **entry_data})

    on_r1 = entry("On R1", route="R1")
    on_r2 = entry("On R2", route="R2")
    entries = [
        on_r1,
        on_r2,
        entry("Near me", device_tracker_id="person.me"),
        SimpleNamespace(title="Other datasource", data={"file": "other", "route": "R2"}),
    ]

    assert feed_filter.routes_in_use(entries, NAME, ["R1"]) == [on_r2]
    assert feed_filter.routes_in_use(entries, NAME, ["R1", "R2"]) == []


def test_train_sensors_block_any_filter():
    train = SimpleNamespace(title="Train", data={"file": NAME, "route": "train"})

    # it rides whichever rail routes link its cities: a filter leaves some out
    assert feed_filter.routes_in_use([train], NAME, ["R1", "R2"]) == [train]


def test_route_left_out_tells_a_filtered_route_from_an_unknown_one(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])

    assert gtfs_helper.route_left_out(hass, GTFS_PATH, NAME, "R2") == ["R1"]
    assert gtfs_helper.route_left_out(hass, GTFS_PATH, NAME, "R99") is None
    assert gtfs_helper.route_left_out(hass, GTFS_PATH, NAME, "") is None
    assert gtfs_helper.route_left_out(hass, GTFS_PATH, NAME, "R1") is None


def test_unreadable_route_records_do_not_break_the_route_choice(hass, gtfs_dir, ir):
    build(hass, gtfs_dir, ["R1"])
    (gtfs_dir / f"{NAME}.filter.json").write_text('{"rou')

    options, selected = gtfs_helper.retained_route_choices(hass, GTFS_PATH, NAME)

    assert [route_id for route_id, _ in options] == ["R0", "R1", "R2", "R3", "R5"]
    # what the database holds
    assert selected == ["R1"]

    live(gtfs_dir).write_bytes(b"not a database")
    _, selected = gtfs_helper.retained_route_choices(hass, GTFS_PATH, NAME)
    assert selected == []
    assert gtfs_helper.route_left_out(hass, GTFS_PATH, NAME, "R2") is None
