"""Extraction of a static GTFS zip into the datasource's sqlite database.

The importer runs in a forked child, and on a small host that child can be
killed half way through. These tests drive the real pygtfs + sqlite path on a
tiny feed built per test, through the same entry points the integration uses
(`get_gtfs`, `check_extracting`), and check what a failed or interrupted
extraction leaves on disk: no partial artifacts, the previous database still
in place, no database with empty core tables in use, and a repair issue that
says so.

Repair issues go through `gtfs_helper.ir`, which is replaced by a Mock: what is
checked is that the integration asks Home Assistant for the issue, not how
Home Assistant renders it.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import sqlite3
import time
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import ha_stub

gtfs_helper = ha_stub.load("gtfs_helper")
pygtfs = gtfs_helper.pygtfs

GTFS_PATH = "gtfs2"
NAME = "feed"

AGENCY = ("agency_id,agency_name,agency_url,agency_timezone\n"
          "A1,Agency,https://example.com,Europe/Brussels\n")
STOPS = ("stop_id,stop_name,stop_lat,stop_lon\n"
         "S1,One,50.0,4.0\n"
         "S2,Two,50.1,4.1\n")
ROUTES = ("route_id,agency_id,route_short_name,route_long_name,route_type\n"
          "R1,A1,1,Line 1,3\n")
TRIPS = ("route_id,service_id,trip_id,shape_id\n"
         "R1,WK,T1,SH1\n")
STOP_TIMES_HEADER = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
STOP_TIMES = (STOP_TIMES_HEADER +
              "T1,08:00:00,08:00:00,S1,1\n"
              "T1,08:10:00,08:10:00,S2,2\n")
CALENDAR = ("service_id,monday,tuesday,wednesday,thursday,friday,saturday,"
            "sunday,start_date,end_date\n"
            "WK,1,1,1,1,1,0,0,20200101,20991231\n")
# a timetable that only starts running in the future, with the blank last
# line some publishers leave
FUTURE_CALENDAR = CALENDAR.replace("20200101", "20990101") + "\n"
SHAPES = ("shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
          "SH1,50.0,4.0,1\n")


def write_feed(path, *, calendar=CALENDAR, stop_times=STOP_TIMES, stop_name="One"):
    files = {
        "agency.txt": AGENCY,
        "stops.txt": STOPS.replace("S1,One", f"S1,{stop_name}"),
        "routes.txt": ROUTES,
        "trips.txt": TRIPS,
        "stop_times.txt": stop_times,
        "shapes.txt": SHAPES,
    }
    if calendar:
        files["calendar.txt"] = calendar
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)


def data(name=NAME, **extra):
    return {"file": name, "url": "na", "extract_from": "zip", **extra}


def wait_until_done(hass, name=NAME, timeout=60):
    deadline = time.monotonic() + timeout
    while gtfs_helper.check_extracting(hass, GTFS_PATH, name):
        assert time.monotonic() < deadline, "extraction did not finish in time"
        time.sleep(0.05)


def extract(hass, update=False):
    result = gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), update)
    assert result == "extracting"
    wait_until_done(hass)


def rows(db_path, table):
    with sqlite3.connect(db_path) as conn:
        return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def stop_name(db_path):
    with sqlite3.connect(db_path) as conn:
        return conn.execute("SELECT stop_name FROM stops WHERE stop_id = 'S1'").fetchone()[0]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def failure(gtfs_dir, name=NAME):
    return json.loads((gtfs_dir / f"{name}.extract_failed").read_text())


def age_failure(gtfs_dir, name=NAME):
    marker = gtfs_dir / f"{name}.extract_failed"
    content = json.loads(marker.read_text())
    content["at"] -= gtfs_helper.EXTRACTION_RETRY_DELAY.total_seconds() + 1
    marker.write_text(json.dumps(content))


def start_hanging_extraction(hass, gtfs_dir, monkeypatch):
    """Start an update whose child loads the staging database, then hangs.

    Returns the child's pid, for the test to kill it.
    """
    real_append_feed = pygtfs.append_feed

    def append_then_hang(schedule, feed_filename, **kwargs):
        real_append_feed(schedule, feed_filename, **kwargs)
        time.sleep(120)

    # the forked child inherits the patched module
    monkeypatch.setattr(pygtfs, "append_feed", append_then_hang)
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), True) == "extracting"
    deadline = time.monotonic() + 30
    while not (gtfs_dir / f"{NAME}.sqlite_new").exists():
        assert time.monotonic() < deadline, "extraction never wrote to staging"
        time.sleep(0.05)
    return json.loads((gtfs_dir / f"{NAME}.extracting").read_text())["pid"]


def serve(monkeypatch, *payloads):
    """Answer the feed downloads with these zip files' bytes, in turn."""
    responses = iter(payloads)
    get = Mock(side_effect=lambda *args, **kwargs: SimpleNamespace(
        content=next(responses).read_bytes(), raise_for_status=lambda: None))
    monkeypatch.setattr(gtfs_helper.requests, "get", get)
    return get


def close(schedule):
    schedule.session.close()
    schedule.engine.dispose()


def test_successful_extraction_replaces_live_db_and_leaves_no_artifacts(hass, gtfs_dir, ir):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source)
    before = digest(source)

    extract(hass)

    live = gtfs_dir / f"{NAME}.sqlite"
    assert rows(live, "stop_times") == 2
    assert rows(live, "shapes") == 0
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.sqlite", f"{NAME}.zip"]
    assert digest(source) == before
    schedule = gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False)
    assert not isinstance(schedule, str)
    close(schedule)
    ir.async_create_issue.assert_not_called()


def test_failed_extraction_removes_partial_artifacts(hass, gtfs_dir, ir):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source, calendar=None)
    before = digest(source)

    extract(hass)

    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.extract_failed", f"{NAME}.zip"]
    assert digest(source) == before


def test_failed_update_preserves_previous_database(hass, gtfs_dir, ir):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source, stop_name="Previous")
    extract(hass)
    live = gtfs_dir / f"{NAME}.sqlite"
    previous = digest(live)

    write_feed(source, calendar=None)
    extract(hass, update=True)

    assert digest(live) == previous
    schedule = gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False)
    assert not isinstance(schedule, str)
    close(schedule)
    assert stop_name(live) == "Previous"


def test_feed_with_empty_core_table_is_rejected(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip", stop_times=STOP_TIMES_HEADER)

    extract(hass)

    assert not (gtfs_dir / f"{NAME}.sqlite").exists()
    assert "stop_times" in failure(gtfs_dir)["error"]
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False) == "extraction_failed"


def test_existing_db_with_empty_core_tables_is_reextracted(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip")
    live = gtfs_dir / f"{NAME}.sqlite"
    # what an interrupted import used to leave behind: a feed row, no timetable
    partial = pygtfs.Schedule(str(live))
    partial.session.add(pygtfs.gtfs_entities.Feed(feed_name="partial"))
    partial.session.commit()
    close(partial)

    extract(hass)

    assert rows(live, "stop_times") == 2


def test_failed_extraction_raises_repair_issue(hass, gtfs_dir, ir):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source, calendar=None)

    extract(hass)

    ir.async_create_issue.assert_called()
    args, kwargs = ir.async_create_issue.call_args
    assert args == (hass, "gtfs2", f"extraction_failed_{NAME}")
    assert kwargs["translation_key"] == "extraction_failed"
    assert kwargs["severity"] == ir.IssueSeverity.ERROR
    assert kwargs["is_fixable"] is False
    assert kwargs["translation_placeholders"]["datasource"] == NAME
    assert "calendar" in kwargs["translation_placeholders"]["error"].lower()

    ir.async_delete_issue.reset_mock()
    write_feed(source)
    extract(hass, update=True)
    ir.async_delete_issue.assert_called_with(hass, "gtfs2", f"extraction_failed_{NAME}")


def test_killed_extraction_is_cleaned_up_and_reported(hass, gtfs_dir, ir, monkeypatch):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source, stop_name="Previous")
    extract(hass)
    live = gtfs_dir / f"{NAME}.sqlite"
    previous = digest(live)

    write_feed(source, stop_name="New")
    pid = start_hanging_extraction(hass, gtfs_dir, monkeypatch)
    assert gtfs_helper.check_extracting(hass, GTFS_PATH, NAME)
    os.kill(pid, signal.SIGKILL)

    wait_until_done(hass)

    assert sorted(os.listdir(gtfs_dir)) == [
        f"{NAME}.extract_failed", f"{NAME}.sqlite", f"{NAME}.zip"]
    assert digest(live) == previous
    assert "interrupted" in failure(gtfs_dir)["error"]
    _, kwargs = ir.async_create_issue.call_args
    assert "interrupted" in kwargs["translation_placeholders"]["error"]


def test_failed_extraction_is_retried_once_after_interval(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip", calendar=None)
    extract(hass)
    assert failure(gtfs_dir)["attempt"] == 1

    # not due yet: nothing is started
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False) == "extraction_failed"
    assert not (gtfs_dir / f"{NAME}.extracting").exists()

    age_failure(gtfs_dir)
    extract(hass)
    assert failure(gtfs_dir)["attempt"] == 2

    # the one retry is used up
    age_failure(gtfs_dir)
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False) == "extraction_failed"
    assert not (gtfs_dir / f"{NAME}.extracting").exists()
    assert failure(gtfs_dir)["attempt"] == 2


def test_automatic_retry_recovers_when_source_is_fixed(hass, gtfs_dir, ir):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source, calendar=None)
    extract(hass)

    write_feed(source)
    age_failure(gtfs_dir)
    extract(hass)

    assert rows(gtfs_dir / f"{NAME}.sqlite", "stop_times") == 2
    assert not (gtfs_dir / f"{NAME}.extract_failed").exists()
    ir.async_delete_issue.assert_called_with(hass, "gtfs2", f"extraction_failed_{NAME}")


def test_legacy_partial_zip_rewrite_is_recovered(hass, gtfs_dir, ir):
    original = gtfs_dir / f"{NAME}_temp.zip"
    write_feed(original)
    before = digest(original)
    (gtfs_dir / f"{NAME}_temp_out.zip").write_bytes(b"PK partial")

    assert not gtfs_helper.check_extracting(hass, GTFS_PATH, NAME)

    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.zip"]
    assert digest(gtfs_dir / f"{NAME}.zip") == before


def test_removing_a_datasource_while_it_extracts_is_refused(hass, gtfs_dir, ir, monkeypatch):
    write_feed(gtfs_dir / f"{NAME}.zip")
    pid = start_hanging_extraction(hass, gtfs_dir, monkeypatch)
    try:
        assert gtfs_helper.remove_datasource(hass, GTFS_PATH, NAME, True) == "extracting"
        assert (gtfs_dir / f"{NAME}.zip").exists()
        assert (gtfs_dir / f"{NAME}.sqlite_new").exists()
    finally:
        os.kill(pid, signal.SIGKILL)
    wait_until_done(hass)


def test_remove_datasource_removes_every_file(hass, gtfs_dir, ir):
    for suffix in (".zip", ".sqlite", ".sqlite-journal", ".sqlite_new",
                   ".sqlite_new-journal", ".extracting", ".extract_failed",
                   ".extract_failed.tmp", ".zip_download", "_temp.zip",
                   "_temp_out.zip", ".sqlite_current"):
        (gtfs_dir / f"{NAME}{suffix}").write_bytes(b"left over")

    assert gtfs_helper.remove_datasource(hass, GTFS_PATH, NAME, True) == "removed"

    assert os.listdir(gtfs_dir) == []


def test_leftovers_of_interrupted_writes_are_cleaned_up(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip")
    (gtfs_dir / f"{NAME}.zip_download").write_bytes(b"PK partial")
    (gtfs_dir / f"{NAME}.extract_failed.tmp").write_text('{"err')

    assert not gtfs_helper.check_extracting(hass, GTFS_PATH, NAME)

    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.zip"]


def test_legacy_sqlite_current_is_restored(hass, gtfs_dir, ir):
    check_legacy_sqlite_current(hass, gtfs_dir, live_exists=False)


def test_legacy_sqlite_current_next_to_live_db_is_removed(hass, gtfs_dir, ir):
    check_legacy_sqlite_current(hass, gtfs_dir, live_exists=True)


def check_legacy_sqlite_current(hass, gtfs_dir, live_exists):
    write_feed(gtfs_dir / f"{NAME}.zip", stop_name="Current")
    extract(hass)
    live = gtfs_dir / f"{NAME}.sqlite"
    current = gtfs_dir / f"{NAME}.sqlite_current"
    # what an earlier version's date check left: the database moved aside
    if live_exists:
        current.write_bytes(live.read_bytes())
    else:
        live.rename(current)
    before = digest(current)

    assert not gtfs_helper.check_extracting(hass, GTFS_PATH, NAME)

    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.sqlite", f"{NAME}.zip"]
    assert digest(live) == before


def test_locked_live_database_is_not_reextracted(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip")
    extract(hass)
    holder = sqlite3.connect(gtfs_dir / f"{NAME}.sqlite")
    holder.execute("BEGIN EXCLUSIVE")
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            gtfs_helper.get_gtfs(hass, GTFS_PATH, data(), False)
    finally:
        holder.rollback()
        holder.close()

    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.sqlite", f"{NAME}.zip"]


def test_route_departures_action_reports_failed_extraction(hass, gtfs_dir, ir):
    write_feed(gtfs_dir / f"{NAME}.zip", calendar=None)
    extract(hass)

    async def executor(func, *args):
        return func(*args)

    entry = SimpleNamespace(options={}, data={
        **data(), "name": "Line 1", "origin": "S1", "destination": "S2",
        "route_type": "3", "route": "R1"})
    hass.config_entries = SimpleNamespace(async_get_entry=lambda entry_id: entry)
    hass.async_add_executor_job = executor

    result = asyncio.run(gtfs_helper.get_route_departures(hass, {"config_entry": "id"}))

    assert result["today"] == [] and result["tomorrow"] == []
    assert result["extracting"] is False
    assert result["error"] == "extraction_failed"


def test_future_only_download_leaves_current_feed(hass, gtfs_dir, ir, monkeypatch, tmp_path):
    source = gtfs_dir / f"{NAME}.zip"
    write_feed(source)
    extract(hass)
    before = digest(source)
    future = tmp_path / "future.zip"
    write_feed(future, calendar=FUTURE_CALENDAR)
    serve(monkeypatch, future)

    result = gtfs_helper.get_gtfs(
        hass, GTFS_PATH, data(extract_from="url", check_source_dates=True), True)

    assert result is None
    assert digest(source) == before
    assert sorted(os.listdir(gtfs_dir)) == [f"{NAME}.sqlite", f"{NAME}.zip"]


def test_automatic_retry_downloads_url_feed_again(hass, gtfs_dir, ir, monkeypatch, tmp_path):
    broken, fixed = tmp_path / "broken.zip", tmp_path / "fixed.zip"
    write_feed(broken, calendar=None)
    write_feed(fixed)
    get = serve(monkeypatch, broken, fixed)
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(extract_from="url"), False) == "extracting"
    wait_until_done(hass)
    assert failure(gtfs_dir)["attempt"] == 1

    age_failure(gtfs_dir)
    assert gtfs_helper.get_gtfs(hass, GTFS_PATH, data(extract_from="url"), False) == "extracting"
    wait_until_done(hass)

    assert get.call_count == 2
    assert rows(gtfs_dir / f"{NAME}.sqlite", "stop_times") == 2
