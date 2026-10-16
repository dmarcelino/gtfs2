"""Filtered datasources: a database that only holds the routes the user retains.

A full feed can be far larger than the few routes a user follows (Carris
Metropolitana: 942 routes, 11 million stop times, 2.7 GB), which is too much
for a small host. A filtered datasource keeps the rest of the feed out of the
database: before pygtfs loads it, the feed is cut down to the retained routes
and what they need, so the full feed is never written to SQLite.

Two records say which routes a datasource retains, on purpose apart:
- <name>.filter.json is the request. Every extraction of the datasource reads
  it, whether started by the config flow, the update action or a retry.
- the gtfs2_retained_routes table is what the live database was built with.
  What the user is shown and checked against comes from there, so a rebuild
  that failed never claims routes the database in use does not hold.
They also differ when the feed has no trips for a requested route (any more):
the database is built without it, <name>.routes_missing says which, and the
request keeps it, so it comes back once a later feed has it again.
"""
from __future__ import annotations

import contextlib
import csv
import io
import json
import os
import sqlite3
import zipfile
from pathlib import Path

from .const import CONF_DEVICE_TRACKER_ID, CONF_FILE, CONF_ROUTE

FILTER_SUFFIX = ".filter.json"
# requested routes the feed has no trips for, left out of the live database
MISSING_ROUTES_SUFFIX = ".routes_missing"
# the cut-down feed, loose CSV files pygtfs loads instead of the zip
FILTERED_FEED_SUFFIX = ".filtered"
RETAINED_TABLE = "gtfs2_retained_routes"


def read_filter(gtfs_dir, name):
    """The route_ids requested for the datasource, or None for the full feed."""
    try:
        with open(os.path.join(gtfs_dir, name + FILTER_SUFFIX)) as f:
            return json.load(f)["routes"] or None
    except FileNotFoundError:
        return None


def write_filter(gtfs_dir, name, routes):
    """Request these routes for the datasource; no routes requests the full feed."""
    _write_routes(os.path.join(gtfs_dir, name + FILTER_SUFFIX), routes)


def read_missing_routes(gtfs_dir, name):
    """The requested routes the live database was built without, the feed
    having no trips for them.

    Only ever informs the user, so a record that cannot be read counts as none.
    """
    try:
        with open(os.path.join(gtfs_dir, name + MISSING_ROUTES_SUFFIX)) as f:
            return list(json.load(f)["routes"])
    except (FileNotFoundError, ValueError, KeyError, TypeError):
        return []


def write_missing_routes(gtfs_dir, name, routes):
    _write_routes(os.path.join(gtfs_dir, name + MISSING_ROUTES_SUFFIX), routes)


def _write_routes(path, routes):
    """Write {"routes": routes} to path whole, or remove it when there are none."""
    if not routes:
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        return
    with open(path + ".tmp", "w") as f:
        json.dump({"routes": sorted(set(routes))}, f)
    os.replace(path + ".tmp", path)


def feed_routes(source):
    """[(route_id, label)] of every route in the feed zip at source.

    Read from the zip itself, so routes can be offered before any database is
    built. Labels read like the route list of the sensor flow.
    """
    with zipfile.ZipFile(source) as archive:
        agencies = {row.get("agency_id", ""): row.get("agency_name", "")
                    for row in _dict_rows(archive, "agency.txt")}
        routes = list(_dict_rows(archive, "routes.txt"))
    # a feed of one agency may leave agency_id out of either file
    default_agency = next(iter(agencies.values()), "") if len(agencies) == 1 else ""

    def label(route):
        return route_label(route["route_id"], route.get("route_short_name"), route.get("route_long_name"),
                           agencies.get(route.get("agency_id", ""), default_agency))

    def order(route):
        route_id = route["route_id"]
        try:
            return (agencies.get(route.get("agency_id", ""), default_agency), 0, float(route_id), route_id)
        except ValueError:
            return (agencies.get(route.get("agency_id", ""), default_agency), 1, 0, route_id)

    return [(route["route_id"], label(route)) for route in sorted(routes, key=order)]


def route_label(route_id, short_name, long_name, agency_name):
    """How a route reads in the route lists: "route_id: (short - long) agency"."""
    names = " - ".join(name for name in (short_name, long_name) if name)
    return f"{route_id}: ({names}) {agency_name or ''}".rstrip()


def route_id_of(value):
    """The route_id of a route_label, or of the route a config entry holds."""
    return str(value).split(": (")[0]


def _dict_rows(archive, filename):
    if filename not in archive.namelist():
        return
    with archive.open(filename) as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        header = [column.strip() for column in next(reader, [])]
        for row in reader:
            if any(cell.strip() for cell in row):
                yield {column: cell.strip() for column, cell in zip(header, row)}


def write_filtered_feed(source, routes, out_dir, ignore_files=()):
    """Write what the routes need of the feed zip at source, as CSV files in out_dir.

    Each file is streamed once, in the order its keys become known: the routes
    give their trips, the trips their stop times and services, the stop times
    their stops. Rows are written as they were read. Files pygtfs would not
    load, or is told to ignore, are left out.
    """
    os.makedirs(out_dir)
    with zipfile.ZipFile(source) as archive:
        members = set(archive.namelist())

        def keep(filename, column, wanted, collect=(), keep_empty=False):
            if filename in members and filename not in ignore_files:
                return _filter_member(archive, out_dir, filename, column, wanted, collect, keep_empty)
            return {name: set() for name in collect}

        kept = keep("routes.txt", "route_id", set(routes), collect=("agency_id",))
        agency_ids = kept["agency_id"]
        kept = keep("trips.txt", "route_id", set(routes), collect=("trip_id", "service_id"))
        trip_ids, service_ids = kept["trip_id"], kept["service_id"]
        stop_ids = keep("stop_times.txt", "trip_id", trip_ids, collect=("stop_id",))["stop_id"]
        keep("stops.txt", "stop_id", _with_ancestors(archive, stop_ids))
        # a route without agency_id belongs to the feed's only agency
        if "" in agency_ids:
            _copy_member(archive, out_dir, "agency.txt")
        else:
            keep("agency.txt", "agency_id", agency_ids)
        keep("calendar.txt", "service_id", service_ids)
        keep("calendar_dates.txt", "service_id", service_ids)
        keep("frequencies.txt", "trip_id", trip_ids)
        keep("fare_rules.txt", "route_id", set(routes), keep_empty=True)
        handled = {"routes.txt", "trips.txt", "stop_times.txt", "stops.txt", "agency.txt",
                   "calendar.txt", "calendar_dates.txt", "frequencies.txt", "fare_rules.txt"}
        for filename in sorted(_loaded_files() - handled - set(ignore_files)):
            if filename in members:
                _copy_member(archive, out_dir, filename)
    return out_dir


def _loaded_files():
    """The files pygtfs loads from a feed."""
    from pygtfs.gtfs_entities import gtfs_all

    return {gtfs_class.__tablename__ + ".txt" for gtfs_class in gtfs_all}


def _filter_member(archive, out_dir, filename, column, wanted, collect, keep_empty):
    """Copy the rows of filename whose column is in wanted, and collect the
    values of the collect columns over the rows kept.

    Values are compared stripped, as pygtfs loads them. A file without the
    column cannot be filtered on it and is copied whole.
    """
    collected = {name: set() for name in collect}
    with archive.open(filename) as raw, open(
            os.path.join(out_dir, filename), "w", encoding="utf-8", newline="") as out:
        reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
        writer = csv.writer(out, lineterminator="\n")
        header = next(reader, [])
        writer.writerow(header)
        index = {name.strip(): i for i, name in enumerate(header)}
        key = index.get(column)
        collect_at = [(collected[name], index.get(name)) for name in collect]
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            if key is not None:
                value = row[key].strip() if key < len(row) else ""
                if value not in wanted and not (keep_empty and not value):
                    continue
            writer.writerow(row)
            for values, i in collect_at:
                values.add(row[i].strip() if i is not None and i < len(row) else "")
    return collected


def _copy_member(archive, out_dir, filename):
    if filename not in archive.namelist():
        return
    with archive.open(filename) as raw, open(os.path.join(out_dir, filename), "wb") as out:
        while chunk := raw.read(1 << 20):
            out.write(chunk)


def _with_ancestors(archive, stop_ids):
    """stop_ids and every parent_station above them: the stations their
    platforms belong to, which the stop lists and departures group by."""
    parents = {row["stop_id"]: row.get("parent_station", "")
               for row in _dict_rows(archive, "stops.txt")}
    kept = set()
    pending = list(stop_ids)
    while pending:
        stop_id = pending.pop()
        if stop_id in kept:
            continue
        kept.add(stop_id)
        if parents.get(stop_id):
            pending.append(parents[stop_id])
    return kept


def missing_route_data(db_path, routes):
    """The routes that have no trip with stop times in the database."""
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        return [route for route in sorted(routes) if not conn.execute(
            """SELECT EXISTS (SELECT 1 FROM trips t
                              INNER JOIN stop_times st ON st.trip_id = t.trip_id
                              WHERE t.route_id = ?)""", (route,)).fetchone()[0]]


def remove_routes(db_path, routes):
    """Take routes, and whatever trips they have without stop times, out of
    the database, so that no list offers them."""
    with contextlib.closing(sqlite3.connect(db_path)) as conn, conn:
        conn.executemany("DELETE FROM trips WHERE route_id = ?", [(route,) for route in routes])
        conn.executemany("DELETE FROM routes WHERE route_id = ?", [(route,) for route in routes])


def check_integrity(db_path):
    """Raise unless SQLite finds the database file sound."""
    with contextlib.closing(sqlite3.connect(db_path)) as conn:
        result = [row[0] for row in conn.execute("PRAGMA quick_check")]
    if result != ["ok"]:
        raise ValueError(f"the database failed its integrity check: {'; '.join(result[:5])}")


def record_retained_routes(db_path, routes):
    """Write down in the database which routes it was built with."""
    with contextlib.closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(f"CREATE TABLE {RETAINED_TABLE} (route_id TEXT PRIMARY KEY)")
        conn.executemany(f"INSERT INTO {RETAINED_TABLE} VALUES (?)", [(route,) for route in sorted(routes)])


def retained_routes(db_path):
    """The routes the database was built with, or None if it holds the full feed."""
    if not os.path.exists(db_path):
        return None
    uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    with contextlib.closing(sqlite3.connect(uri, uri=True)) as conn:
        try:
            return [row[0] for row in conn.execute(f"SELECT route_id FROM {RETAINED_TABLE} ORDER BY route_id")]
        except sqlite3.OperationalError as ex:
            if "no such table" in str(ex):
                return None
            raise


def routes_in_use(entries, name, routes):
    """The config entries on datasource name that need a route not in routes.

    A train sensor rides whichever rail routes link its two cities, so it
    needs the full feed: any filter leaves it without some. Sensors near a
    location follow no route.
    """
    retained = set(routes)
    return [entry for entry in entries
            if entry.data.get(CONF_FILE) == name
            and CONF_DEVICE_TRACKER_ID not in entry.data
            and entry.data.get(CONF_ROUTE) is not None
            and (entry.data[CONF_ROUTE] == "train" or route_id_of(entry.data[CONF_ROUTE]) not in retained)]
