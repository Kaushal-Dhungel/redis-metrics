import json
import pickle
from collections import Counter
from datetime import datetime
from math import ceil

from django.conf import settings
import redis


DEFAULT_SCAN_COUNT = 200
DEFAULT_PAGE_SIZE = 25
DEFAULT_PREVIEW_LENGTH = 120
DEFAULT_SAMPLE_LIMIT = 250
DEFAULT_SLOWLOG_LIMIT = 10
DEFAULT_TIMESERIES_POINTS = 24
SNAPSHOT_KEY_PREFIX = "__redis_metrics__:snapshots:"
DEFAULT_LIVE_WINDOW = 20


def safe_decode(value):
    if value is None:
        return ""

    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None

    if isinstance(value, str):
        return value

    return str(value)


def maybe_decode(value):
    decoded = safe_decode(value)
    return decoded if decoded is not None else "[binary]"


def build_preview(value, max_length=DEFAULT_PREVIEW_LENGTH):
    if value is None:
        return "[binary / non-text value]"

    if len(value) <= max_length:
        return value

    return value[:max_length] + "..."


def format_ttl(ttl):
    if ttl == -1:
        return "No expiry"
    if ttl == -2:
        return "Missing"
    return f"{ttl}s"


def format_number(value):
    if value is None:
        return "0"
    return f"{int(value):,}"


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def format_bytes(value):
    size = float(value or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f}{unit}" if unit != "B" else f"{int(size)}B"
        size /= 1024


def format_duration(seconds):
    seconds = safe_int(seconds)
    if seconds <= 0:
        return "0s"

    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts[:3])


def percent(value, total):
    if not total:
        return 0.0
    return round((value / total) * 100, 2)


def chart_points(values):
    cleaned = [float(v or 0) for v in values]
    if not cleaned:
        return ""
    max_value = max(cleaned) or 1
    x_step = 100 / max(len(cleaned) - 1, 1)
    points = []
    for index, value in enumerate(cleaned):
        x_pos = round(index * x_step, 2)
        y_pos = round(36 - ((value / max_value) * 30), 2)
        points.append(f"{x_pos},{y_pos}")
    return " ".join(points)


def normalize_connection_entry(name, value):
    if isinstance(value, str):
        return {
            "name": name,
            "label": name.replace("_", " ").title(),
            "url": value,
            "key_prefix": "",
            "role": infer_connection_role(name, name),
        }

    if isinstance(value, dict):
        url = value.get("LOCATION") or value.get("URL") or value.get("url")
        if not url:
            return None
        return {
            "name": name,
            "label": value.get("LABEL") or name.replace("_", " ").title(),
            "url": url,
            "key_prefix": value.get("KEY_PREFIX") or value.get("key_prefix") or "",
            "role": value.get("ROLE") or value.get("role") or infer_connection_role(name, value.get("LABEL") or name),
        }

    return None


def infer_connection_role(name, label):
    haystack = f"{name} {label}".lower()
    if "celery" in haystack:
        return "celery"
    if "rate" in haystack and "limit" in haystack:
        return "rate_limit"
    if "cache" in haystack:
        return "cache"
    return "generic"


def get_configured_connections():
    configured = {}
    explicit = getattr(settings, "REDIS_METRICS_CONNECTIONS", None)
    if explicit:
        for name, value in explicit.items():
            normalized = normalize_connection_entry(name, value)
            if normalized:
                configured[name] = normalized

    for name, cache_config in getattr(settings, "CACHES", {}).items():
        backend = (cache_config.get("BACKEND") or "").lower()
        location = cache_config.get("LOCATION")
        if "redis" not in backend or not location:
            continue
        if name in configured:
            continue
        configured[name] = {
            "name": name,
            "label": name.replace("_", " ").title(),
            "url": location,
            "key_prefix": cache_config.get("KEY_PREFIX", ""),
            "role": infer_connection_role(name, name),
        }

    if not configured:
        configured["default"] = {
            "name": "default",
            "label": "Default",
            "url": "redis://127.0.0.1:6379/0",
            "key_prefix": "",
            "role": "generic",
        }

    return configured


def get_connection_choices():
    return list(get_configured_connections().values())


def get_connection_config(connection_name=None):
    connections = get_configured_connections()
    if connection_name and connection_name in connections:
        return connections[connection_name]
    return next(iter(connections.values()))


def get_redis_client(connection_name=None):
    connection = get_connection_config(connection_name)
    return redis.Redis.from_url(connection["url"])


def is_internal_key(key):
    return key.startswith(SNAPSHOT_KEY_PREFIX)


def normalize_pattern(query):
    query = (query or "").strip()
    if not query:
        return "*"

    if "*" in query or "?" in query or "[" in query:
        return query

    return f"*{query}*"


def get_key_type(client, key):
    key_type = client.type(key)
    return safe_decode(key_type) or "unknown"


def get_ttl(client, key):
    return client.ttl(key)


def get_string_value(client, key):
    value = client.get(key)
    return safe_decode(value)


def serialize_value_for_editor(value):
    if isinstance(value, str):
        return value, "text"
    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        return json.dumps(value, indent=2, sort_keys=True), "json"
    return repr(value), "repr"


def deserialize_editor_value(value, serializer):
    if serializer == "pickle_json":
        return pickle.dumps(json.loads(value), protocol=pickle.HIGHEST_PROTOCOL)
    if serializer == "pickle_text":
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    if serializer == "json":
        return json.loads(value)
    return value


def inspect_string_value(client, key):
    raw_value = client.get(key)
    decoded_value = safe_decode(raw_value)
    if decoded_value is not None and (decoded_value != "" or raw_value in {b"", ""}):
        serialized, serializer = serialize_value_for_editor(decoded_value)
        return {
            "preview": build_preview(decoded_value),
            "value": serialized,
            "value_display": decoded_value,
            "is_editable": True,
            "serializer": serializer,
            "value_format": "plain text",
        }

    try:
        unpacked = pickle.loads(raw_value)
    except (pickle.PickleError, TypeError, ValueError, EOFError, AttributeError):
        return {
            "preview": "[binary / non-text value]",
            "value": None,
            "value_display": None,
            "is_editable": False,
            "serializer": "binary",
            "value_format": "binary",
        }

    serialized, serializer = serialize_value_for_editor(unpacked)
    return {
        "preview": build_preview(serialized),
        "value": serialized,
        "value_display": serialized,
        "is_editable": serializer in {"text", "json"},
        "serializer": f"pickle_{serializer}" if serializer in {"text", "json"} else "binary",
        "value_format": "django pickled value",
    }


def get_memory_usage(client, key):
    try:
        return safe_int(client.memory_usage(key), default=0)
    except redis.RedisError:
        return 0


def get_object_metric(client, key, metric_name):
    try:
        return safe_int(client.object(metric_name, key), default=None)
    except (redis.RedisError, TypeError, ValueError):
        return None


def get_preview_for_key(client, key, key_type):
    if key_type != "string":
        return f"[{key_type}]"

    return inspect_string_value(client, key)["preview"]


def scan_all_matching_keys(connection_name=None, pattern="*", limit=None):
    client = get_redis_client(connection_name)
    cursor = 0
    keys = []

    while True:
        cursor, batch = client.scan(cursor=cursor, match=pattern, count=DEFAULT_SCAN_COUNT)
        for raw_key in batch:
            key = safe_decode(raw_key)
            if key is None or is_internal_key(key):
                continue
            keys.append(key)

            if limit is not None and len(keys) >= limit:
                keys.sort()
                return keys

        if cursor == 0:
            break

    keys.sort()
    return keys


def infer_prefix_group(key, configured_prefix=""):
    cleaned = key.lstrip(":")
    parts = [part for part in cleaned.split(":") if part]
    if not parts:
        return "unscoped"

    if configured_prefix and configured_prefix in parts:
        return configured_prefix

    if parts[0].isdigit() and len(parts) > 1:
        return parts[1]

    return parts[0]


def build_key_record(client, key, configured_prefix=""):
    key_type = get_key_type(client, key)
    ttl = get_ttl(client, key)
    value_meta = inspect_string_value(client, key) if key_type == "string" else {
        "value": None,
        "value_display": None,
        "is_editable": False,
        "serializer": "binary",
        "value_format": key_type,
        "preview": f"[{key_type}]",
    }
    memory_bytes = get_memory_usage(client, key)
    frequency = get_object_metric(client, key, "freq")
    idle_seconds = get_object_metric(client, key, "idletime")

    return {
        "key": key,
        "type": key_type,
        "ttl": ttl,
        "ttl_label": format_ttl(ttl),
        "preview": value_meta["preview"],
        "value": value_meta["value"],
        "value_display": value_meta["value_display"],
        "is_editable": value_meta["is_editable"],
        "serializer": value_meta["serializer"],
        "value_format": value_meta["value_format"],
        "memory_bytes": memory_bytes,
        "memory_human": format_bytes(memory_bytes),
        "frequency": frequency,
        "idle_seconds": idle_seconds,
        "idle_label": format_duration(idle_seconds),
        "prefix_group": infer_prefix_group(key, configured_prefix=configured_prefix),
    }


def get_key_page(connection_name=None, query="", page=1, page_size=DEFAULT_PAGE_SIZE):
    connection = get_connection_config(connection_name)
    pattern = normalize_pattern(query)
    all_keys = scan_all_matching_keys(connection["name"], pattern=pattern)

    total_count = len(all_keys)
    total_pages = max(1, ceil(total_count / page_size))

    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size

    client = get_redis_client(connection["name"])
    items = [
        build_key_record(client, key, configured_prefix=connection["key_prefix"])
        for key in all_keys[start:end]
    ]

    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "query": query,
        "pattern": pattern,
        "total_count": total_count,
        "total_pages": total_pages,
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": page - 1,
        "next_page": page + 1,
        "connection": connection,
    }


def delete_key(key, connection_name=None):
    client = get_redis_client(connection_name)
    return client.delete(key)


def delete_keys_by_pattern(query, connection_name=None, max_delete=500):
    connection = get_connection_config(connection_name)
    pattern = normalize_pattern(query)
    keys = scan_all_matching_keys(connection["name"], pattern=pattern, limit=max_delete)
    if not keys:
        return 0

    client = get_redis_client(connection["name"])
    return client.delete(*keys)


def clear_connection(connection_name=None, max_delete=2000):
    connection = get_connection_config(connection_name)
    keys = scan_all_matching_keys(connection["name"], pattern="*", limit=max_delete)
    if not keys:
        return 0
    client = get_redis_client(connection["name"])
    return client.delete(*keys)


def get_key_details(key, connection_name=None):
    connection = get_connection_config(connection_name)
    client = get_redis_client(connection["name"])

    if not client.exists(key):
        return None

    return build_key_record(client, key, configured_prefix=connection["key_prefix"])


def update_string_key(key, value, connection_name=None, serializer="text", keep_ttl=True):
    client = get_redis_client(connection_name)
    ttl = client.ttl(key) if keep_ttl else None
    payload = deserialize_editor_value(value, serializer)

    if ttl is not None and ttl > 0:
        client.setex(key, ttl, payload)
    else:
        client.set(key, payload)


def parse_client_info(raw_value):
    if isinstance(raw_value, dict):
        return raw_value
    parsed = {}
    for line in safe_decode(raw_value).splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key] = value
    return parsed


def collect_health_status(info):
    checks = []
    connected_clients = safe_int(info.get("connected_clients"))
    maxclients = safe_int(info.get("maxclients"))
    rejected_connections = safe_int(info.get("rejected_connections"))
    mem_fragmentation = float(info.get("mem_fragmentation_ratio") or 0)
    blocked_clients = safe_int(info.get("blocked_clients"))

    checks.append({
        "label": "Ping",
        "state": "healthy",
        "detail": "Redis responded to PING.",
    })
    checks.append({
        "label": "Connections",
        "state": "warning" if maxclients and connected_clients / maxclients > 0.8 else "healthy",
        "detail": f"{connected_clients} / {maxclients or 'unlimited'} clients in use.",
    })
    checks.append({
        "label": "Rejected Connections",
        "state": "warning" if rejected_connections else "healthy",
        "detail": f"{rejected_connections} rejected connection attempts.",
    })
    checks.append({
        "label": "Blocked Clients",
        "state": "warning" if blocked_clients else "healthy",
        "detail": f"{blocked_clients} blocked clients.",
    })
    checks.append({
        "label": "Memory Fragmentation",
        "state": "warning" if mem_fragmentation >= 1.5 else "healthy",
        "detail": f"Fragmentation ratio {mem_fragmentation:.2f}.",
    })

    state_order = {"healthy": 0, "warning": 1, "critical": 2}
    overall_state = max(checks, key=lambda item: state_order[item["state"]])["state"]
    return overall_state, checks


def get_sampled_key_records(connection_name, client, configured_prefix="", sample_limit=DEFAULT_SAMPLE_LIMIT):
    sampled_keys = scan_all_matching_keys(connection_name, pattern="*", limit=sample_limit)
    return [build_key_record(client, key, configured_prefix=configured_prefix) for key in sampled_keys]


def summarize_prefixes(records, configured_prefix=""):
    prefixes = Counter(record["prefix_group"] for record in records)
    ttl_values = [record["ttl"] for record in records if record["ttl"] and record["ttl"] > 0]
    configured_present = any(record["prefix_group"] == configured_prefix for record in records) if configured_prefix else False

    return {
        "configured_key_prefix": configured_prefix or "Not set",
        "detected_key_prefix": configured_prefix if configured_present else (prefixes.most_common(1)[0][0] if prefixes else "None detected"),
        "entry_count": len(records),
        "average_ttl_label": format_duration(sum(ttl_values) / len(ttl_values)) if ttl_values else "No expiring keys in sample",
        "most_frequent_prefixes": [
            {"name": name, "count": count, "share": percent(count, len(records))}
            for name, count in prefixes.most_common(6)
        ],
    }


def summarize_requested_keys(records):
    ranked = sorted(
        records,
        key=lambda record: (
            -(record["frequency"] if record["frequency"] is not None else -1),
            record["idle_seconds"] if record["idle_seconds"] is not None else 10**9,
        ),
    )

    return [
        {
            "key": record["key"],
            "score": record["frequency"] if record["frequency"] is not None else "idle-based",
            "detail": (
                f"LFU frequency {record['frequency']}"
                if record["frequency"] is not None
                else f"Idle only {record['idle_label']}"
            ),
        }
        for record in ranked[:5]
    ]


def summarize_useless_keys(records):
    ranked = sorted(
        records,
        key=lambda record: (
            record["ttl"] != -1,
            -(record["idle_seconds"] if record["idle_seconds"] is not None else 0),
            record["frequency"] if record["frequency"] is not None else 10**9,
        ),
    )
    return [
        {
            "key": record["key"],
            "detail": f"Idle {record['idle_label']} • TTL {record['ttl_label']}",
        }
        for record in ranked[:5]
    ]


def summarize_size_analysis(records):
    largest = sorted(records, key=lambda record: record["memory_bytes"], reverse=True)[:5]
    total_memory = sum(record["memory_bytes"] for record in records)
    return {
        "sampled_memory_human": format_bytes(total_memory),
        "largest_keys": [
            {
                "key": record["key"],
                "memory_human": record["memory_human"],
                "type": record["type"],
            }
            for record in largest
        ],
    }


def get_slowlog_entries(client, limit=DEFAULT_SLOWLOG_LIMIT):
    try:
        entries = client.slowlog_get(limit)
    except redis.RedisError:
        return []

    output = []
    for entry in entries:
        arguments = [maybe_decode(arg) for arg in entry.get("command", [])]
        if len(arguments) == 1 and " " in arguments[0]:
            command_text = arguments[0]
        else:
            command_text = " ".join(arguments)
        output.append({
            "id": entry.get("id"),
            "started_at": datetime.utcfromtimestamp(entry.get("start_time")).strftime("%Y-%m-%d %H:%M:%S"),
            "duration_ms": round((entry.get("duration") or 0) / 1000, 2),
            "command": build_preview(command_text, max_length=120),
        })
    return output


def record_snapshot(connection_name, info):
    client = get_redis_client(connection_name)
    snapshot_key = f"{SNAPSHOT_KEY_PREFIX}{connection_name}"
    payload = json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "memory": safe_int(info.get("used_memory")),
        "ops": safe_int(info.get("instantaneous_ops_per_sec")),
        "hits": safe_int(info.get("keyspace_hits")),
        "misses": safe_int(info.get("keyspace_misses")),
        "evicted": safe_int(info.get("evicted_keys")),
        "expired": safe_int(info.get("expired_keys")),
    })
    client.lpush(snapshot_key, payload)
    client.ltrim(snapshot_key, 0, DEFAULT_TIMESERIES_POINTS - 1)


def read_snapshots(connection_name):
    client = get_redis_client(connection_name)
    snapshot_key = f"{SNAPSHOT_KEY_PREFIX}{connection_name}"
    snapshots = []
    for raw_item in reversed(client.lrange(snapshot_key, 0, DEFAULT_TIMESERIES_POINTS - 1)):
        try:
            snapshots.append(json.loads(safe_decode(raw_item)))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    return snapshots


def build_timeseries(snapshots):
    if not snapshots:
        return []

    series = []
    previous = None
    for snapshot in snapshots:
        point = {
            "label": snapshot["timestamp"][11:16],
            "memory": snapshot.get("memory", 0),
            "ops": snapshot.get("ops", 0),
            "hit_rate": 0.0,
        }
        if previous:
            hit_delta = snapshot.get("hits", 0) - previous.get("hits", 0)
            miss_delta = snapshot.get("misses", 0) - previous.get("misses", 0)
            point["hit_rate"] = percent(hit_delta, hit_delta + miss_delta)
        series.append(point)
        previous = snapshot
    return series


def metric_lookup_map(metrics):
    return {item["label"]: item["value"] for item in metrics}


def build_live_chart_series(points):
    memory_values = [item["memory"] for item in points]
    ops_values = [item["ops"] for item in points]
    hit_rate_values = [item["hit_rate"] for item in points]
    return [
        {
            "key": "memory",
            "label": "Memory",
            "value": format_bytes(memory_values[-1]) if memory_values else "0B",
            "points": chart_points(memory_values),
            "raw_value": memory_values[-1] if memory_values else 0,
        },
        {
            "key": "ops",
            "label": "Ops / Sec",
            "value": format_number(ops_values[-1]) if ops_values else "0",
            "points": chart_points(ops_values),
            "raw_value": ops_values[-1] if ops_values else 0,
        },
        {
            "key": "hit_rate",
            "label": "Hit Rate",
            "value": f"{hit_rate_values[-1]}%" if hit_rate_values else "0%",
            "points": chart_points(hit_rate_values),
            "raw_value": hit_rate_values[-1] if hit_rate_values else 0,
        },
    ]


def build_live_metric_payload(connection_name=None):
    connection = get_connection_config(connection_name)
    client = get_redis_client(connection["name"])
    info = client.info()
    client.ping()
    record_snapshot(connection["name"], info)

    hits = safe_int(info.get("keyspace_hits"))
    misses = safe_int(info.get("keyspace_misses"))
    total_lookups = hits + misses
    health_state, _checks = collect_health_status(info)
    snapshots = read_snapshots(connection["name"])
    timeseries = build_timeseries(snapshots)[-DEFAULT_LIVE_WINDOW:]

    return {
        "connection": connection["name"],
        "timestamp": datetime.utcnow().strftime("%H:%M:%S"),
        "metrics": {
            "Used Memory": info.get("used_memory_human", format_bytes(info.get("used_memory"))),
            "Ops / Sec": format_number(info.get("instantaneous_ops_per_sec")),
            "Hit Rate": f"{percent(hits, total_lookups)}%",
            "Connected Clients": format_number(info.get("connected_clients")),
        },
        "health_state": health_state,
        "series": build_live_chart_series(timeseries),
    }


def scan_matching_key_types(client, patterns, sample_limit=150):
    seen = set()
    items = []
    for pattern in patterns:
        cursor = 0
        while True:
            cursor, batch = client.scan(cursor=cursor, match=pattern, count=DEFAULT_SCAN_COUNT)
            for raw_key in batch:
                key = safe_decode(raw_key)
                if key is None or key in seen or is_internal_key(key):
                    continue
                seen.add(key)
                items.append((key, get_key_type(client, key)))
                if len(items) >= sample_limit:
                    return items
            if cursor == 0:
                break
    return items


def summarize_celery(client, connection):
    if connection.get("role") != "celery":
        return None

    key_types = scan_matching_key_types(
        client,
        patterns=["celery*", "*kombu*", "*unacked*", "*pidbox*", "*reply*.pidbox*", "*schedule*", "*eta*"],
        sample_limit=200,
    )

    queue_sizes = []
    scheduled_tasks = 0
    reserved_tasks = 0
    result_keys = 0

    for key, key_type in key_types:
        lowered = key.lower()
        if key_type == "list" and ".pidbox" not in lowered and ".reply" not in lowered:
            queue_sizes.append({
                "name": key,
                "size": safe_int(client.llen(key)),
            })
        elif key_type == "zset" and any(token in lowered for token in ("schedule", "eta", "delayed", "unacked_index")):
            scheduled_tasks += safe_int(client.zcard(key))
        elif any(token in lowered for token in ("unacked", "reserved")):
            if key_type == "hash":
                reserved_tasks += safe_int(client.hlen(key))
            elif key_type == "list":
                reserved_tasks += safe_int(client.llen(key))
            elif key_type == "set":
                reserved_tasks += safe_int(client.scard(key))
            elif key_type == "zset":
                reserved_tasks += safe_int(client.zcard(key))
        elif lowered.startswith("celery-task-meta-"):
            result_keys += 1

    queue_sizes = sorted(queue_sizes, key=lambda item: item["size"], reverse=True)

    return {
        "enabled": True,
        "queue_count": len(queue_sizes),
        "total_queue_depth": sum(item["size"] for item in queue_sizes),
        "queues": queue_sizes[:8],
        "scheduled_tasks": scheduled_tasks,
        "reserved_tasks": reserved_tasks,
        "result_keys": result_keys,
    }


def summarize_clients(client):
    try:
        clients = [parse_client_info(item) for item in client.client_list()]
    except redis.RedisError:
        return {
            "connected_clients": [],
            "blocked_clients": 0,
        }

    connected = [
        {
            "addr": item.get("addr", "unknown"),
            "name": item.get("name") or "anonymous",
            "age": format_duration(item.get("age")),
            "idle": format_duration(item.get("idle")),
            "db": item.get("db", "0"),
        }
        for item in clients[:5]
    ]
    return {
        "connected_clients": connected,
        "blocked_clients": sum(1 for item in clients if item.get("cmd") == "BLPOP"),
    }


def get_redis_dashboard(connection_name=None):
    connection = get_connection_config(connection_name)
    client = get_redis_client(connection["name"])
    info = client.info()
    client.ping()
    record_snapshot(connection["name"], info)

    hits = safe_int(info.get("keyspace_hits"))
    misses = safe_int(info.get("keyspace_misses"))
    total_lookups = hits + misses
    total_keys = 0
    for key, value in info.items():
        if key.startswith("db") and isinstance(value, dict):
            total_keys += value.get("keys", 0)

    overall_state, health_checks = collect_health_status(info)
    sampled_records = get_sampled_key_records(
        connection["name"],
        client,
        configured_prefix=connection["key_prefix"],
        sample_limit=min(total_keys or DEFAULT_SAMPLE_LIMIT, DEFAULT_SAMPLE_LIMIT),
    )
    prefix_summary = summarize_prefixes(sampled_records, configured_prefix=connection["key_prefix"])
    size_summary = summarize_size_analysis(sampled_records)
    snapshots = read_snapshots(connection["name"])
    timeseries = build_timeseries(snapshots)
    client_summary = summarize_clients(client)
    live_series = build_live_chart_series(timeseries[-DEFAULT_LIVE_WINDOW:])
    celery_summary = summarize_celery(client, connection)

    metrics = [
        {"label": "Version", "value": info.get("redis_version", "unknown")},
        {"label": "Uptime", "value": format_duration(info.get("uptime_in_seconds"))},
        {"label": "Used Memory", "value": info.get("used_memory_human", format_bytes(info.get("used_memory")))},
        {"label": "Peak Memory", "value": info.get("used_memory_peak_human", format_bytes(info.get("used_memory_peak")))},
        {"label": "Ops / Sec", "value": format_number(info.get("instantaneous_ops_per_sec"))},
        {"label": "Hit Rate", "value": f"{percent(hits, total_lookups)}%"},
        {"label": "Total Keys", "value": format_number(total_keys)},
        {"label": "Expired Keys", "value": format_number(info.get("expired_keys"))},
        {"label": "Evicted Keys", "value": format_number(info.get("evicted_keys"))},
        {"label": "Connected Clients", "value": format_number(info.get("connected_clients"))},
    ]

    return {
        "connection": connection,
        "health": {
            "state": overall_state,
            "checks": health_checks,
        },
        "metrics": metrics,
        "metric_map": metric_lookup_map(metrics),
        "prefix_summary": prefix_summary,
        "key_insights": {
            "most_requested": summarize_requested_keys(sampled_records),
            "useless_keys": summarize_useless_keys(sampled_records),
            "size_analysis": size_summary,
        },
        "timeseries": live_series,
        "live_chart_seed": timeseries[-DEFAULT_LIVE_WINDOW:],
        "slowlog": get_slowlog_entries(client),
        "eviction": {
            "expired_keys": format_number(info.get("expired_keys")),
            "evicted_keys": format_number(info.get("evicted_keys")),
            "maxmemory_policy": info.get("maxmemory_policy", "noeviction"),
            "active_defrag": info.get("active_defrag_running", 0),
        },
        "clients": client_summary,
        "celery": celery_summary,
        "sample_size": len(sampled_records),
    }
