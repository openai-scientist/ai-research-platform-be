import pytest

from platform_be.services.connectors.base import ConnectorError
from platform_be.services.connectors.network_guard import (
    ResolvedHost,
    is_host,
    resolve_public_host,
    system_resolver,
)

PUBLIC = "93.184.216.34"


def resolver_for(table: dict[str, list[str]]):
    asked: list[str] = []

    async def resolve(host: str, port: int) -> list[str]:
        asked.append(host)
        if host not in table:
            raise OSError("name not known")
        return table[host]

    resolve.asked = asked
    return resolve


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.5",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "255.255.255.255",
        "224.0.0.1",
        "ff02::1",
        "::1",
        "fe80::1",
        "fd00::1",
        "::ffff:10.0.0.1",
        "::ffff:127.0.0.1",
        # IPv6 prefixes that embed an IPv4 destination.
        "64:ff9b::a00:1",
        "64:ff9b:1::a00:1",
        "2002:a00:1::",
        "2001:0:a00:1::",
        "::10.0.0.1",
    ],
)
async def test_internal_addresses_are_refused(address: str) -> None:
    resolver = resolver_for({"db.example.com": [address]})
    for host in (address, "db.example.com"):
        with pytest.raises(ConnectorError) as raised:
            await resolve_public_host(host, 5432, resolver=resolver)
        assert raised.value.reason == "host_not_allowed"


@pytest.mark.asyncio
async def test_a_public_address_passes_and_is_pinned() -> None:
    public_v6 = "2606:2800:220:1:248:1893:25c8:1946"
    resolver = resolver_for({"db.example.com": [public_v6, PUBLIC], "v6.example.com": [public_v6]})

    resolved = await resolve_public_host(" db.example.com ", 5432, resolver=resolver)

    assert resolved == ResolvedHost(hostname="db.example.com", ip=PUBLIC, port=5432)
    # A literal address needs no lookup.
    literal = await resolve_public_host(PUBLIC, 6543, resolver=resolver)
    assert literal.ip == PUBLIC
    assert resolver.asked == ["db.example.com"]
    mapped = await resolve_public_host(f"::ffff:{PUBLIC}", 5432, resolver=resolver)
    assert mapped.ip == PUBLIC
    # IPv6 is used when it is all the name has.
    assert (await resolve_public_host("v6.example.com", 5432, resolver=resolver)).ip == public_v6


@pytest.mark.asyncio
async def test_one_internal_address_among_several_refuses_the_host() -> None:
    resolver = resolver_for({"mixed.example.com": [PUBLIC, "10.0.0.5"]})

    with pytest.raises(ConnectorError) as raised:
        await resolve_public_host("mixed.example.com", 5432, resolver=resolver)

    assert raised.value.reason == "host_not_allowed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host",
    [
        "",
        "/var/run/postgresql",
        "db host",
        "db\n.example.com",
        "db\x00",
        "-db.example.com",
        "a" * 300,
    ],
)
async def test_text_that_is_not_a_host_name_is_refused_without_a_lookup(host: str) -> None:
    resolver = resolver_for({})

    with pytest.raises(ConnectorError) as raised:
        await resolve_public_host(host, 5432, allow_private=True, resolver=resolver)

    assert raised.value.reason == "host_not_allowed"
    assert resolver.asked == []


@pytest.mark.asyncio
async def test_unknown_names_are_unreachable_and_private_hosts_need_the_switch() -> None:
    resolver = resolver_for({"localhost": ["127.0.0.1"], "empty.example.com": []})

    for host in ("missing.example.com", "empty.example.com"):
        with pytest.raises(ConnectorError) as raised:
            await resolve_public_host(host, 5432, resolver=resolver)
        assert raised.value.reason == "unreachable"

    local = await resolve_public_host("localhost", 5432, allow_private=True, resolver=resolver)
    assert local.ip == "127.0.0.1"


@pytest.mark.asyncio
async def test_the_system_resolver_answers_from_its_own_threads() -> None:
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-dns") as executor:
        resolve = system_resolver(executor)
        local = await resolve_public_host("localhost", 5432, allow_private=True, resolver=resolve)
        assert local.ip in ("127.0.0.1", "::1")
        with pytest.raises(ConnectorError) as raised:
            await resolve_public_host("localhost", 5432, resolver=resolve)
        assert raised.value.reason == "host_not_allowed"
        with pytest.raises(ConnectorError) as raised:
            await resolve_public_host("no-such-host.invalid", 5432, resolver=resolve)
        assert raised.value.reason == "unreachable"


def test_only_names_and_addresses_have_the_shape_of_a_host() -> None:
    for text in ("db.example.com", "DB-1.example.com", "xn--e28h.example.com", "8.8.8.8", "::1"):
        assert is_host(text), text
    for text in (
        "postgresql://reader:pw@db.example.com:5432/analytics",
        "reader:pw@db.example.com",
        "db.example.com:5432",
        "db.example.com/analytics",
        "db example.com",
        "db.example.com.",
        "[::1]",
        "fe80::1%eth0",
        "",
    ):
        assert not is_host(text), text
