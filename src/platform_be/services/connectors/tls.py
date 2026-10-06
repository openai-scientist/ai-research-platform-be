import ssl


class _PinnedHostnameContext(ssl.SSLContext):
    """Verifies the certificate against the original hostname while the socket dials an IP.

    The drivers use their `host` argument for both the TCP address and the TLS server name.
    The connection has to go to the IP the network guard checked, so the name is swapped in
    here: `wrap_bio` is what asyncpg calls, `wrap_socket` what PyMySQL calls.
    """

    server_name: str | None = None

    def wrap_bio(self, incoming, outgoing, server_side=False, server_hostname=None, session=None):
        return super().wrap_bio(
            incoming,
            outgoing,
            server_side=server_side,
            server_hostname=self.server_name or server_hostname,
            session=session,
        )

    def wrap_socket(self, sock, *args, server_hostname=None, **kwargs):
        return super().wrap_socket(
            sock, *args, server_hostname=self.server_name or server_hostname, **kwargs
        )


def build_ssl_context(mode: str, hostname: str) -> ssl.SSLContext | bool:
    """`disable` is plain TCP, `require` encrypts without checking the certificate, and
    `verify-full` also checks it against the system CAs and the host name."""
    if mode == "disable":
        return False
    context = _PinnedHostnameContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    # Sent as SNI in both modes: some hosted databases route on it.
    context.server_name = hostname
    if mode == "verify-full":
        context.load_default_certs()
    else:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context
