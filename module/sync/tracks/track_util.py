from urllib import parse


def url_split(url):
    obj = {}
    if url:
        de_url = parse.unquote(url.lower())
        url_sp = parse.urlsplit(de_url)
        obj = {
            'scheme': url_sp.scheme,
            'hostname': url_sp.netloc,
            'path': url_sp.path,
            'params': dict([(k, v[0]) for k, v in parse.parse_qs(url_sp.query).items()])
        }
    return obj
