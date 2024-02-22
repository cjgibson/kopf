from typing import Collection, List, Tuple, Optional

from kopf._cogs.clients import api
from kopf._cogs.configs import configuration
from kopf._cogs.helpers import typedefs
from kopf._cogs.structs import bodies, references


async def list_objs(
        *,
        settings: configuration.OperatorSettings,
        resource: references.Resource,
        namespace: references.Namespace,
        logger: typedefs.Logger,
) -> Tuple[Collection[bodies.RawBody], str]:
    """
    List the objects of specific resource type.

    The cluster-scoped call is used in two cases:

    * The resource itself is cluster-scoped, and namespacing makes not sense.
    * The operator serves all namespaces for the namespaced custom resource.

    Otherwise, the namespace-scoped call is used:

    * The resource is namespace-scoped AND operator is namespaced-restricted.
    """
    items: List[bodies.RawBody] = []
    page_limit = settings.watching.pagination_limit
    continue_token: Optional[str] = None
    resource_version: Optional[str] = None
    while True:
        params = {}
        if page_limit is not None:
            params['limit'] = page_limit
        if continue_token:
            params['continue'] = continue_token
        rsp = await api.get(
            url=resource.get_url(namespace=namespace, params=params),
            logger=logger,
            settings=settings,
        )

        resource_version = rsp.get('metadata', {}).get('resourceVersion', None)
        for item in rsp.get('items', []):
            if 'kind' in rsp:
                item.setdefault('kind', rsp['kind'][:-4] if rsp['kind'][-4:] == 'List' else rsp['kind'])
            if 'apiVersion' in rsp:
                item.setdefault('apiVersion', rsp['apiVersion'])
            items.append(item)

        continue_token = rsp.get('metadata', {}).get('continue', None)
        if not continue_token:
            break
        logger.debug(f'Fetching more {resource.plural} with continue token {continue_token}.')

    return items, resource_version
