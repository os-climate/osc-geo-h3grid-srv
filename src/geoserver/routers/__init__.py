# Copyright 2024 Broda Group Software Inc.
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.
#
# Created: 2024-03-27 by davis.broda@brodagroupsoftware.com
from .route_constants import API_PREFIX
from .geomesh_router import router as geomesh_router,\
    ENDPOINT_PREFIX as GEO_ENDPOINT_PREFIX
from .point_router import router as point_router,\
    ENDPOINT_PREFIX as POINT_ENDPOINT_PREFIX

