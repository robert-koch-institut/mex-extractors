from mex.common.identity.registry import register_provider
from mex.extractors.identity import BackendIdentityProvider
from mex.extractors.pipeline import load_job_definitions
from mex.extractors.types import ExtractorIdentityProvider

defs = load_job_definitions()
register_provider(ExtractorIdentityProvider.BACKEND, BackendIdentityProvider)
