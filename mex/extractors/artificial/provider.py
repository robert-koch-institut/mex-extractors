import re
from datetime import datetime
from enum import Enum
from functools import partial
from typing import Any, cast, get_args, get_origin

from annotated_types import MaxLen, MinLen
from faker.providers import BaseProvider as BaseFakerProvider
from faker.providers.internet import Provider as InternetFakerProvider
from faker.providers.python import Provider as PythonFakerProvider
from pydantic.fields import FieldInfo

from mex.common.identity import Identity
from mex.common.models import ExtractedData
from mex.common.types import (
    TEMPORAL_ENTITY_FORMATS_BY_PRECISION,
    UTC,
    Email,
    Identifier,
    Link,
    LinkLanguage,
    TemporalEntity,
    TemporalEntityPrecision,
    Text,
)
from mex.extractors.artificial.identity import IdentityMap
from mex.extractors.settings import Settings


class BuilderProvider(PythonFakerProvider):
    """Faker provider that deals with interpreting pydantic model fields."""

    def min_max_for_field(self, field: FieldInfo) -> tuple[int, int]:
        """Return a min and max item count for a field."""
        if get_origin(field.annotation) is list:
            # calculate the item counts based on field annotations
            min_items = 0
            max_items = self.pyint(1, 3)
            if min_lengths := [x for x in field.metadata if isinstance(x, MinLen)]:
                min_items = min_lengths[0].min_length
                max_items += min_items  # the max should be higher than the min
            if max_lengths := [x for x in field.metadata if isinstance(x, MaxLen)]:
                max_items = max_lengths[0].max_length
        else:
            # required fields need at least 1 item, optional fields 0
            min_items = int(field.is_required())
            # a list with length 1 will be unpacked by `fix_listyness`
            max_items = 1
        return min_items, max_items

    def inner_type_and_pattern(self, field: FieldInfo) -> tuple[Any, str | None]:
        """Return the inner type and pattern of a field.

        If the Field arguments, randomly pick an argument.
        """
        # determine field type and unpack unions, lists, and other types with args
        if args := get_args(field.annotation):
            # mixed types are not yet supported
            return self.random_element(
                [
                    self.inner_type_and_pattern(field.from_annotation(type_))
                    for type_ in args
                    if type_ is not type(None)
                ]
            )
        pattern = None
        if pattern_metadata_entries := [
            m for m in field.metadata if hasattr(m, "pattern")
        ]:
            pattern = pattern_metadata_entries[0].pattern
        return field.annotation, pattern

    def field_value(
        self,
        field: FieldInfo,
        identity: Identity,
    ) -> list[Any]:
        """Get a single artificial value for the given field and identity."""
        inner_type, pattern = self.inner_type_and_pattern(field)
        if pattern:
            factory = partial(self.generator.pattern, pattern)
        elif issubclass(inner_type, Identifier):
            factory = partial(self.generator.reference, inner_type, exclude=identity)
        elif issubclass(inner_type, Link):
            factory = self.generator.link
        elif issubclass(inner_type, Email):
            factory = self.generator.email
        elif issubclass(inner_type, Text):
            factory = self.generator.text_object
        elif issubclass(inner_type, TemporalEntity):
            factory = partial(
                self.generator.temporal_entity, inner_type.ALLOWED_PRECISION_LEVELS
            )
        elif issubclass(inner_type, Enum):
            factory = partial(self.random_element, inner_type)
        elif issubclass(inner_type, str):
            factory = self.generator.text_string
        else:
            raise RuntimeError(f"Cannot create fake data for {field}")
        return [factory() for _ in range(self.pyint(*self.min_max_for_field(field)))]

    def extracted_data(self, model: type[ExtractedData]) -> list[ExtractedData]:
        """Get a list of extracted data instances for the given model class."""
        models = []
        for identity in cast(list[Identity], self.generator.identities(model)):
            # manually set identity related fields
            payload: dict[str, Any] = dict(
                identifier=identity.identifier,
                hadPrimarySource=identity.hadPrimarySource,
                identifierInPrimarySource=identity.identifierInPrimarySource,
                stableTargetId=identity.stableTargetId,
                entityType=model.__name__,
            )
            # dynamically populate all other fields
            for name, field in model.model_fields.items():
                if name not in payload:
                    payload[name] = self.field_value(field, identity)
            models.append(model.model_validate(payload))
        return models


class IdentityProvider(BaseFakerProvider):
    """Faker provider that creates identities and helps with referencing them."""

    def __init__(self, factory: Any, identities: IdentityMap) -> None:
        """Create and persist identities for all entity types."""
        super().__init__(factory)
        self._identities = identities

    def identities(self, model: type[ExtractedData]) -> list[Identity]:
        """Return a list of identities for the given model class."""
        return self._identities[model.__name__.removeprefix("Extracted")]

    def reference(
        self,
        inner_type: type[Identifier],
        exclude: Identity,
    ) -> Identifier | None:
        """Return ID for random identity of given type (that is not excluded)."""
        if choices := [
            identity
            for entity_type in re.findall(
                r"Merged([A-Za-z]+)Identifier", str(inner_type)
            )
            for identity in self._identities[entity_type]
            # avoid self-references by skipping excluded ids
            if identity.stableTargetId != exclude.stableTargetId
        ]:
            return Identifier(self.random_element(choices).stableTargetId)
        return None


class LinkProvider(InternetFakerProvider, PythonFakerProvider):
    """Faker provider that can return links with optional title and language."""

    def link(self) -> Link:
        """Return a link with optional title and language."""
        title, language = None, None
        if self.pybool():
            title = self.domain_word().replace("-", " ").title()
            if self.pybool():
                language = self.random_element(LinkLanguage)
        return Link(url=self.url(), title=title, language=language)


class TemporalEntityProvider(PythonFakerProvider):
    """Faker provider that can return a custom TemporalEntity with random precision."""

    def temporal_entity(
        self, allowed_precision_levels: list[TemporalEntityPrecision]
    ) -> TemporalEntity:
        """Return a custom temporal entity with random date, time and precision."""
        return TemporalEntity(
            datetime.fromtimestamp(
                self.pyint(int(8e8), int(datetime.now().timestamp())), tz=UTC
            ).strftime(
                TEMPORAL_ENTITY_FORMATS_BY_PRECISION[
                    self.random_element(allowed_precision_levels)
                ]
            )
        )


class TextProvider(PythonFakerProvider):
    """Faker provider that handles custom text related requirements."""

    def text_string(self) -> str:
        """Return a randomized sequence of words as a string."""
        return " ".join(self.generator.word() for _ in range(self.pyint(1, 3)))

    def text_object(self) -> Text:
        """Return a random text paragraph with an auto-detected language."""
        settings = Settings.get()
        return Text(
            value=self.generator.paragraph(
                self.pyint(1, settings.artificial.chattiness)
            )
        )


class PatternProvider(BaseFakerProvider):
    """Faker provider to create strings matching given patterns."""

    # XXX: Hardcoding is not an ideal but a quick way to get matching random strings
    REGEX_TO_NUMERIFY = {
        r"^https://www\.wikidata\.org/entity/[PQ0-9]{2,64}$": "https://www.wikidata.org/entity/P######",
        r"^https://isni\.org/isni/[X0-9]{16}$": "https://isni.org/isni/################",
        r"^https://viaf\.org/viaf/[0-9]{2,22}$": "https://viaf.org/viaf/#########",
        r"^https://d\-nb\.info/gnd/[-X0-9]{3,10}$": "https://d-nb.info/gnd/3########",
        r"^https://gepris\.dfg\.de/gepris/institution/[0-9]{1,64}$": "https://gepris.dfg.de/gepris/institution/#######",
        r"^https://orcid\.org/[-X0-9]{9,21}$": "https://orcid.org/0000-####-####-###X",
        r"^https://ror\.org/[a-z0-9]{9}$": "https://ror.org/#########",
    }
    MESH_TO_TEMPLATE = {
        r"^http://id\.nlm\.nih\.gov/mesh/[A-Z0-9]{2,64}$": "http://id.nlm.nih.gov/mesh/{}"
    }

    def __init__(self, factory: Any) -> None:
        """Initialize the provider by loading the contents of the `mesh_file`."""
        super().__init__(factory)
        settings = Settings.get()
        with open(str(settings.artificial.mesh_file), mode="br") as fh:
            self._mesh_ids = re.findall(
                r"UI = (D[0-9]+)", fh.read().decode(errors="ignore")
            )

    def pattern(self, regex: str) -> str:
        """Return a randomized string matching the given pattern."""
        if template := self.MESH_TO_TEMPLATE.get(regex):
            return template.format(self.random_element(self._mesh_ids))
        return self.numerify(self.REGEX_TO_NUMERIFY[str(regex)])
