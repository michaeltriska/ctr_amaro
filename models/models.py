from schematics.models import Model
from schematics.types import StringType


class AggregationRequest(Model):
    startTimestamp = StringType(required=True)
    endTimestamp = StringType(required=True)
    aggregation = StringType(required=True)
    product = StringType()
    platform = StringType()
