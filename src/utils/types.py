from typing import Any, NewType

from pipeline_import.visitors import (
    CastlingVisitor,
    ClocksVisitor,
    EvalsVisitor,
    MaterialVisitor,
    PositionsVisitor,
    PromotionsVisitor,
    QueenExchangeVisitor,
)

Json = NewType('Json', dict[str, Any])
Visitor = (CastlingVisitor |
           ClocksVisitor |
           EvalsVisitor |
           MaterialVisitor |
           PositionsVisitor |
           PromotionsVisitor |
           QueenExchangeVisitor
           )
