from enum import Enum

STATUS_NEW = "New"
STATUS_GROUP = "Grouped"
STATUS_ERR = "Error"
STATUS_PROCESSING = "Processing"
STATUS_ANALYZED = "Analyzed"
STATUS_ANALYZING = "Analyzing"
STATUS_ANALYZE_ERROR = "AnalyzeError"
STATUS_FINISHED = "Finished"
WAITING_FOR_CREATE_SO = "WaitingForCreateSO"
WAITING_FOR_CREATE_STO = "WaitingForCreateSTO"
STATUS_COMPLETED = "Completed"
STATUS_CREATE_STO = "CreatedSTO"
STATUS_CANCEL = "CANCELED"
SO_STATUS_WAITING_CREATE_DO = "WaitingForCreateDO";
class BubbleRule(Enum):
    MAX_SKU = 1
    MIN_SKU = 2
    PACKAGE_TYPE = 3
    MAX_UNIT = 4
    MIN_UNIT = 5
