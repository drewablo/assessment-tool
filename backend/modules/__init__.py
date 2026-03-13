from modules.housing import HousingModule
from modules.schools import SchoolsModule
from modules.elder_care import ElderCareModule

MODULE_REGISTRY = {
    "schools": SchoolsModule(),
    "housing": HousingModule(),
    "elder_care": ElderCareModule(),
}
