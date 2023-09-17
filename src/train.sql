select offer_id, prop, val 
from offers_detail
where prop in ('isApartments', 'isPenthouse', 
			   'totalArea', 'livingArea', 'kitchenArea', 
			   'loggiasCount', 'windowsViewType', 'repairType', 'roomsCount', 'floorNumber', 
			   'priceTotal', 
			   'passengerLiftsCount', 'cargoLiftsCount',
			   'isFromBuilder',
			   'building.materialType', 'building.floorsCount', 'building.buildYear', 'building.parking.type', 'building.hasGarbageChute',
			   'building.ceilingHeight', 'building.series',
			   'newbuilding.house.isFinished', 'newbuilding.newbuildingFeatures.deadlineInfo')
or prop like 'geo.undergrounds.travel%' or prop like 'geo.undergrounds.name%'