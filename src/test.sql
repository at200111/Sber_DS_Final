select od.* 
from offers_detail od
left join offers o on od.offer_id = o.offer_id
where prop in ('id', 'isApartments', 'isPenthouse', 
			   'totalArea', 'livingArea', 'kitchenArea', 
			   'loggiasCount', 'windowsViewType', 'repairType', 'roomsCount', 'floorNumber', 
			   'priceTotal', 
			   'passengerLiftsCount', 'cargoLiftsCount',
			   'isFromBuilder',
			   'building.materialType', 'building.floorsCount', 'building.buildYear', 'building.parking.type', 'building.hasGarbageChute',
			   'building.ceilingHeight', 'building.series',
			   'newbuilding.house.isFinished', 'newbuilding.newbuildingFeatures.deadlineInfo')
or prop like 'geo.undergrounds.travel%' or prop like 'geo.undergrounds.name%'	
order by o.modifydate desc
limit 500