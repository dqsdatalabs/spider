# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'sicasabari_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Sicasabari_PySpider_italy"
    start_urls = ['https://sicasabari.it/api/AdsSearch/PostMiniFichasAdsMaps']  # LEVEL 1

    payload = {"currentPage":0,"itemsPerPage":0,"order":"desc","orderfield":"creationDate","ids":[],"UserContactId":None,"showAddress":0,"adOperationId":2,"adScopeId":None,"adTypologyId":"0","priceMin":None,"priceMax":None,"CreationDateMin":None,"CreationDateMax":None,"locationId":[{"id":"0-EU-IT-BA","label":"Bari","zoneLevelId":4,"latitude":None,"longitude":None,"haschildren":False}],"drawShapePath":None,"homes":None,"chalets":None,"countryhouses":None,"isDuplex":None,"isPenthouse":None,"isStudio":None,"isIndependentHouse":None,"isSemidetachedHouse":None,"isTerracedHouse":None,"constructedAreaMin":None,"constructedAreaMax":None,"rooms_0":None,"rooms_1":None,"rooms_2":None,"rooms_3":None,"rooms_4":None,"baths_1":None,"baths_2":None,"baths_3":None,"builtTypeId":None,"isTopFloor":None,"isIntermediateFloor":None,"isGroundFloor":None,"isFirstFloor":None,"hasAirConditioning":None,"hasWardrobe":None,"hasGarage":None,"hasLift":None,"hasTerrace":None,"hasBoxRoom":None,"hasSwimmingPool":None,"hasGarden":None,"flatLocationId":None,"hasKitchen":None,"hasAutomaticDoor":None,"hasPersonalSecurity":None,"HasSecurity24h":None,"garageCapacityId":None,"hasHotWater":None,"hasExterior":None,"hasSuspendedFloor":None,"hasHeating":None,"isFurnish":None,"isBankOwned":None,"distributionId":None,"isOnlyOfficeBuilding":None,"ubicationId":None,"warehouseType_1":None,"warehouseType_2":None,"isATransfer":None,"isCornerLocated":None,"hasSmokeExtractor":None,"landType_1":None,"landType_2":None,"landType_3":None,"HasAllDayAccess":None,"HasLoadingDockAccess":None,"HasTenant":None,"addressVisible":None,"mlsIncluded":None,"freeText":None,"RefereceText":None,"isLowered":None,"priceDropDateFrom":0,"priceDropDateTo":0,"arePetsAllowed":None,"Equipment":None,"OperationStatus":None,"AdContract":None,"IsRent":False,"IsSale":True,"IsAuction":False,"AdState":None,"cmbprovincia":"0-EU-IT-BA","cmblocalidad":"0","cmbzona":"0"}
    
    headers = {
        'Accept': '*/*',
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Referer': 'https://sicasabari.it/listadoads.aspx',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
    }
    
    def start_requests(self):
        yield Request(
            url=self.start_urls[0], 
            body=json.dumps(self.payload),
            method="POST",
            headers=self.headers,
            callback=self.parse
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data["ads"]:
            follow_url = f"https://sicasabari.it/ad/{item['id']}"
            yield Request(follow_url, callback=self.populate_item, meta={"data":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1/text()").get()
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        data = response.meta.get('data')
        item_loader.add_value("title", title)
        
        address = data["property"]["address"]
        if address:
            item_loader.add_value("zipcode", address["postalCode"])
            item_loader.add_value("address", address["AddressName"])
            item_loader.add_value("city", address["AddressName"].split(",")[-1].strip())
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split(" ")[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//span[@class='property-ref']/text()").get()
        if external_id:
            external_id = external_id.split(".")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        room_count = response.xpath("//li[span[contains(@class,'bed')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//li[span[contains(@class,'bath')]]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//li[span[contains(@class,'plan')]]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        description = "".join(response.xpath("//p[@class='contitle']/@title").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        elevator = response.xpath("//li[contains(.,'ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        energy_label = response.xpath("//li[contains(.,'energetica ')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[0].strip().split(" ")[-1])
            
        latitude_longitude = response.xpath("//input[@name='defaultLatLng']/@value").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(",")[0].split(":")[1].strip()
            longitude = latitude_longitude.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        terrace = data["property"]["housing"]["hasTerrace"]
        if terrace:
            item_loader.add_value("terrace", True)
        
        swimming_pool = data["property"]["housing"]["hasSwimmingPool"]
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        elevator = data["property"]["housing"]["hasLift"]
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = data["property"]["housing"]["parkingSpace"]
        if parking and "true" in str(parking).lower():
            item_loader.add_value("parking", True)
        
        images = response.xpath("//script[contains(.,'multimediaId')]/text()").get()
        if images:
            images = "["+images.split("'[")[1].split("]'")[0].strip()+"]"
            img = json.loads(images)
            for i in img:
                item_loader.add_value("images", i["src"])
        
        item_loader.add_value("landlord_name", "Si Casa di Scippa Salvatore")
        item_loader.add_value("landlord_phone", "+39 0802377279")
        item_loader.add_value("landlord_email", "info@sicasabari.it")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("trilocale" in p_type_string.lower() or "casa" in p_type_string.lower() or "villetta" in p_type_string.lower() or "villino" in p_type_string.lower() or "villa" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "monolocale" in p_type_string.lower():
        return "studio"
    else:
        return None