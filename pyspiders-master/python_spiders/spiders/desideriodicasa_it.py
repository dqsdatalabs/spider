# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'desideriodicasa_it'
    external_source = "Desideriodicasa_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it' 
    post_url = ['https://www.desideriodicasa.it/api/AdsSearch/PostSearch']  # LEVEL 1

    payload = {"currentPage":1,"itemsPerPage":20,"order":"desc","orderfield":"creationDate","ids":[],"UserContactId":None,"showAddress":1,"adOperationId":"2","adScopeId":None,"adTypologyId":"0","priceMin":None,"priceMax":None,"CreationDateMin":None,"CreationDateMax":None,"locationId":[],"drawShapePath":None,"homes":None,"chalets":None,"countryhouses":None,"isDuplex":None,"isPenthouse":None,"isStudio":None,"isIndependentHouse":None,"isSemidetachedHouse":None,"isTerracedHouse":None,"constructedAreaMin":None,"constructedAreaMax":None,"rooms_0":None,"rooms_1":None,"rooms_2":None,"rooms_3":None,"rooms_4":None,"baths_1":None,"baths_2":None,"baths_3":None,"builtTypeId":None,"isTopFloor":None,"isIntermediateFloor":None,"isGroundFloor":None,"isFirstFloor":None,"hasAirConditioning":None,"hasWardrobe":None,"hasGarage":None,"hasLift":None,"hasTerrace":None,"hasBoxRoom":None,"hasSwimmingPool":None,"hasGarden":None,"flatLocationId":None,"hasKitchen":None,"hasAutomaticDoor":None,"hasPersonalSecurity":None,"HasSecurity24h":None,"garageCapacityId":None,"hasHotWater":None,"hasExterior":None,"hasSuspendedFloor":None,"hasHeating":None,"isFurnish":None,"isBankOwned":None,"distributionId":None,"isOnlyOfficeBuilding":None,"ubicationId":None,"warehouseType_1":None,"warehouseType_2":None,"isATransfer":None,"isCornerLocated":None,"hasSmokeExtractor":None,"landType_1":None,"landType_2":None,"landType_3":None,"HasAllDayAccess":None,"HasLoadingDockAccess":None,"HasTenant":None,"addressVisible":None,"mlsIncluded":None,"freeText":None,"RefereceText":None,"isLowered":None,"priceDropDateFrom":0,"priceDropDateTo":0,"arePetsAllowed":None,"Equipment":None,"OperationStatus":None,"AdContract":None,"IsRent":True,"IsSale":False,"IsAuction":False,"AdState":None}
    headers = {
        'Connection': 'keep-alive',
        'Pragma': 'no-cache', 
        'Cache-Control': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", "Chromium";v="93"',
        'Accept': '*/*',
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'sec-ch-ua-platform': '"Windows"',
        'Origin': 'https://www.desideriodicasa.it',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://www.desideriodicasa.it/ads/affitto/immobile/',
        'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
        'Cookie': 'isAdsOffice=; isMap=; URLListadoFriendly=https%3A%2F%2Fwww.desideriodicasa.it%2Fads%2Faffitto%2Fimmobile%2F; search_buscador_offices=null; _ac=true; _technical=true; _performance=false; search_buscador_pro=%7B%22currentPage%22%3A2%2C%22itemsPerPage%22%3A20%2C%22order%22%3A%22desc%22%2C%22orderfield%22%3A%22creationDate%22%2C%22ids%22%3A%5B%5D%2C%22UserContactId%22%3Anull%2C%22showAddress%22%3A1%2C%22adOperationId%22%3A%222%22%2C%22adScopeId%22%3Anull%2C%22adTypologyId%22%3A%220%22%2C%22priceMin%22%3Anull%2C%22priceMax%22%3Anull%2C%22CreationDateMin%22%3Anull%2C%22CreationDateMax%22%3Anull%2C%22locationId%22%3A%5B%5D%2C%22drawShapePath%22%3Anull%2C%22homes%22%3Anull%2C%22chalets%22%3Anull%2C%22countryhouses%22%3Anull%2C%22isDuplex%22%3Anull%2C%22isPenthouse%22%3Anull%2C%22isStudio%22%3Anull%2C%22isIndependentHouse%22%3Anull%2C%22isSemidetachedHouse%22%3Anull%2C%22isTerracedHouse%22%3Anull%2C%22constructedAreaMin%22%3Anull%2C%22constructedAreaMax%22%3Anull%2C%22rooms_0%22%3Anull%2C%22rooms_1%22%3Anull%2C%22rooms_2%22%3Anull%2C%22rooms_3%22%3Anull%2C%22rooms_4%22%3Anull%2C%22baths_1%22%3Anull%2C%22baths_2%22%3Anull%2C%22baths_3%22%3Anull%2C%22builtTypeId%22%3Anull%2C%22isTopFloor%22%3Anull%2C%22isIntermediateFloor%22%3Anull%2C%22isGroundFloor%22%3Anull%2C%22isFirstFloor%22%3Anull%2C%22hasAirConditioning%22%3Anull%2C%22hasWardrobe%22%3Anull%2C%22hasGarage%22%3Anull%2C%22hasLift%22%3Anull%2C%22hasTerrace%22%3Anull%2C%22hasBoxRoom%22%3Anull%2C%22hasSwimmingPool%22%3Anull%2C%22hasGarden%22%3Anull%2C%22flatLocationId%22%3Anull%2C%22hasKitchen%22%3Anull%2C%22hasAutomaticDoor%22%3Anull%2C%22hasPersonalSecurity%22%3Anull%2C%22HasSecurity24h%22%3Anull%2C%22garageCapacityId%22%3Anull%2C%22hasHotWater%22%3Anull%2C%22hasExterior%22%3Anull%2C%22hasSuspendedFloor%22%3Anull%2C%22hasHeating%22%3Anull%2C%22isFurnish%22%3Anull%2C%22isBankOwned%22%3Anull%2C%22distributionId%22%3Anull%2C%22isOnlyOfficeBuilding%22%3Anull%2C%22ubicationId%22%3Anull%2C%22warehouseType_1%22%3Anull%2C%22warehouseType_2%22%3Anull%2C%22isATransfer%22%3Anull%2C%22isCornerLocated%22%3Anull%2C%22hasSmokeExtractor%22%3Anull%2C%22landType_1%22%3Anull%2C%22landType_2%22%3Anull%2C%22landType_3%22%3Anull%2C%22HasAllDayAccess%22%3Anull%2C%22HasLoadingDockAccess%22%3Anull%2C%22HasTenant%22%3Anull%2C%22addressVisible%22%3Anull%2C%22mlsIncluded%22%3Anull%2C%22freeText%22%3Anull%2C%22RefereceText%22%3Anull%2C%22isLowered%22%3Anull%2C%22priceDropDateFrom%22%3A0%2C%22priceDropDateTo%22%3A0%2C%22arePetsAllowed%22%3Anull%2C%22Equipment%22%3Anull%2C%22OperationStatus%22%3Anull%2C%22AdContract%22%3Anull%2C%22IsRent%22%3Atrue%2C%22IsSale%22%3Afalse%2C%22IsAuction%22%3Afalse%2C%22AdState%22%3Anull%7D; selected_buscador_pro=22594096%2C22593699%2C20438412%2C22565484%2C22287463%2C22352904%2C20702413%2C22549269%2C22524124%2C22540241%2C22540127%2C17133174%2C22506234%2C22506935%2C22261190%2C22498204%2C22500458%2C22497403%2C22500343%2C22499690'
    }
    
    def start_requests(self):
        yield Request(self.post_url[0], callback=self.parse, method="POST", body=json.dumps(self.payload), headers=self.headers)
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        page_source = json.loads(response.body)
        for i in page_source["PlainTextArray"]:
            sel = Selector(text=i, type="html")
            url = sel.xpath("//div[@class='result-details']/a/@href").get()
            yield Request(response.urljoin(url), callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            self.payload["currentPage"] = page
            yield Request(
                self.post_url[0], 
                dont_filter=True, 
                callback=self.parse, 
                method="POST", 
                body=json.dumps(self.payload), 
                headers=self.headers,
                meta={"page":page+1}
            )
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1[@id='titulo']/text()").get()
        if title:
            item_loader.add_value("title", title)
            address = title.split("affitto")[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())

        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            return
        
        rent = response.xpath("//span[@class='price']/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("substring-after(//span[@class='property-ref']/text(),'.')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        room_count = response.xpath("//span[@class='icon-double-bed']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//span[@class='icon-bathroom']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        square_meters = response.xpath("//span[@class='icon-plans']/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        description = "".join(response.xpath("//p[@class='contitle']//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))

        terrace = response.xpath("//li[contains(.,'Terrazza')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            item_loader.add_value("terrace", False)

        parking = response.xpath("//div[@id='caracteristicas']//ul//li//text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        furnished = response.xpath("//div[@id='caracteristicas']//ul//li//text()[contains(.,'arredata')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        energy_label = response.xpath("//li[contains(.,'energetica')]/text()").get()
        if energy_label:
            energy_label = energy_label.split("energetica")[1].strip().split(" ")[0].replace(":","")
            if "Non" not in energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        images = response.xpath("//script[contains(.,'multimediaId')]/text()").get()
        if images:
            images = "["+images.split("'[")[1].split("]'")[0].strip()+"]"
            data = json.loads(images)
            for i in data:
                item_loader.add_value("images", i["src"])
        
        latitude_longitude = response.xpath("//input[@name='defaultLatLng']/@value").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split("ltd:")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latitude_longitude.split("lng:")[1].split("}")[0].strip())
        
        item_loader.add_value("landlord_name", "Desiderio di Casa")
        item_loader.add_value("landlord_phone", "+39 0915083033")
        item_loader.add_value("landlord_email", "info@desideriodicasa.it")
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartamento" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("casa" in p_type_string.lower() or "villetta" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None