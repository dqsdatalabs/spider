# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from html.parser import HTMLParser
import json
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'servicheck_esadsalquilerinmueble'
    execution_type='testing'
    country='spain'
    locale='es'
    
    # custom_settings = {
    #     "PROXY_ON": True,
    # }

    payload = "{\"currentPage\":next_page,\"itemsPerPage\":20,\"order\":\"desc\",\"orderfield\":\"creationDate\",\"ids\":[],\"showAddress\":1,\"adOperationId\":\"2\",\"adScopeId\":null,\"adTypologyId\":\"0\",\"priceMin\":null,\"priceMax\":null,\"CreationDateMin\":null,\"CreationDateMax\":null,\"locationId\":[],\"drawShapePath\":null,\"homes\":null,\"chalets\":null,\"countryhouses\":null,\"isDuplex\":null,\"isPenthouse\":null,\"isStudio\":null,\"isIndependentHouse\":null,\"isSemidetachedHouse\":null,\"isTerracedHouse\":null,\"constructedAreaMin\":null,\"constructedAreaMax\":null,\"rooms_0\":null,\"rooms_1\":null,\"rooms_2\":null,\"rooms_3\":null,\"rooms_4\":null,\"baths_1\":null,\"baths_2\":null,\"baths_3\":null,\"builtTypeId\":null,\"isTopFloor\":null,\"isIntermediateFloor\":null,\"isGroundFloor\":null,\"isFirstFloor\":null,\"hasAirConditioning\":null,\"hasWardrobe\":null,\"hasGarage\":null,\"hasLift\":null,\"hasTerrace\":null,\"hasBoxRoom\":null,\"hasSwimmingPool\":null,\"hasGarden\":null,\"flatLocationId\":null,\"hasKitchen\":null,\"hasAutomaticDoor\":null,\"hasPersonalSecurity\":null,\"HasSecurity24h\":null,\"garageCapacityId\":null,\"hasHotWater\":null,\"hasExterior\":null,\"hasSuspendedFloor\":null,\"hasHeating\":null,\"isFurnish\":null,\"isBankOwned\":null,\"distributionId\":null,\"isOnlyOfficeBuilding\":null,\"ubicationId\":null,\"warehouseType_1\":null,\"warehouseType_2\":null,\"isATransfer\":null,\"isCornerLocated\":null,\"hasSmokeExtractor\":null,\"landType_1\":null,\"landType_2\":null,\"landType_3\":null,\"HasAllDayAccess\":null,\"HasLoadingDockAccess\":null,\"HasTenant\":null,\"addressVisible\":null,\"mlsIncluded\":null,\"freeText\":null,\"RefereceText\":null,\"isLowered\":null,\"priceDropDateFrom\":0,\"priceDropDateTo\":0,\"arePetsAllowed\":null,\"Equipment\":null,\"OperationStatus\":null,\"AdContract\":null,\"IsRent\":true,\"IsSale\":false,\"AdState\":null}"

    headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,tr-TR;q=0.8,tr;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json; charset=UTF-8'
            }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.servicheck.es/ads/alquiler/inmueble/"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.jump)
    def jump(self, response):
        for item in response.xpath("//div[@class='info-results']/h1/a"):
            title = item.xpath("./text()").extract_first()
            if "Piso" in title or "Estudio" in title or "Loft" in title or "Ático" in title:
                follow_url = response.urljoin(item.xpath("./@href").extract_first())
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            elif "Dúplex" in title:
                follow_url = response.urljoin(item.xpath("./@href").extract_first())
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
        
        
        self.payload = self.payload.replace("next_page", "2")
        url = "https://www.servicheck.es/api/AdsSearch/PostSearch"
        yield Request(url, self.parse, method="POST", headers=self.headers, dont_filter=True, body=self.payload)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 3)
        seen = False
        data = json.loads(response.body)
        
        content = data.get("PlainTextArray")
        for c in content:
            sel = Selector(text=c, type="html")
            for item in sel.xpath("//div[@class='info-results']/h1/a"):
                title = item.xpath("./text()").extract_first()
                if "Piso" in title or "Estudio" in title or "Loft" in title or "Ático" in title:
                    follow_url = response.urljoin(item.xpath("./@href").extract_first())
                    property_type = "apartment"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
                    
                elif "Dúplex" in title:
                    follow_url = response.urljoin(item.xpath("./@href").extract_first())
                    property_type = "house"
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
                seen = True

        
        if page < 26:
           
            self.payload = self.payload.replace("next_page", str(page))
            
            url = "https://www.servicheck.es/api/AdsSearch/PostSearch"
            yield Request(url, self.parse, method="POST", dont_filter=True, headers=self.headers, body=self.payload, meta={"page" : page +1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "ServicheckEsadsalquilerinmueble_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//span[@id='titulo']/text()")

        studio = response.xpath("//h1[@id='titulo']/text()").extract_first()
        if "studio" in studio:
            item_loader.add_value("room_count", "1")
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
       
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("bathroom_count", "substring-before(//section/ul/li/span[@class='icon-bathroom']/following-sibling::text(),'Baño')")
        
        external_id=response.xpath("//span[@class='property-ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('.')[1].strip())
        
        rent=response.xpath("//span[@class='price']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)

            deposit_text = response.xpath("//ul/li[contains(.,'Fianza')]/text()").get()
            if deposit_text:
                deposit_text = deposit_text.split(" ")[0].strip()[-1]
                rent = rent.split(" ")[0].replace(".","").strip()
                item_loader.add_value("deposit", str(int(deposit_text) * int(rent)))
        
        square_meters=response.xpath("//section[@class='encabezado']/ul/li[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0])
        
        room_count=response.xpath("//section[@class='encabezado']/ul/li[contains(.,'Dorm')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0])
        
        latitude_longitude=response.xpath("//input[@name='defaultLatLng']/@value").get()
        if latitude_longitude:
            lat=latitude_longitude.split('ltd:')[1].split(',')[0].strip()
            lng=latitude_longitude.split('lng:')[1].split('}')[0].strip()

            item_loader.add_value("longitude", lat)
            item_loader.add_value("latitude", lng)
        
        address = "".join(response.xpath("substring-after(//h1[@id='titulo']/text(),'alquiler')").extract())
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                item_loader.add_value("city", address.strip().split(",")[1].strip())
            elif "-" in address:
                item_loader.add_value("city", address.strip().split("-")[1].strip())
            else:
                item_loader.add_value("city", address.strip())

        desc = "".join(response.xpath("//p[@class='contitle']/text()").getall())
        if desc:
            item_loader.add_value("description", desc)
        
        floor=response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'Planta')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split('Planta')[1].split(',')[0].strip())
            
        energy_label = "".join(response.xpath("substring-after(//div[@class='caracteristicas']/ul/li[contains(.,' energética')]/text(),'energética')").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(" ")[0].strip())
        
        images=[]
        try:
            img="".join(response.xpath(
                "//script[@type='text/javascript' and contains(.,'multimediaId')]/text()").extract())
            image="[" + img.split("'[")[1].split("]'")[0]+ "]"
            json_l=json.loads(image)
            for image in json_l :
                img=image.get("src")
                images.append(img)           
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        except IndexError:
            pass
        
        
        elevator=response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'ascensor')]/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        terrace=response.xpath("//div[@class='caracteristicas']/ul/li[contains(.,'Terraza')]/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)

        item_loader.add_value("landlord_phone", '619 413 579')
        item_loader.add_value("landlord_email", 'info@servicheck.es')
        item_loader.add_value("landlord_name", 'SERVICHECK')
        
        yield item_loader.load_item()