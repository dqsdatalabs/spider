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
import dateparser
class MySpider(Spider):
    name = 'lmhtimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://lmht-immobilier.fr/properties?utf8=%E2%9C%93&buyable=false&property_category_id=1&address=&price=&area=&commit=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://lmht-immobilier.fr/properties?utf8=%E2%9C%93&buyable=false&property_category_id=4&address=&price=&area=&commit=Rechercher",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Lmhtimmo_PySpider_france")

        title =response.xpath("//div[@class='container']//h1/text()").extract_first()
        if title:
            item_loader.add_value("title", title)  
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",title.replace(",","."))
            if unit_pattern:
                sq=int(float(unit_pattern[0][0]))
                item_loader.add_value("square_meters", str(sq))
        address =response.xpath("//p[i[contains(@class,'fa-map-marker-alt')]]/text()[normalize-space()]").extract_first()
        if address:
            item_loader.add_value("address", address.strip()) 
    
        external_id = response.xpath("//div[@class='important-infos']//small[contains(.,'RÉF')]//text()").extract_first()
        if external_id: 
            item_loader.add_value("external_id",external_id.split(" - ")[-1].strip())    
  
        room_count = response.xpath("//span[contains(@class,'label-primary') and contains(.,'chambre')]/b/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count) 
        else:
            room_count = response.xpath("//span[contains(@class,'label-primary') and contains(.,'pièce')]/b/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count) 

        item_loader.add_xpath("bathroom_count", "//div[@class='row icons']/div[contains(.,'salle de bain')]//b/text()")
        rent = response.xpath("//div[@class='important-infos']//b[contains(.,'Loyer hors charges')]/following-sibling::text()[1]").extract_first()
        if rent:     
            item_loader.add_value("rent_string",rent.replace('\xa0', '').replace(' ',''))  
  
        deposit = response.xpath("//div[@class='important-infos']//b[contains(.,'Dépôt de garantie')]/following-sibling::text()[1]").extract_first()
        if deposit:   
            deposit = deposit.split("€")[0].replace(":","").strip().replace(" ","")
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
        
        utilities = "".join(response.xpath("//div[contains(@class,'left-mobile')]/b[contains(.,'Provision sur charges')]/following-sibling::text()").getall())
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip().replace(",",".")
            if utilities != "0.00":
                item_loader.add_value("utilities", int(float(utilities)))
        
        latlng = response.xpath("//script[contains(.,'lng') and contains(.,'lat')]//text()").get()
        if latlng:
            lat = latlng.split('"lat": "')[1].split('"')[0].strip()
            lng = latlng.split('"lng": "')[1].split('"')[0].strip()
            if lat and lng:
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng)

        swimming_pool =response.xpath("//div[@class='row icons']/div[contains(.,'Piscine')]/text()").extract_first()    
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        parking =response.xpath("//div[@class='row icons']/div[contains(.,'Parking')]/text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace =response.xpath("//div[@class='row icons']/div[contains(.,'Terrasse')]/text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        elevator =response.xpath("//div[@class='row icons']/div[contains(.,'Ascenseur')]/text()").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)
    
        available_date = response.xpath("//div[@id='home']/p//text()[contains(.,'Disponible')]").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible")[1].split(".")[0].replace("immédiatement","now").strip())
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
    
        desc = " ".join(response.xpath("//div[@id='home']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@class='picture img']/div[@class='carousel']//a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Agence immobilière LMHT")
        item_loader.add_value("landlord_phone", "01 45 34 57 40")
        yield item_loader.load_item()