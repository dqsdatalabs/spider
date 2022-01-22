# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser


class MySpider(Spider):
    name = 'margauimmo_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Margauimmo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.margauimmo.com/margau-immo-location-de-votre-bien.php#blocAppartements"}
        ]  # LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        )
    # 1. FOLLOWING
    def parse(self, response):
 
        for item in response.xpath("//article[@class='blocAppartements']//div[@class='row']/div"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            if "APPARTEMENT" in item.xpath(".//h4/text()").get():
                property_type = "apartment"
            elif "MAISON" in item.xpath(".//h4/text()").get():
                property_type = "house"
            else:
                property_type = False
            if property_type:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("id=")[1])
        title = " ".join(response.xpath("//h2//text()").extract())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_value("external_source", self.external_source)
     
        address =response.xpath("//div/h2/text()[normalize-space()]").extract_first()
        if address:
            item_loader.add_value("address",address.strip() )     
            zipcode = address.strip().split(" ")[-1]
            item_loader.add_value("city",address.replace(zipcode,"").strip() )  
            item_loader.add_value("zipcode",zipcode.strip() )    
                
        rent =response.xpath("//div/span[contains(@class,'c-template_price')]//text()").extract_first()
        if rent:     
            rent = rent.replace(" ","").split("€")[0].strip()
            item_loader.add_value("rent",rent)    
        item_loader.add_value("currency","EUR")    
        
        utilities =response.xpath("//div[p[contains(.,'Charges')]]/p[2]//text()[not(contains(.,'non'))]").extract_first()    
        if utilities:
            item_loader.add_value("utilities",int(float(utilities.replace("€","").strip()))) 
        deposit =response.xpath("//div[p[contains(.,' de garantie')]]/p[2]//text()[not(contains(.,'non'))]").extract_first()    
        if deposit:
            item_loader.add_value("deposit",int(float(deposit.replace("€","").strip()))) 

        desc = " ".join(response.xpath("//p[contains(@class,'c-template_paragraphe')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "pièce" in desc:
                room = desc.split("pièce")[0].strip().split(" ")[-1]
                if room.isdigit():
                    item_loader.add_value("room_count",room)
            if "m²" in desc:
                unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(m²|meters2|metres2|meter2|metre2|mt2|m2|M2)",desc.split(",")[0].replace(",","."))
                if unit_pattern:
                    sq=int(float(unit_pattern[0][0]))
                    item_loader.add_value("square_meters", str(sq))
            available = ""
            if "Disponible à partir du" in desc:
                available = desc.split("Disponible à partir du")[1].strip().split(".")[0]
            elif "Disponible au" in desc:
                available = desc.split("Disponible au")[1].strip().split(".")[0]
            elif "Disponible immédiatement" in desc:
                available = "now"
            if available:
                date_parsed = dateparser.parse(available.strip(), languages=['fr'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)           
   
        item_loader.add_value("landlord_name", "MARGAU IMMO")
        item_loader.add_value("landlord_phone", "05 32 09 11 93")
        item_loader.add_value("landlord_email", "contact@margaugestion.com")
    
        images = [response.urljoin(x)for x in response.xpath("//div[@class='slides']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
            
        yield item_loader.load_item()