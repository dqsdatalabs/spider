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
    name = 'cabinetboucomont_com'
    execution_type='testing' 
    country='france'
    locale='fr'
    external_source="Cabinet_Boucomont_PySpider_france"
    def start_requests(self):
        start_urls = [
            {"url": "https://cabinet-boucomont.com/location/appartements/1", "property_type": "apartment"},
	        {"url": "https://cabinet-boucomont.com/location/maisons-villas/1", "property_type": "house"},
            
        ]  # LEVEL 1 
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),"base_url":url.get('url'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'selectionBien')]/article//h2/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            base_url = response.meta.get("base_url")
            url = base_url.replace(f"/1",f"/{page+1}")
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type":property_type, "base_url":base_url})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//h1[@class='titleBien']/text()")
        titlecheck=item_loader.get_output_value("title")
        if not titlecheck:
            title=response.xpath("//title//text()").get()
            if title:
                item_loader.add_value("title",title)
        
        address = response.xpath("//ul[@id='infos']/li[contains(.,'Ville')]/text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
            item_loader.add_value("city", address.split(":")[1].strip())
        
        zipcode = response.xpath("//ul[@id='infos']/li[contains(.,'Code')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())

        square_meters = response.xpath("//ul[@id='infos']/li[contains(.,'habitable')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].strip().split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", str(int(float(square_meters))))
        else:
            square_meters = response.xpath("//ul[@id='infos']/li[contains(.,'m²')]/text()").get()
            square_meters = square_meters.split(":")[1].strip().split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", str(int(float(square_meters))))
        
        
        room_count = response.xpath("//ul[@id='infos']/li[contains(.,'pièce')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
            
            
        bathroom_count = response.xpath("//ul[@id='details']/li[contains(.,'salle')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())
        
        rent = response.xpath("//ul[@id='infosfi']/li[contains(.,'Loyer')]/text()").get()
        if rent:
            price = rent.split(":")[1].split("€")[0].replace(" ","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//ul[@id='infosfi']/li[contains(.,'garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].replace(" ","")
            item_loader.add_value("deposit", deposit)
        
        utilities = response.xpath("//ul[@id='infosfi']/li[contains(.,'Charge')]/text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].replace(" ","")
            item_loader.add_value("utilities", utilities)
        
        floor = response.xpath("//ul[@id='infos']/li[contains(.,'Etage')]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
        
        lat_lng = response.xpath("//script[contains(.,'lat')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("lat :")[1].split(",")[0].strip()
            longitude = lat_lng.split("lng:")[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude.replace("}","").strip())
        
        external_id = response.xpath("//p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        desc = "".join(response.xpath("//div[@class='offreContent']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//ul[@class='slider_Mdl']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "DPE" in desc:
            desc = desc.replace("\n"," ")
            energy_label = desc.split("DPE")[1].replace("=","").replace(":","").strip().split(" ")[0]
            if "vierge" not in energy_label:
                item_loader.add_value("energy_label", energy_label.replace(".",""))
        
        furnished = response.xpath("//ul[@id='infos']/li[contains(.,'Meublé')]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//ul[@id='infos']/li[contains(.,'Ascenseur')]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//ul[@id='details']/li[contains(.,'Terrasse')]/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            elif "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
        
        balcony = response.xpath("//ul[@id='details']/li[contains(.,'Balcon')]/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            elif "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
        
        parking = response.xpath("//ul[@id='details']/li[contains(.,'garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "CABINET BOUCOMONT")
        item_loader.add_value("landlord_phone", "04 73 91 41 48")
        item_loader.add_value("landlord_email", "e.boucomont@cabinet-boucomont.com")
        
        yield item_loader.load_item()