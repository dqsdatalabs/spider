# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'planetimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    start_urls = ['https://www.planetimmobilier.com/location/1']  # LEVEL 1
    custom_settings = {
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
 
    }

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'links-group__link')]/@href").extract():
            follow_url = response.urljoin(item)
            if "location" in follow_url:
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        try:
            page = response.xpath("//ul[@class='pagination__items']/li/a/text()").getall()[-3].strip()
            print(page)
            for i in range(2,int(page)+1):
                url = f"https://www.planetimmobilier.com/location/{i}"
                yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        except: pass

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[contains(@class,'main-info__text-block')]//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            if get_p_type_string(response.url):
                item_loader.add_value("property_type", get_p_type_string(response.url))
            else:
                return
        item_loader.add_value("external_source", "PlanetImmobilier_PySpider_france")
        
        title = response.xpath("//h1/span/text()").get()
        item_loader.add_value("title", title)
        
        address = response.xpath("//header[contains(@class,'main-info__info')]//span[contains(@class,'title-subtitle__subtitle')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            city = address.split("(")[0].strip()
            zipcode = address.split("(")[1].split(")")[0].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
        external_id = response.xpath("//header[contains(@class,'main-info__info')]//div[contains(@class,'id')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        square_meters = response.xpath("//div[span[contains(.,'habitable')]]/span[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//div[span[contains(.,'pièces')]]/span[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[span[contains(.,'salle')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
            
        rent = response.xpath("//div[span[contains(.,'Loyer')]]/span[2]/text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(" ","").split("€")[0].strip())
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath("//div[span[contains(.,'Dépôt')]]/span[2]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").split("€")[0].strip())
        
        utilities = response.xpath("//div[span[contains(.,'Charge')]]/span[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip())
        
        desc = "".join(response.xpath("//div[contains(@class,'main-info__text-block')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor = response.xpath("//div[span[contains(.,'Etage')]]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//div[span[contains(.,'Meublé')]]/span[2]/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
                
        elevator = response.xpath("//div[span[contains(.,'Ascenseur')]]/span[2]/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
        
        latitude = response.xpath("//div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-slider__list')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        parking=response.xpath("//span[contains(.,'garage')]/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        
        item_loader.add_value("landlord_name", "Planet Immobilier Real Estate")
        item_loader.add_value("landlord_phone", "04 93 61 44 50")
        item_loader.add_value("landlord_email", "info@planetimmobilier.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None