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
    name = 'cabinet_vaillant_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.cabinet-vaillant.com/category/location/"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//h2[@class='entry-title']"):
            p_type_info = item.xpath("./a//text()").get()
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"p_type_info":p_type_info})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.cabinet-vaillant.com/category/location/page/{page}/"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cabinet_Vaillant_PySpider_france")
        item_loader.add_value("external_link", response.url)
        p_type_info = response.meta["p_type_info"]
        if get_p_type_string(p_type_info):
            item_loader.add_value("property_type", get_p_type_string(p_type_info))
        else:
            return
        
        title = response.xpath("//h1/strong/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        address = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'Localisation')]").get()
        if address:
            address = address.split(":")[1].strip()
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'Surface')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].split("m")[0].strip())
        
        room_count = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'chambre')]").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            if room_count:
                item_loader.add_value("room_count", room_count)
            else:
                room_count = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'pièce')]").get()
                item_loader.add_value("room_count", room_count.split(":")[1].strip())
                
        bathroom_count = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'Salle')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)        
        
        rent = response.xpath(
            "//p/strong[contains(.,'CONDITIONS')]/parent::p//text()[contains(.,'Loyer')]").get()
        if rent:
            item_loader.add_value("rent", rent.split(":")[1].split("euro")[0].replace(" ",""))
            item_loader.add_value("currency", "EUR")
        
        floor = response.xpath("//p/strong[contains(.,'CARACTERIS')]/parent::p//text()[contains(.,'Etage')]").get()
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].strip())
        
        energy_label = response.xpath(
            "//p/strong[contains(.,'DIAGNOSTICS')]/parent::p//text()[contains(.,'Classe Energie')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
        
        deposit = response.xpath(
            "//p/strong[contains(.,'CONDITIONS')]/parent::p//text()[contains(.,'garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(":")[1].split("euro")[0].replace(" ",""))
        
        utilities = response.xpath(
            "//p/strong[contains(.,'CONDITIONS')]/parent::p//text()[contains(.,'Charge')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("euro")[0].replace(" ",""))
        
        desc = "".join(response.xpath("//div[@class='entry-content']//p[1]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//img[@loading='lazy']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//h1/strong/text()[contains(.,'meublé') or contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        
        
        item_loader.add_value("landlord_name", "CABINET VAILLANT")
        item_loader.add_value("landlord_phone", "01 83 75 05 30")
        item_loader.add_value("landlord_email", "contact@cabinet-vaillant.com")
        
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("pièces" in p_type_string.lower() or "pieces" in p_type_string.lower()):
        return "room"
    else:
        return None