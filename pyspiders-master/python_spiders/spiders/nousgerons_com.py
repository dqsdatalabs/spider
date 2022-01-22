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
    name = 'nousgerons_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.nousgerons.com/recherche"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a/div[@class='info-item']/parent::a"):
            address = item.xpath(".//p[@class='city_name_']/text()").get()
            square_meters = item.xpath(".//p[@class='price_surface']/em/text()").get()
            if square_meters:
                square_meters = square_meters.split(" ")[0]
            follow_url = response.urljoin(item.xpath(".//@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'address': address, 'square_meters': square_meters})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Nousgerons_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)
    
        desc = "".join(response.xpath("//div[@class='description']/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        title = "".join(response.xpath("//div/p[@class='ref']/parent::div/p[2]//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        rent = response.xpath("//div/p[contains(.,'Loyer')]/text()").get()
        if rent:
            price = rent.split(":")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        address = response.meta.get('address')
        item_loader.add_value("address", address)
        
        zipcode = address.split("(")[1].split(")")[0]
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        city = address.split("(")[0].strip()
        if city:
            item_loader.add_value("city", city)
        
        item_loader.add_value("square_meters", response.meta.get('square_meters'))
        
        room_count = response.xpath("//div/p[@class='ref']/parent::div/p/text()[contains(.,'pièce')]").get()
        if room_count:
            room_count = room_count.split("pièce")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//p[contains(.,'pièce')]/text()").re_first(r'(\d)\s*pièce')
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("latitude = '")[1].split("'")[0]
            longitude = latitude_longitude.split("longitude = '")[1].split("'")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        external_id = response.xpath("//div/p[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Réf")[1].strip())
        
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//div[contains(@id,'first_picture')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        deposit = response.xpath("//div/p[contains(.,'garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        
        utilities = "".join(response.xpath("//div[@class='description']/text()[contains(.,'charge')]").getall())
        if utilities:
            if "Charges :" in utilities:
                utility = utilities.split("Charges :")[1].strip().split(" ")[0]
                item_loader.add_value("utilities", utility)
            if "+" in utilities:
                utility = utilities.split("+")[1].strip().split(" ")[0]
                if "€" in utility:
                    if utility.replace("€","").isdigit():
                        item_loader.add_value("utilities", utility.replace("€",""))
                    else:
                        utility = utilities.split("+")[2].strip().split(" ")[0].replace("€","")
                        item_loader.add_value("utilities", utility)
            elif "de charge" in utilities:
                utility = utilities.split("de charge")[0].replace("€","").replace("euros","").strip().split(" ")[-1]
                if utility:
                    item_loader.add_value("utilities", utility)

        if not item_loader.get_collected_values("utilities"): 
            if response.xpath("//text()[contains(.,'Charges:')]").get(): 
                item_loader.add_value("utilities", response.xpath("//text()[contains(.,'Charges:')]").get().split('Charges:')[1].split('€')[0].strip())

        if not item_loader.get_collected_values("utilities"): 
            if response.xpath("//p[contains(.,\"Frais d'agence\")]/text()").get(): 
                item_loader.add_value("utilities", response.xpath("//p[contains(.,\"Frais d'agence\")]/text()").get().split(':')[-1].split('€')[0].strip())
        
        furnished = response.xpath("//li/i[contains(@class,'furnished')]//following-sibling::span/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li/i[contains(@class,'elevator')]//following-sibling::span/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        energy_label = response.xpath("//img[@alt='diagnostic']/@src[not(contains(.,'ges-'))]").get()
        if energy_label:
            try:
                energy_label = energy_label.split("ce-")[1].split(".")[0].capitalize()
                item_loader.add_value("energy_label", energy_label)
            except: pass
        
        washing_machine = response.xpath("//li/i[contains(@class,'washing_machine')]//following-sibling::span/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "NOUSGERONS")
        item_loader.add_value("landlord_phone", "01 84 80 26 21")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None