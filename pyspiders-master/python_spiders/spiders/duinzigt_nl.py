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
from datetime import datetime
from word2number import w2n

class MySpider(Spider): 
    name = 'duinzigt_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source="Duinzigt_PySpider_netherlands"
    start_urls = ["https://www.duinzigt.nl/nl/search?minPrice=%E2%82%AC0&maxPrice=%E2%82%AC3000&minRooms=1&maxResidents=1"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='result']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        f_text = " ".join(response.xpath("//dt[contains(.,'Objectsoort')]/following-sibling::dd/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1[@class='title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        room_count = response.xpath("//dt[contains(.,'Aantal Kamers')]/following-sibling::dd/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//dt[contains(.,'Oppervlak')]/following-sibling::dd/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
            
        rent = response.xpath("//dt[contains(.,'Totale huur')]/following-sibling::dd/text()").re_first(r"\d+")
        if rent:
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
        city=response.xpath("//div[@class='breadcrumbs']//span/text()").get()
        if city:
            item_loader.add_value("city",city)

        adres=response.xpath("//div[@class='breadcrumbs']//a[3]/text()").get()
        if adres:
            item_loader.add_value("address",adres+" "+city)

        
        desc = response.xpath("//h3/following-sibling::b//text()").get()
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        descheck=item_loader.get_output_value("description") 
        if not descheck:
            description=" ".join(response.xpath("//article[@class='project__article']//text()").getall())
            if description:
                description=description.replace("\n","")
                item_loader.add_value("description",re.sub('\s{2,}', ' ', description))
        
        images = response.xpath("//div[@class='swiper-slide']/img/@src").getall()
        if images:
            item_loader.add_value("images", images)
        
        utilities = response.xpath("//dt[contains(.,'Huurprij')]/following-sibling::dd/text()").re_first(r"\d+")
        if utilities:
            utilities = int(rent)-int(utilities)
            if utilities != 0:
                item_loader.add_value("utilities", utilities)
        
        floor = response.xpath("//dt[contains(.,'Etage')]/following-sibling::dd/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        available_date = response.xpath("//dt[contains(.,'Beschikbaar')]/following-sibling::dd/text()").get()
        if available_date:
            if "Direct" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
            
        pets_allowed = response.xpath("//div/dt[contains(.,'Huisdieren:')]/following-sibling::dd/text()").get()
        if pets_allowed:
            if "Nee" in pets_allowed:
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)
        
        features = "".join(response.xpath("//dt[contains(.,'Tuin')]/following-sibling::dd/text()").getall())
        if features:
            if "Balkon" in features:
                item_loader.add_value("balcony", True)
            if "terras" in features:
                item_loader.add_value("terrace", True)
        
        furnished = "".join(response.xpath("//dt[contains(.,'Inrichting')]/following-sibling::dd/text()").getall())
        if furnished:
            item_loader.add_value("furnished", True)
        
        item_loader.add_value("landlord_name", "Duinzigt Wonen BV")
        item_loader.add_value("landlord_phone", "31 (0)70-3606365")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None