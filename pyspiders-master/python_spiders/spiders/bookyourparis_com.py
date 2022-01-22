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
    name = 'bookyourparis_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    headers = {
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'http://bookyourparis.com/list-appartements.php?dotri=yes',
        'Accept-Language': 'tr,en;q=0.9',
        'Cookie': '__utmz=59843774.1614748991.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); PHPSESSID=d3a491026b93c9bab785d1ac30d742c9; __utma=59843774.683736318.1614748991.1614748991.1614845973.2; __utmc=59843774; __utmt=1; __utmb=59843774.4.10.1614845973; PHPSESSID=31eeefc6ac597f2aa8106833f6c49b36'
    }
    custom_settings = {"HTTPCACHE_ENABLED": False}

    def start_requests(self):
        yield Request("http://bookyourparis.com/list-appartements.php?dotri=yes", dont_filter=True, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 16)
        seen = False

        for item in response.xpath("//div[@class='item']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        if page == 16 or seen:
            follow_url = f"http://bookyourparis.com/scrollAppartements.php?total=525&last={page}"
            yield Request(follow_url, headers=self.headers, callback=self.parse, meta={"page": page + 16})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
             
        item_loader.add_value("external_link", response.url) 
        property_type = " ".join(response.xpath("//span[@class='TxtAppart']//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        item_loader.add_value("external_source", "Bookyourparis_PySpider_france")

        external_id = response.xpath("//div[contains(@class,'RefAppart')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//span[contains(@class,'Address')]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//span[contains(@class,'Address')]/text()[2]").get()
        if city:
            item_loader.add_value("city", city.strip())

        square_meters = response.xpath("//div[contains(@class,'ChAppart')]/text()").get()
        if square_meters:
            square_meters = square_meters.split(",")[1].split("m²")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        rent = response.xpath("//span[contains(@class,'PriceAppart')]/strong//text()").get()
        if rent:
            rent = rent.replace("€","").strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        desc = " ".join(response.xpath("//span[contains(@class,'TxtAppart')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'ChAppart')]/text()").get()
        if room_count:
            room_count = room_count.split(",")[0].strip()
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
            else:
                room_count = room_count.split(" ")[0]
                item_loader.add_value("room_count", room_count)
        
        floor = response.xpath("//div[contains(@class,'ChAppart')]/text()").get()
        if floor:
            floor = floor.split(",")[2].strip()
            item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//div[contains(@class,'listImages')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        # balcony = response.xpath("//span[contains(.,'Balcon')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        # if balcony:
        #     item_loader.add_value("balcony", True)
        
        # terrace = response.xpath("//span[contains(.,'Terrasse')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        # if terrace:
        #     item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[contains(@class,'OptionsAppart')]//text()[contains(.,'meuble')][not(contains(.,'immeuble') or contains(.,'Immeuble'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'OptionsAppart')]//text()[contains(.,'ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        dishwasher = response.xpath("//div[contains(@class,'OptionsAppart')]//text()[contains(.,'lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        energy_label = response.xpath("//div[contains(@id,'consoenergy')]//@src").get()
        if energy_label:
            energy_label = energy_label.split("/")[-1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Book Your Paris")
        item_loader.add_value("landlord_phone", "+33 (0)1 75 77 14 35")
        item_loader.add_value("landlord_email", "contact@bookyourparis.com")
            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "maison" in p_type_string.lower():
        return "house"
    else:
        return None
       

        
        
          

        

      
     