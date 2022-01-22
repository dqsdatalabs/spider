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
    name = 'brighignaimmobiliare_it'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Brighignaimmobiliare_PySpider_italy"
    start_urls = ['https://www.brighignaimmobiliare.it/ricerche/?order=ASC&action=rem_search_property&search_property=&property_purpose=Affitto&property_tipo=Residenziale&property_city=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'hover-effect image')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = "".join(response.xpath("//div[strong[contains(.,'Tipo di')]]/text()").getall())
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//title/text()").get()
        item_loader.add_value("title", title)

        rent = response.xpath("//div[strong[contains(.,'Prezzo')]]/span/text()").get()
        if rent:
            rent = rent.split(",")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")



        bathroom_count = "".join(response.xpath("//div[strong[contains(.,'Bagni')]]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())

        utilities = "".join(response.xpath("//div[strong[contains(.,'Spese')]]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].strip())

        square_meters = "".join(response.xpath("//div[strong[contains(.,'Superficie')]]/text()").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(":")[1].strip())

        city = "".join(response.xpath("//div[strong[contains(.,'Città')]]/text()").getall())
        if city:
            item_loader.add_value("city", city.strip())

        elevator = response.xpath("//div[contains(@class,'ascensore')]/span/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[contains(@class,'terrazzino')]/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath("//div[contains(@class,'terrazzino')]/span/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        description = "".join(response.xpath("//div[@class='description']//p//text()").getall())
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))


        room_count = "".join(response.xpath("//div[strong[contains(.,'Vani')]]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        else:
            room_count = re.search("(\d) vani",description)
            if room_count:
                room_count = room_count.group(1)
                item_loader.add_value("room_count",room_count)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value("external_id",external_id)

        zipcode = "".join(response.xpath("//div[strong[contains(.,'CAP')]]/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())

        

        latitude_longitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":"')[1].split('"')[0]
            longitude = latitude_longitude.split('longitude":"')[1].split('"')[0].strip()
            address = latitude_longitude.split('address":"')[1].split('"')[0]
            item_loader.add_value("address", f"{address}, {city}".strip())
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='fotorama-custom']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Brighigna Immobiliare")
        item_loader.add_value("landlord_phone", "055 4684583")
        item_loader.add_value("landlord_email", "info@brighignaimmobiliare.it")

        floor = response.xpath("//span[contains(text(),'Piano:')]/text()").get()
        if floor:
            floor = floor.split(":")[-1].strip()
            item_loader.add_value("floor",str(convert_floor_number(floor)))

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and ("appartament" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None

def convert_floor_number(number_text):
    if "Terra" in number_text:
        return 0
    if number_text == "Primo":
        return 1
    elif number_text == "Secondo":
        return 2

    elif number_text == "Terzo":
        return 3
    
    elif number_text == "Quarto":
        return 4
    elif number_text == "Quinto":
        return 5
    elif number_text == "Sesto":
        return 6
    else: 
        return ""