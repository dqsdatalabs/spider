# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re



class MySpider(Spider):
    name = 'delattreimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Delattreimmobilier_com_PySpider_france'


    custom_settings = { 
        # "PROXY_UK_ON": True,
        "HTTPCACHE_ENABLED":False,
        "CONCURRENT_REQUESTS" : 16,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "USER_AGENT":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0"
    }

    def start_requests(self):
        url = "https://www.immobiliervalbonne.fr/fr/locations"
        yield Request(url, callback=self.parse)   





    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//li[@class='ad']/a/@href").getall():

            new_url = "https://www.immobiliervalbonne.fr" + url
            with open("xxx_url_file","a",encoding='utf-8') as file:
                file.write(new_url + '\n')
            yield Request(new_url, callback=self.populate_item)




    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)

        if "LOUE" in str(response.xpath("//p[@class='comment']/text()").getall()):
            return

        item_loader.add_value("external_source", self.external_source)

        id = response.xpath("//span[@class='reference']/text()").get()
        if id:
            external_id = id.split()[-1]
            item_loader.add_value("external_id",external_id)


        item_loader.add_value("property_type","house")


        
        rent=response.xpath("//div[@class='title']/h2/text()").get()
        if rent:
            rent = rent.split("€")[0].replace(" ","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        title=response.xpath("//div[@class='title']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)

        description = " ".join(response.xpath("//p[@class='comment']").getall())
        if description:
            item_loader.add_value("description",description)

        room = response.xpath("//span[contains(text(),'pièces')]/text()").get()
        if room:
            room_count = room.split()[0]
            item_loader.add_value("room_count",room_count)

        square_meters = response.xpath("//span[contains(text(),'m²')]/text()").get()
        if square_meters:
            square_meters = square_meters.split()[0]
            item_loader.add_value("square_meters",square_meters)


        utilities = response.xpath("//span[contains(text(),'€ / Mois')]/text()").get()
        if utilities:
            utilities = utilities.split()[0]
            item_loader.add_value("utilities",utilities)

        deposit = response.xpath("//li[contains(text(),'garantie')]/span/text()").get()
        if deposit:
            deposit = deposit.replace(" ","").replace("€","")
            item_loader.add_value("deposit",deposit)

        images= response.xpath("//div[@class='item resizePicture']/img/@src").getall()
        if images:
            item_loader.add_value("images",images)

        item_loader.add_value("address","Riviera-Monaco")
        item_loader.add_value("city","Monaco")
        item_loader.add_value("landlord_phone","+33 4 93 77 96 81")
        item_loader.add_value("landlord_email","delattreimmobilier@orange.fr")
        item_loader.add_value("landlord_name","DELATTRE IMMOBILIER")


        bathroom_count = response.xpath("//li[contains(text(),'Salle')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split()[0]
            item_loader.add_value("bathroom_count",bathroom_count)
        else:
            bathroom_count = response.xpath("//li[contains(text(),'douche')]/text()").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split()[0]
                item_loader.add_value("bathroom_count",bathroom_count)

        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "huis" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None




# def energy_label_calculate(energy_number):
#     energy_number = int(energy_number)
#     energy_label = ""
#     if energy_number <= 50:
#         energy_label = "A"
#     elif energy_number > 50 and energy_number <= 90:
#         energy_label = "B"
#     elif energy_number > 90 and energy_number <= 150:
#         energy_label = "C"
#     elif energy_number > 150 and energy_number <= 230:
#         energy_label = "D"
#     elif energy_number > 230 and energy_number <= 330:
#         energy_label = "E"
#     elif energy_number > 330 and energy_number <= 450:
#         energy_label = "F"
#     elif energy_number > 450:
#         energy_label = "G"
#     return energy_label