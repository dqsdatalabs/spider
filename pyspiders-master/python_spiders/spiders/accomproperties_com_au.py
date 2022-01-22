# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 

class MySpider(Spider):
    name = 'accomproperties_com_au' 
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Accomproperties_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://accomproperties.com.au/property-for-rent/page-1"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse)
    # 1. FOLLOWING


    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//div[@class='property-item-info']"):
            follow_url = item.xpath(".//a/@href").get()
            yield Request(follow_url,callback=self.populate_item)
        follow_btn = response.xpath("//a[@class='page-link']/i[@class='fas fa-chevron-right']")
        if  follow_btn:
            nextpage=f"https://accomproperties.com.au/property-for-rent/page-{page}"
            if nextpage:
                yield Request(nextpage, callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title",title)

        price = response.xpath("//h3[@class='text-muted text-uppercase mb-4']/text()").get()
        if price:
            if "offer" in price.lower():
                return
        if price:
            if re.search("\d+",price):
                rent = int(re.search("\d+",price)[0])
                item_loader.add_value("rent",4*rent)
        a=item_loader.get_output_value("rent")
        if not a:
            rent=response.xpath("//p[@class='text-primary']/following-sibling::h1/text()").get()
            if rent:
                item_loader.add_value("rent",rent.split("$")[-1].split("we")[0])

        bedroom = response.xpath("//i[@class='fa fa-bed mr-1 text-primary']/following-sibling::text()").get()
        if bedroom:
            bedroom = bedroom.strip().split()[0]
            item_loader.add_value("room_count",bedroom)


        bathroom = response.xpath("//i[@class='fa fa-bath mr-1 text-primary']/following-sibling::text()").get()
        if bathroom:
            bathroom = bathroom.strip().split()[0]
            item_loader.add_value("bathroom_count",bathroom)

        garage = response.xpath("//i[@class='fa fa-car mr-1 text-primary']/following-sibling::text()").get()
        if garage:
            item_loader.add_value("parking",True)

        desc = " ".join([desc.strip() for desc in response.xpath("//div[@class='col-lg-8'][div/@class='py-4']/text()").getall()])
        if desc:
            item_loader.add_value("description",desc.strip())

        position = response.xpath("//a[contains(@href,'maps?ll=')]/@href").get()
        if position:
            lat = re.search("maps\?ll=([\d.-]+),([\d.-]+)",position).group(1)
            long = re.search("maps\?ll=([\d.-]+),([\d.-]+)",position).group(2)

            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)
       

        address = response.xpath("//div[@class='listing-address']/strong/text() | //p[@class='text-primary']/i/following-sibling::text()").get()
        if address:
            item_loader.add_value("address",address)
            city = address.split(",")[-1]
            item_loader.add_value("city",city)

        
        item_loader.add_value("currency","USD")
        item_loader.add_value("landlord_phone","07 5440 5322")
        item_loader.add_value("landlord_email","mail@accomproperties.com.au")
        item_loader.add_value("landlord_name","AccomProperties")

        prop_type = get_p_type_string(response.url)
        if prop_type:
            item_loader.add_value("property_type",prop_type)
        else:
            prop_type = get_p_type_string(desc)
            if prop_type:
                item_loader.add_value("property_type",prop_type)

        if prop_type == "studio":
            item_loader.add_value("room_count","1")



        features = " ".join(response.xpath("//ul[@class='listReset featureItems']/li").getall())
        if features:
            if "balcony" in features.lower():
                item_loader.add_value("balcony",True)
            if "furnished" in features.lower():
                item_loader.add_value("furnished",True)

        images = response.xpath("//a[@data-toggle='gallery-top']/@href").getall()
        if images:
            item_loader.add_value("images",images)

        
        if "furnished" in desc.lower():
            item_loader.add_value("furnished",True)
                




        yield item_loader.load_item()


def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label


def get_p_type_string(p_type_string):
    if p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "duplex" in p_type_string.lower() or "triplex" in p_type_string.lower() or "unit" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None    