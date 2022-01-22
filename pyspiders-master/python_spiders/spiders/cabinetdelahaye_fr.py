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
    name = 'cabinetdelahaye_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cabinetdelahaye.fr/annonces-immobilieres/?orderBy=PriceDesc&rubric=Rent&townID=&DepartmentId=&SearchLocation=&radius=&type=Apartment&maxPrice=&minArea=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.cabinetdelahaye.fr/annonces-immobilieres/?orderBy=PriceDesc&rubric=Rent&townID=&DepartmentId=&SearchLocation=&radius=&type=House&maxPrice=&minArea=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='properties__thumb']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//ul[@class='pagination-custom']/li[last()]/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Cabinetdelahaye_PySpider_france")      
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        external_id = response.xpath("//div[@class='property__id']/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
     
        room_count = response.xpath("//dl[dd[.='Chambres']]/dd[2]/text()[.!='-']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//dl[dd[.='Pièces']]/dd[2]/text()[.!='-']")
        bathroom_count = response.xpath("//dl[dd[.='Sdb']]/dd[2]/text()[.!='-']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            bathroom_count = response.xpath("//div[@class='property__description-wrap']//text()[contains(.,'salle d')]").get()
            if bathroom_count:
                bathroom_count = bathroom_count.split("salle d")[0].strip().split(" ")[-1]
                if bathroom_count.isdigit():
                    item_loader.add_value("bathroom_count", bathroom_count)
          
        item_loader.add_xpath("floor", "//li[contains(.,'étage')]/strong/text()")     
        square_meters = response.xpath("//dl[dd[.='Surface']]/dd[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        energy_label = response.xpath("//div[@class='dpe']/span/@class").get()
        if energy_label:
            energy = energy_label.split("dpe-")[-1]
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy )
      
        description = " ".join(response.xpath("//div[@class='property__description-wrap']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        script_map = response.xpath("//script[contains(.,'lat: ')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("lat: '")[1].split("'")[0].strip())
            item_loader.add_value("longitude", script_map.split("lng: '")[1].split("'")[0].strip())
        address = response.xpath("//script[contains(.,'address: ')]/text()").get()
        if address:
            address = script_map.split("address: '")[1].split("'")[0].strip()
            zipcode = address.split(" ")[0]
            city = " ".join(address.split(" ")[1:])
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city.strip())

        deposit = response.xpath("//text()[contains(.,'euros caution')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("euros caution")[1].strip().split(" ")[0].strip())
    
        images = [x for x in response.xpath("//div[@class='slides']//img/@data-original").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date = response.xpath("//div[@class='property__description-wrap']//text()[contains(.,'Libre')]").get()
        if available_date:
            available_date= available_date.split("Libre")[-1].replace("au","").replace("le","").strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent ="".join(response.xpath("//div[@class='property__price']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))
   
        utilities = response.xpath("//li[contains(.,'Charges ')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(",")[0])
        item_loader.add_value("landlord_name", "CABINET DELAHAYE ABBEVILLE")
        item_loader.add_value("landlord_phone", "+33 3 22 24 28 27")
        item_loader.add_value("landlord_email", "abbeville@cabinetdelahaye.fr")
        yield item_loader.load_item()
