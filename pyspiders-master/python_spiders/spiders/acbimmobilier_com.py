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
from datetime import datetime

class MySpider(Spider):
    name = 'acbimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        yield Request("https://www.acbimmobilier.com/locations", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='blocimageaffaire']/@href").getall():
            follow_url = response.urljoin(item)
            property_type = None
            if "appartement" in follow_url.lower(): property_type = "apartment"
            elif "maison" in follow_url.lower(): property_type = "house"
            if property_type: yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
        
        next_button = response.xpath("//img[contains(@src,'right.')]/../@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])  
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Acbimmobilier_PySpider_france")
        
        title = response.xpath("//div[@class='zonetitrelarge']/h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            if "réf" in title.lower():
                external_id = title.lower().split("réf")[1].replace(".","").strip().split(" ")[0]
                item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//div[contains(@class,'cadre100')][1]/span/strong/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
        
        rent = response.xpath("//div[contains(@class,'cadre100')]/span/strong/text()[contains(.,'€')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace(" ",""))
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//div[contains(@class,'cadre100')]/span/text()[contains(.,'charge')]").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().split(" ")[-1]
            if utilities !="0":
                item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//text()[contains(.,'Dépot')]").get()
        if deposit:
            deposit = deposit.lower().split("garantie")[1].replace(" ","")
            if deposit:
                item_loader.add_value("deposit", deposit)
        
        description = " ".join(response.xpath("//h2[contains(.,'Description')]/../p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        if "chambre" in description:
            room_count = description.split("chambre")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            elif "piece" in response.url:
                room_count = response.url.split("_piece")[0].strip().split("_")[-1]
                item_loader.add_value("room_count", room_count)
        elif "piece" in response.url:
            room_count = response.url.split("_piece")[0].strip().split("_")[-1]
            item_loader.add_value("room_count", room_count)
            
        if "m²" in description:
            square_meters = description.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        images = [x for x in response.xpath("//figure//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//div[@id='dpeval']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name","ACB - VIARMES")
        item_loader.add_value("landlord_phone", "01 30 35 37 00")
        item_loader.add_value("landlord_email", "viarmes@acbimmobilier.com")

        import dateparser
        available_date = response.xpath("//div[contains(@class,'cadre100')]/span/text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.split(":")[-1].split(")")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                if "0001" not in date2: item_loader.add_value("available_date", date2)            
                if (date_parsed.year == datetime.now().year) or (date_parsed.year == datetime.now().year - 1):
                    yield item_loader.load_item()
                elif date2.split('-')[0] == '1':
                    yield item_loader.load_item()