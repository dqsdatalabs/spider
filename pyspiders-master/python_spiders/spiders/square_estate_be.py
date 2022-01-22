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
import dateparser

class MySpider(Spider):
    name = 'square_estate_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.square-estate.be/nl/component/properties/?view=list&page=1&ptype=2&goal=1&pricemin=0&pricemax=2500",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.square-estate.be/nl/component/properties/?view=list&page=1&ptype=1&goal=1&pricemin=0&pricemax=2500",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.square-estate.be/nl/component/properties/?view=list&page=1&ptype=3&goal=1&pricemin=0&pricemax=2500",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='image']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Square_Estate_PySpider_belgium")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("&id=")[-1].split("&")[0])
        
        title = response.xpath("//h3[1]/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        address = response.xpath("//div[@class='name'][contains(.,'Adres')]/following-sibling::div[@class='value']/text()").get()
        zipcode = ""
        city = ""
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                zipcode = address.split(",")[-1].strip().split(" ")[0]
                city = address.split(zipcode)[1].strip()
            else:
                zipcode = address.split(" ")[0]
                city = address.split(zipcode)[1].strip()
            
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)
        
        rent = response.xpath("//div[@class='name'][contains(.,'Prij')]/following-sibling::div[@class='value']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[1].replace(" ",""))
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div[@class='name'][contains(.,'slaapkamer')]/following-sibling::div[@class='value']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[@class='name'][contains(.,'badkamer')]/following-sibling::div[@class='value']/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//div[@class='name'][contains(.,'Bewoonbare')]/following-sibling::div[@class='value']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        desc = " ".join(response.xpath("//div[@class='content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@class='galleria']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'AddMarker')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("AddMarker(")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("AddMarker(")[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        available_date = response.xpath("//div[@class='name'][contains(.,'Beschikbaarheid')]/following-sibling::div[@class='value']/text()").get()
        if available_date:
            if "Onmiddellijk" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif available_date.replace(" ","").replace("-","").replace("/","").isdigit():
                date_parsed = dateparser.parse(available_date)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        utilities = response.xpath("//div[@class='name'][contains(.,'Lasten')]/following-sibling::div[@class='value']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[1].strip())
        
        energy_label = response.xpath("//div[@class='name'][contains(.,'EPC')]/following-sibling::div[@class='value']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("kW")[0].strip())
        
        terrace = response.xpath("//div[@class='name'][contains(.,'Terras')]/following-sibling::div[@class='value']/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[@class='name'][contains(.,'Comfort')]/following-sibling::div[@class='value']/text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        dishwasher = response.xpath("//div[@class='name'][contains(.,'Comfort')]/following-sibling::div[@class='value']/text()[contains(.,'Afwasmachine')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        item_loader.add_value("landlord_name", "Square Estate")
        item_loader.add_value("landlord_phone", "32 (0)2 460 42 01")
        item_loader.add_value("landlord_email","info@square-estate.be")
        
        yield item_loader.load_item()