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

class MySpider(Spider):
    name = 'urbis_immo_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=Appartement&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=appartement&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=duplex&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=Maison&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=maison+bourgeoise&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.urbis-immo.be/a-louer/?fleximmo-category=studio&fleximmo-localite=&fleximmo-price-min=&fleximmo-price-max=",
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
        for item in response.xpath("//a[@class='estate__link']"):
            status = item.xpath(".//span[@class='estate__status']//text()").get()
            if status and "Loué" in status:
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='pagination__prevnext']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Urbis_Immo_PySpider_belgium")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1[contains(@class,'name')]/text()").get()
        if title:
            item_loader.add_value("title", title.replace("\t","").strip())
        
        address = response.xpath("//div[contains(@class,'address')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())
        
        rent = response.xpath("//div[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
            
        square_meters = response.xpath("//li[contains(.,'habitable')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//li[contains(.,'Chambre')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Salle')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//li[contains(.,'Garage')]/strong/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("lat:")[1].split(",")[0].strip()
            longitude = latitude_longitude.split("lng:")[1].split("}")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = " ".join(response.xpath("//div[contains(@class,'descriptive')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        energy_label = response.xpath("//td/img[contains(@src,'peb')]/parent::td/following-sibling::td/text()").get()
        if "PEB CODE" in desc:
            energy_label = desc.split("PEB CODE")[1].strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label)
        elif energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        
        if "libre le" in desc.lower():
            available_date = desc.lower().split("libre le")[1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[contains(@class,'picture')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "URBIS IMMO")
        
        phone = response.xpath("//div[contains(@class,'phone')]/a/text()").get()
        if phone:
            phone = phone.split("+")[1].strip()
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_email","info@urbis-immo.be")
        
        yield item_loader.load_item()