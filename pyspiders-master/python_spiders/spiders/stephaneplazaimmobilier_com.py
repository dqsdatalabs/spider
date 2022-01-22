# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import dateparser
from datetime import datetime


class MySpider(Spider):
    name = 'stephaneplazaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Stephaneplazaimmobilier_PySpider_france_fr"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://auch.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=2&agency_id=316&sort=&idagency=166652",
                "property_type" : "house"
            },
            {
                "url" : "https://auch.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=1&agency_id=316&sort=&idagency=166652",
                "property_type" : "apartment"
            },
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        data = json.loads(response.body)
        token = data["token"]
        for item in data["results"]:
            item_loader = ListingLoader(response=response)
            f_url = "https://auch.stephaneplazaimmobilier.com/immobilier-acheter/" + str(item["id"]) + "/" + item["slug"] + "?token=" + token
            item_loader.add_value("rent_string", item["price"])
            item_loader.add_value("title", item["title"])
            item_loader.add_value("external_id", str(item["id"]))
            item_loader.add_value("address", item["properties"]["adresse"])
            item_loader.add_value("latitude", item["properties"]["latitude"])
            item_loader.add_value("longitude", item["properties"]["longitude"])
            item_loader.add_value("energy_label", item["properties"]["consoEner"])
            item_loader.add_value("zipcode", item["properties"]["codePostal"])
            # item_loader.add_value("room_count", item["properties"]["room"])
            item_loader.add_value("city", item["properties"]["city"])
            item_loader.add_value("square_meters", item["properties"]["surface"].split("m")[0].strip())
            item_loader.add_value("description", item["description"])
            item_loader.add_value("images", item["thumbnails"])

            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={'item': item_loader,"property_type" : response.meta.get("property_type")}
            )
            
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader = response.meta.get("item")

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("currency", "EUR")
        room_count=response.xpath("//div/label[.='Chambres']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        bathroom_count=response.xpath("//div/label[contains(.,'Salle d')]/following-sibling::span/text()[not(contains(.,'0'))]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        deposit=response.xpath("//div/label[contains(.,'garantie')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip().split(" ")[0])
        
        utilities=response.xpath("normalize-space(//div/label[contains(.,'Charges ')]/following-sibling::span/text())").get()
        if utilities and utilities.strip() != '0 EUR':
            item_loader.add_value("utilities", utilities.strip().split(" ")[0])

        available_date = response.xpath("normalize-space(//div/label[contains(.,'Date')]/following-sibling::span/text())").get()
        if available_date:
            if not available_date.replace(" ","").isalpha():
                date_parsed = dateparser.parse(available_date)
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
            else:    
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        floor=response.xpath("//div/label[contains(.,'étages')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        furnished=response.xpath("//div[@id='description'][contains(.,' meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
            
        parking =response.xpath("//div/label[contains(.,'Nombre garages') or contains(.,' parking') ]/following-sibling::span/text()").get()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        elevator =response.xpath("//div/label[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        terrace=response.xpath("//div[@id='description'][contains(.,'terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        name=response.xpath("//div[@class='mtitle']/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        
        phone=response.xpath("//div[@class='member-contact']/a[@class='phone']/@title").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        
        
        yield item_loader.load_item()

