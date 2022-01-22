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
import math
import dateparser

class MySpider(Spider):
    name = 'bari_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Bari_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://offres.bari.fr/immobilier/recherche?type=location&bien=1&localisation=&budget-location=0%2C5000&budget-vente=0%2C1000000&surface=0%2C500&pieces=0%2C5",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://offres.bari.fr/immobilier/recherche?type=location&bien=2&localisation=&budget-location=0%2C5000&budget-vente=0%2C1000000&surface=0%2C500&pieces=0%2C5",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(@class,'btn-primary')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source","Bari_PySpider_"+ self.country)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ',title.strip()))
        
        address = response.xpath("//div/a/@data-gallery/parent::a/parent::div/h4/text()").get()
        if address:
            address = re.sub('\s{2,}', ' ', address.split(" - ")[-1].strip())
            zipcode = address.split(" ")[0]
            city = address.split(zipcode)[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//div/h2[contains(.,'Conditions')]/parent::div/p/text()[contains(.,'Loyer')]").get()
        if rent:
            price = rent.split("€")[0].split(":")[1].strip().replace(" ","").replace(",",".")
            item_loader.add_value("rent", math.ceil(float(price)))
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("substring-before(//p[@class='bien-info-short']/text()[contains(.,'m')],'m')").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.replace(",",".").strip())))
        
        room_count = response.xpath("//p[@class='bien-info-short']/text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        desc = "".join(response.xpath("//div/h2[contains(.,'Descriptif')]/parent::div/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [ x for x in response.xpath("//div/a/@data-gallery/parent::a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div/h2[contains(.,'Descriptif')]/parent::div/p[contains(.,'Référence')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        available_date = response.xpath("//p[@class='bien-info-short']/i[contains(@class,'date')]/parent::p/text()[contains(.,'/')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        else:
            match = re.search(r'(\d+/\d+/\d+)', desc)
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
        
        energy_label = response.xpath("//div/h4[contains(.,'Diagnostic')]/parent::div/img/@src").get()
        if energy_label:
            energy_label = energy_label.split("de-")[1].split(".")[0].upper()
            item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//p[@class='bien-info-short']/parent::div/h4/text()[contains(.,'ème')]").get()
        if floor:
            item_loader.add_value("floor", floor.replace("ème","").split(" ")[1])
        
        utilities = response.xpath("//div/h2[contains(.,'Conditions')]/parent::div/p/text()[contains(.,'charges')]").get()
        if utilities:
            utilities = utilities.split("€")[0].split(":")[1].strip().replace(" ","").replace(",",".")
            if utilities != "0.00":
                item_loader.add_value("utilities", math.ceil(float(utilities)))
        
        deposit = response.xpath("//div/h2[contains(.,'Conditions')]/parent::div/p/text()[contains(.,'garantie')]").get()
        if deposit:
            deposit = deposit.split("€")[0].split(":")[1].strip().replace(" ","").replace(",",".")
            item_loader.add_value("deposit", math.ceil(float(deposit)))
        
        item_loader.add_value("landlord_name", "BARI ADMINISTRATION DE BIENS")
        item_loader.add_value("landlord_phone", "04 72 44 39 04")
        item_loader.add_value("landlord_email", "location@bari.fr")

        yield item_loader.load_item()