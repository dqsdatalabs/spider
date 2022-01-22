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
    name = 'riviera_bay_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.riviera-bay.com/immobilier/location-type/appartement-categorie/1p-pieces/", 
                "property_type": "apartment"
            },
            {
                "url": "https://www.riviera-bay.com/immobilier/location-type/maison-categorie/1p-chambres/", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='liste-items']/li/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Riviera_Bay_PySpider_france")

        external_id = "".join(response.xpath("//h2[contains(@class,'titre')]//text()").getall())
        if external_id:
            external_id = external_id.split("réf.")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]/text()").getall())
        if address:
            address = address.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        square_meters = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//small//text()").getall())
        if square_meters:
            if "m²" in square_meters:
                square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
                item_loader.add_value("square_meters", square_meters.strip())

        rent = "".join(response.xpath("//p[contains(@class,'prix')]/text()").getall())
        if rent:
            rent = rent.strip().replace("€","").replace(" ","")
            if rent.isdigit():
                item_loader.add_value("rent", rent)
            else:       
                rent = "".join(response.xpath("//li[contains(.,'Loyer mensuel')]/text()").getall())
                if rent:
                    price = rent.split("€")[0].replace("Loyer mensuel","").strip().replace("\u00a0","")
                    item_loader.add_value("rent", price.strip())
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//li[contains(.,' dépôt de garantie')]//text()").getall())
        if deposit:
            deposit = deposit.split("de dépôt de garantie")[0].replace("€","").strip().split(" ")[-1].replace("\u00a0","")
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//li[contains(.,' dont')]//text()").getall())
        if utilities:
            utilities = utilities.split(" dont")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)

        desc = " ".join(response.xpath("//p[contains(@class,'detail-offre-texte')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'chambre')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//h3[contains(@class,'detail-offre-titre')]//small//text()").getall())
            if room_count and "pièce" in room_count:
                room_count = room_count.split("pièce")[0].strip().split(" ")[-1]
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'salle')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'gallery2')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'garage') or contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(@class,'detail-offre-caracteristique')][contains(.,'terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
                  
        item_loader.add_xpath("longitude", "//div//@data-longgps")
        item_loader.add_xpath("latitude", "//div//@data-latgps")

        item_loader.add_value("landlord_name", "RIVIERA BAY")
        item_loader.add_value("landlord_phone", "+33(0)4 93 04 22 12")
        item_loader.add_value("landlord_email", "contact@riviera-bay.com")

        yield item_loader.load_item()