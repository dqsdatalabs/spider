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
from datetime import datetime

class MySpider(Spider):
    name = 'easymomenthome_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Easymomenthome_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=3&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=4&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=5&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=6&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=7&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=8&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.easymomenthome.com/trouvez-votre-meuble?bien_pieces=All&bien_type=1&bien_ville=&bien_postal_1=All&bien_postal=&items_per_page=12",
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
        for item in response.xpath("//div[contains(@class,'photo')]/span/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Easymomenthome_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='ville-cp']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("-")[0].strip())
            item_loader.add_value("zipcode", address.split("-")[1].strip())
        
        square_meters = response.xpath("//div/text()[contains(.,'Surface')]").get()
        if square_meters:
            square_meters = square_meters.split(":")[1].split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//div/text()[contains(.,'Nombre de pièce')]").get()
        if room_count:
            room_count = room_count.split(":")[1].strip()
            item_loader.add_value("room_count", room_count)
        
        rent = response.xpath("//div[@class='loyer']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split(".")[0])
            item_loader.add_value("currency", "EUR")
        
        desc = "".join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        if "salle de bain" in desc:
            bathroom_count = desc.replace("belle","").split("salle de bain")[0].strip().split(" ")[-1]
            if "une" in bathroom_count:
                item_loader.add_value("bathroom_count", "1")
        
        if "Disponible imm\u00e9diatement" in desc:
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        elif "compter du" in desc:
            available_date = desc.split("compter du")[1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "disponible en" in desc:
            available_date = desc.split("disponible en")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "Disponible au" in desc:
            available_date = desc.split("Disponible au")[1].strip().split(" ")[0].strip(".")
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "Disponible d\u00e8s le" in desc:
            available_date = desc.split("Disponible d\u00e8s le")[1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        energy_label = response.xpath("//img/@src[contains(.,'econome')]").get()
        if energy_label:
            energy_label = energy_label.split("econome-")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        images = [x for x in response.xpath("//a[@rel='gal']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        utilities = response.xpath("//i[contains(.,'charge')]/text()").get()
        if utilities:
            utilities = utilities.split(".")[0].strip()
            if utilities != "0":
                item_loader.add_value("utilities", utilities)
        
        item_loader.add_value("landlord_name", "EASY MOMENT HOME")
        item_loader.add_value("landlord_phone", "33 4 91 99 02 65")
        
        yield item_loader.load_item()