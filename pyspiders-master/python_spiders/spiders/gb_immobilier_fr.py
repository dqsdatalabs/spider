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
    name = 'gb_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.gbimmobilier.net/rechercher.php?type_transaction=location&type_bien=appartement&nb_pieces=&secteur=0&budget=0%2C2000&submit_recherche=Rechercher",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.gbimmobilier.net/rechercher.php?type_transaction=location&type_bien=maison&nb_pieces=&secteur=0&budget=0%2C2000&submit_recherche=Rechercher",
                    
                    ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response): 
        for item in response.xpath("//div[contains(@class,'entry-cover')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        next_page = response.xpath("//li[contains(.,' > ')]/a/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Gb_Immobilier_PySpider_france")
        item_loader.add_value("external_id", response.url.split("_")[-1].split(".")[0])
        
        address = ["fourchambault", "nevers", "proche gare", "centre ville", "magny cours"]
        title = response.xpath('/html/head/title/text()').get()
        if title:
            item_loader.add_value("title", title.strip())
            for add in address:
                if add in title.lower().replace("-"," "):
                    item_loader.add_value("address", add.capitalize())
                    item_loader.add_value("city", add.capitalize())
                    break

        rent = response.xpath("//span[@class='prix']//text()").get()
        if rent:
            price = rent.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//div[@class='surfaceHab']//text()[contains(.,'Habitable')]").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//div[@class='nbChambres']//text()[contains(.,'chambre')][not(contains(.,'0'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = response.xpath("//div[@class='nbPieces']//text()[contains(.,'pièce')][not(contains(.,'0'))]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        description = " ".join(response.xpath("//div[@class='entry-content']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        if "garantie" in description:
            deposit = description.split("garantie")[1].split("euro")[0].replace(":","").strip()
            if deposit.isdigit():
                item_loader.add_value("deposit", deposit)
        
        if "dont" in description:
            utilities = description.split("dont")[1].split("euro")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        elif "charges :" in description.lower():
            utilities = description.split("Charges :")[1].split("euro")[0].strip()
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)
        
        import dateparser
        if "Libre le" in description:
            available_date = description.split("Libre le")[1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        energy_label = response.xpath("//img/@src[contains(.,'DPE')]").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            if energy_label != "NS":
                item_loader.add_value("energy_label", energy_label)
        
        
        item_loader.add_value("landlord_name", "GERARD BELON IMMOBILIER")
        item_loader.add_value("landlord_phone", "03 86 61 14 15")
        item_loader.add_value("landlord_email", "nevers@gbimmobilier.net")
        
        yield item_loader.load_item()