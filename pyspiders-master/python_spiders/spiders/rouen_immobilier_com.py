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
    name = 'rouen_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.rouen-immobilier.com/immobilier/recherche?transaction=location&zone=&ville=&tb%5B0%5D=appartement&nb_piece_min=&nb_piece_max=&pmin=&pmax=&additionalFilter=&page=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.rouen-immobilier.com/immobilier/recherche?transaction=location&zone=0&zone=&ville=&tb%5B%5D=maison&nb_piece_min=&nb_piece_max=&pmin=&pmax=&additionalFilter=&page=1",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Voir le bien')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='next_actice']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Rouen_Immobilier_PySpider_france")
        
        title = response.xpath("normalize-space(//h1/text())").get()
        item_loader.add_value("title", title)
        
        item_loader.add_value("address", "Rouen")
        item_loader.add_value("city", "Rouen")
        item_loader.add_value("zipcode", "76000")
        
        rent = response.xpath("//div[@class='section-title']/p[@class='price']/text()").get()
        if rent:
            price = rent.replace(",",".").split("€")[0].replace(" ","")
            item_loader.add_value("rent", int(float(price)))
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//td[contains(.,'habitable')]/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split(" m")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//li[contains(.,'pièce')]/text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//td[contains(.,'Salle')]/following-sibling::td/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        external_id = response.xpath("//td[contains(.,'Ref')]/following-sibling::td/text()").get()
        item_loader.add_value("external_id", external_id)
        
        energy_label = response.xpath("//div[@id='conso_reel']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        desc = " ".join(response.xpath("//div[@class='desc']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            deposit = re.search("garantie :[ ]*([\d]+)",desc)
            if deposit:
                item_loader.add_value("deposit",deposit.group(1))
            utility = re.search("Charges :[ ]*([\d]+)",desc)
            if utility:
                item_loader.add_value("utilities",utility.group(1))
        
        images = [x.split("url(")[1].split(");")[0] for x in response.xpath("//div[@class='slide-image']//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Rouen Immobilier")
        item_loader.add_xpath("landlord_phone", "//li[@class='tel']/a/text()")
        
        yield item_loader.load_item()