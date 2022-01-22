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
    name = 'abc81_fr'
    execution_type='testing'
    country='france' 
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.abc81.fr/fr/liste.htm?page={}&TypeModeListeForm=text&tdp=5&filtre=2&lieu-alentour=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.abc81.fr/fr/liste.htm?page={}&TypeModeListeForm=text&tdp=5&filtre=8&lieu-alentour=0",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        total_count = response.xpath("//span[@class='NbBien']/text()").get()
        if total_count:
            total_count = int(total_count)
        for item in response.xpath("//a[@itemprop='url']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if page-1 <= total_count/12:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page + 1, "base":base})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Abc81_PySpider_france")
        dontallow=response.xpath("//strong[contains(.,'Oops')]/text()").get()
        if dontallow and "oops" in dontallow.lower():
            return
        item_loader.add_xpath("title", "//h1[@class='heading2']/text()")
        
        address = response.xpath("//h2[contains(@class,'ville')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
        
        square_meters = response.xpath("//span[contains(@class,'surface')]/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//span[contains(@class,'chambre')]/following-sibling::text()[not(contains(.,'NC'))]").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(@class,'piece')]/following-sibling::text()[not(contains(.,'NC'))]").get()
            if room_count:
                room_count = room_count.strip().split(" ")[0]
                item_loader.add_value("room_count", room_count)
        
        rent = response.xpath("//costpermonth/@data-price").get()
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//span[@class='cout_charges_mens']/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'garantie')]/span[@class='cout_honoraires_loc']/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
        
        description = " ".join(response.xpath("//span[@itemprop='description']//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x for x in response.xpath("//div[@class='diapo is-flap']//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//li[@class='gg-map-marker-lat']/text()").get()
        item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//li[@class='gg-map-marker-lng']/text()").get()
        item_loader.add_value("longitude", longitude)
        
        energy_label = response.xpath("//img/@src[contains(.,'dpe') and contains(.,'nrj')][not(contains(.,'Vierge'))]").get()
        if energy_label:
            energy_label = energy_label.split("-")[-1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        external_id = response.xpath("//span[@itemprop='productID']/text()").getall()
        item_loader.add_value("external_id", external_id[1])
        
        item_loader.add_value("landlord_name","ABC Immobilier")
        item_loader.add_value("landlord_phone", "05 63 54 64 09")
        
        yield item_loader.load_item()