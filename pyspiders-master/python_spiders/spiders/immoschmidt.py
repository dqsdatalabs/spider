# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from __future__ import absolute_import
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy import Request, FormRequest
from lxml import html as lxhtml
import json
from scrapy.linkextractors import LinkExtractor
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from python_spiders.loaders import ListingLoader
import dateparser


class MySpider(Spider):
    name = "immoschmidt"
    # download_timeout = 60
    # custom_settings = {
    #     "PROXY_ON": True
    # }
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    external_source = "Immoschmidt_PySpider_belgium_en"

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.immoschmidt.be/locations/type-03-Appartement/price-200;4500/price_from-200/price_to-4500/room-0;5/room_from-0/room_to-5/view-view2", "property_type" : "apartment"
                #"url" : "https://www.immoschmidt.be/locations/type-03-Appartement/price-0;2500/room-0;5/view-view2", "property_type" : "apartment"
            },
            {
               "url" : "https://www.immoschmidt.be/locations/type-01-Maison/price-200;4500/price_from-200/price_to-4500/room-0;5/room_from-0/room_to-5/view-view2", "property_type" : "house"

                #"url" : "https://www.immoschmidt.be/locations/type-01-Maison/price-0;2500/price_from-0/price_to-2500/room-0;5/room_from-0/room_to-5/view-view2", "property_type" : "house"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse, meta={"property_type": url.get("property_type")})


    def parse(self, response):
        
        for item in response.xpath("//div[@class='properties__offer-column']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(url=follow_url,
                            callback=self.populate_item,
                            meta={'property_type': response.meta.get('property_type')})

    def populate_item(self, response):

        item_loader = ListingLoader(response=response)            
        property_type = response.meta.get("property_type")
        prop = "".join(response.xpath("//h1/text()").extract())
        if prop and "studio" in prop.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", property_type)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("description", "//div[@class='property__description']/p//text()[normalize-space()]")
        rent = response.xpath(
            "//div[@class='property__price']//strong/text()"
        ).extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        
        utilities = response.xpath("//li[text()='charges locataire :']/strong/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities", utilities)       

        room =  response.xpath("//li[text()='nombre de chambres :']/strong/text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.strip().split("bedroom(s)")[0])
        else:
            studio = "".join(response.xpath("//head/title/text()").extract())              
            if "Studio" in studio:
                item_loader.add_value("room_count","1")
        address = response.xpath("//div[h4[.='Situation']]//li/text()").get()
        if address:
            item_loader.add_value("address", address)
        city = response.xpath("//h1/span/text()").get()
        if city:
            item_loader.add_value("zipcode", city.strip().split(" ")[0])
            item_loader.add_value("city", " ".join(city.strip().split(" ")[1:]))
        square =  response.xpath("//li[text()='surface habitable nette :']/strong/text()").extract_first()                
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
   
        bathroom_count = response.xpath("//li[text()='nombre de sdb :' or text()='nombre de sdd :']/strong/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        date = response.xpath("//div//li[text()='disponibilité :']/strong/text()").extract_first()
        if date:
            date_parsed = dateparser.parse(date, date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        energy_label = response.xpath("//li[text()='classe énergétique :']/strong/img/@src").get()
        if energy_label:
            item_loader.add_value(
                "energy_label",
                ((energy_label.split("/")[-1]).split(".")[0])
                .upper()
                .replace("P", "+")
                .replace("M", "-"),
                )

        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//div[@id='properties-thumbs']/div//a/@href"
            ).extract()
        ]
        item_loader.add_value("images", images)

        parking = response.xpath("//li[text()='garage :']/strong/text()").get()
        if parking:
            if parking.strip() == "oui":
                item_loader.add_value("parking", True)
            elif parking.strip() == "non":
                item_loader.add_value("parking", False)

        item_loader.add_value(
            "landlord_name", "Schmidt Immobilier Etterbeek"
        )

        furnished = response.xpath("//div[contains(@class,'property__params')][1]//h4//text()").get()
        if furnished and "meuble" in furnished.lower():
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "+ 32 2 736 77 44")
        item_loader.add_value("landlord_email", "info@immoschmidt.be")

        yield item_loader.load_item()
