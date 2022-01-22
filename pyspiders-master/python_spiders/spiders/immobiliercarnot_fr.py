# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'immobiliercarnot_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "http://www.immobilier-carnot.fr/locations-06/"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='centre']/ul/li/ul/li"):
            for item2 in item.xpath("./a[contains(@href,'.php')]"):
                follow_url = response.urljoin(item2.xpath("./@href").get())
                prop_type = item2.xpath(".//text()").get()
                property_type = ""
                if "appartement" in prop_type.lower():
                    property_type = "apartment"
                elif "maison" in prop_type.lower():
                    property_type = "house"
                elif "studios" in prop_type.lower():
                    property_type = "apartment"
                elif "duplex" in prop_type.lower():
                    property_type = "apartment"
                elif "villas" in prop_type.lower():
                    property_type = "house"
                elif "rooms" in prop_type.lower():
                    property_type = "apartment"
                if property_type != "":
                    yield Request(follow_url, callback=self.parse2, meta={'property_type': property_type})
    def parse2(self, response): 
        
        for item in response.xpath("//p[@class='droite']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get("property_type")})
        pagination = response.xpath("//a[./img[contains(@alt,'suivante')]]/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse2, meta={'property_type': response.meta.get("property_type")})
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
      
        item_loader.add_css("title", "h1")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Immobiliercarnot_fr_PySpider_"+ self.country + "_" + self.locale)
        rent =  "".join(response.xpath("//tr[th[. ='Prix :']]/td//text()[not(contains(.,'NC'))]").extract())       
        if rent:
            item_loader.add_value("rent_string", rent)

        external_id =  "".join(response.xpath("//caption/strong[contains(.,'Réf. : AC-G4136')]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1])

        meters =  ".".join(response.xpath("//tr[th[.='Surface (m²) :']]/td/text()").extract())
        if meters:
            item_loader.add_value("square_meters", meters)

        room_count =  ".".join(response.xpath("//tr[th[.='Nombre de pièces :']]/td/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count)

        address =  ".".join(response.xpath("//tr[th[.='Ville :']]/td/text()").extract())
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

        desc = "".join(response.xpath("//div[@id='prestation']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@id='prestation']//img/@src[not(contains(.,'gif'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//tr[th[.='Garage(s) :']]/td/text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            item_loader.add_value("parking", False)

        parking = response.xpath("//tr[th[.='Piscine : ']]/td/text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("swimming_pool", True)
        else:
            item_loader.add_value("swimming_pool", False)

        item_loader.add_value("landlord_phone", "04 93 35 97 80")
        item_loader.add_value("landlord_name", "Agence Carnot")
        yield item_loader.load_item()