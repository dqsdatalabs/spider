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
from python_spiders.helper import ItemClear

class MySpider(Spider):
    name = 'normanparker57_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Normanparker57_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "type" : "Appartement|1",
                "property_type" : "apartment"
            },
            {
                "type" : " Maison|2",
                "property_type" : "house"
            },
            
        ]
        for url in start_urls:
            r_type = str(url.get("type"))
            formdata = {
                "search-form-97408[search][category]": "Location|2",
                "search-form-97408[search][type][]": r_type,
                "search-form-97408[search][price]": "",
                "search-form-97408[search][area_min]": "",
                "search-form-97408[submit]": "",
                "search-form-97408[search][order]": "",
            }
            
            yield FormRequest(url="https://normanparker57.fr/fr/recherche",callback=self.parse,formdata=formdata,meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='button']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//p[contains(.,'Réf.')]//text()").get()
        if external_id:
            external_id = external_id.split(".")[1].strip()
            item_loader.add_value("external_id", external_id)

        title =response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        square_meters = response.xpath("//p[contains(.,'m²')]//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//p[contains(.,'Mois')]/text()").get()
        if rent:
            rent = rent.strip().split("€")[0].replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("//text()[contains(.,'Dépôt de garantie')]").extract())
        if deposit:
            deposit = deposit.strip().split(":")[1].split("euro")[0]
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//li[contains(.,'Honoraires locataire')]/text()/following-sibling::span//text()").get()
        if utilities:
            utilities = utilities.strip().replace("€","").replace(" ","")
            item_loader.add_value("utilities", utilities)

        desc ="".join(response.xpath("//p[@id='description']//text()").getall())
        if desc:

            item_loader.add_value("description", desc)

        room_count = response.xpath("//p[contains(.,'pièces')]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        images = [x for x in response.xpath("//div/a/img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        parking=response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)
        terrace=response.xpath("//li[contains(.,'Terrain')]").get()
        if terrace:
            item_loader.add_value("terrace","True")

        # furnished = response.xpath("//span[contains(.,'MeubléNon ')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        # if furnished:
        #     item_loader.add_value("furnished", True)

        # elevator = response.xpath("//span[contains(.,'Ascenseur')]//following-sibling::span//text()[contains(.,'OUI')]").get()
        # if elevator:
        #     item_loader.add_value("elevator", True)

        # floor = response.xpath("//span[contains(.,'Etage')]//following-sibling::span//text()").get()
        # if floor:
        #     item_loader.add_value("floor", floor.strip())

        # latitude_longitude = response.xpath("//script[contains(.,'center')]//text()").get()
        # if latitude_longitude:
        #     latitude = latitude_longitude.split('lat :')[1].split(',')[0].strip()
        #     longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()      
        #     item_loader.add_value("longitude", longitude)
        #     item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'commercialcoord')]//p[1]//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//p[contains(@class,'nego')]//a//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        item_loader.add_value("landlord_phone", "03 82 56 38 15")
      
        yield item_loader.load_item()