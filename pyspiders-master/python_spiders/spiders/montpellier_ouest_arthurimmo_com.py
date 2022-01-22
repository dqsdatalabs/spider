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
    name = 'montpellier_ouest_arthurimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Montpellier_Ouest_Arthurimmo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.montpellier-ouest-arthurimmo.com/recherche,basic.htm?transactions=louer",
                ],

            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='w-full flex flex-col items-start']//h2//a//@href").getall():
            yield Request(
                item,
                callback=self.populate_item
            )
    

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("normalize-space(//h1/text())").get()
        if title:
            item_loader.add_value("title", title)
            if "(" in title:
                item_loader.add_value("zipcode", title.split("(")[1].split(")")[0])
        property_type=item_loader.get_output_value("title")
        if property_type:
            item_loader.add_value("property_type",get_p_type_string(property_type))
            if "Bureau" in property_type or "Parking" in property_type:return 
        address =item_loader.get_output_value("title")
        if address:
            item_loader.add_value("address", address.split("à")[1].strip())
            item_loader.add_value("city", address.split("à")[1].strip())
        zipcode=response.xpath("//title//text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split("(")[1].split(")")[0])
        rent = response.xpath("//div//text()[contains(.,'mois')]").get()
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].replace("\n","").strip())
        item_loader.add_value("currency", "EUR")
        external_id = response.xpath("//div[@class='text-gray-800']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("Référence")[1].split("-")[0].replace("\n","").strip())     
        square_meters = response.xpath("//li[contains(.,'Surface')]/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters)))       
        room_count = response.xpath("//div[.='Nombre de pièces']/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        bathroom_count = response.xpath("//div[contains(.,'Nombre de salles d')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())     
        deposit = response.xpath("//p[@class='detail-desc-txt']//text()[contains(.,'Dépôt')]").get()
        if deposit:
            deposit = deposit.split("garantie")[1].split("euro")[0].replace(":","").replace(" ","")
            item_loader.add_value("deposit", deposit)
        utilities = "".join(response.xpath("//text()[contains(.,'Dont provision sur charges')]").extract())
        if utilities:
            utilities = utilities.split(":")[-1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        parking = response.xpath("//div[.='Nb parkings']").get()
        if parking:
            item_loader.add_value("parking", True)
        description = " ".join(response.xpath("//p[@x-init='clamped = $el.scrollHeight > $el.clientHeight']//text()").getall())
        if description:
            description= re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())
        images = [x for x in response.xpath("//a[@class='absolute inset-0']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        terrace=response.xpath("//div[.='Terrasse']/following-sibling::div/text()").get()
        if terrace and terrace=="Oui":
            item_loader.add_value("terrace",True)

        item_loader.add_value("landlord_name", "ARTHURIMMO")
        item_loader.add_value("landlord_phone", "04 67 27 15 55")
        item_loader.add_value("landlord_email", "espaceimmo@arthurimmo.com")

        if not item_loader.get_collected_values("deposit"):
            deposit = response.xpath("//text()[contains(.,'Dépôt de garantie')]").get()
            if deposit: item_loader.add_value("deposit", "".join(filter(str.isnumeric, deposit.split(":")[-1].strip())))
        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None