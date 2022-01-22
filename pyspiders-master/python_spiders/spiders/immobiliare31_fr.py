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

class MySpider(Spider):
    name = 'immobiliare31_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.immobiliare31.fr/programmes?search%5Bcategory%5D=RENTAL&search%5Btype%5D%5B%5D=APARTMENT",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.immobiliare31.fr/programmes?search%5Bcategory%5D=RENTAL&search%5Btype%5D%5B%5D=HOUSE",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='bloc-annonce-liste']/a[not(@class)]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split('?')[0])
        item_loader.add_value("external_id", response.url.split('?')[0].split("/")[-1])
        item_loader.add_value("external_source", "Immobiliare31_PySpider_france")      
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(" - ")[0].strip())
 
        room_count = response.xpath("//li[contains(.,'Chambres')]/strong/text()[.!='-']").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//li[contains(.,'Pièces')]/strong/text()[.!='-']")

        item_loader.add_xpath("bathroom_count", "//li[contains(.,'Salles d')]/strong/text()[.!='0']")     
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        energy_label = response.xpath("//div[contains(@class,'bloc-energie')]/strong/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
      
        description = " ".join(response.xpath("//div[//h3[.='Description']]/div[@class='info-content']/text()[normalize-space()]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        script_map = response.xpath("//script[contains(.,'programCoordinates = [')]/text()").get()
        if script_map:
            latlng = script_map.split("programCoordinates = [")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[1].strip())
            item_loader.add_value("longitude", latlng.split(",")[0].strip())
    
        elevator = response.xpath("//div[@class='sidebar']//p/strong[.='Ascenseur']/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        terrace = response.xpath("//div[@class='sidebar']//p/strong[.='Terrasse']/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[@class='sidebar']//p/strong[.='Balcon']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        swimming_pool = response.xpath("//li[contains(.,'Piscine')]/strong/text()").get()
        if swimming_pool:
            if swimming_pool.strip() =="0":
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)
        parking = " ".join(response.xpath("//li[contains(.,'Parking')]/strong/text()").getall())
        if parking:
            if "oui" in parking.lower() or "1" in parking.lower():
                item_loader.add_value("parking", True)
            elif "non" in parking.lower() or "0" in parking.lower():
                item_loader.add_value("parking", False)
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'popup-gallery')]//a[@class='photo-annonce']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        available_date = response.xpath("//div[//h3[.='Description']]/div[@class='info-content']/text()[contains(.,'DISPONIBLE LE')]").get()
        if available_date:
            available_date = available_date.split("DISPONIBLE LE")[-1].strip().split(" ")[0]
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = response.xpath("//div[@class='prix']/p//text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//li/span[contains(.,' pour charges')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        item_loader.add_xpath("landlord_name", "//div[@class='nom-contact']/p[1]/text()")
        item_loader.add_value("landlord_phone", "05.61.21.00.83")
        yield item_loader.load_item()



def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label