# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
import re
import dateparser
class MySpider(Spider):
    name = 'immobiliare_toulouse_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Immobiliare_Toulouse_PySpider_france'
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
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item, callback=self.parse, meta={'property_type': url.get('property_type')})

    def parse(self, response):

        for item in response.xpath("//div[@class='bloc-annonce-liste']/a[1]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Immobiliare_Toulouse_PySpider_france")  
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))       
        external_id = response.xpath("//link[@rel='canonical']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('programmes/')[-1])
        room_count = response.xpath("//div[@class='col-xs-12']//li[contains(.,'Pièces ')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[@class='col-xs-12']//li[contains(.,'Chambres ')]/strong/text()")
        bathroom_count = response.xpath("//li[contains(.,'Salles d')]/strong/text()[.!='0']").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        address = response.xpath("//h1/span//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(" - ")[0].strip())
        square_meters = response.xpath("//li[contains(.,'Surface habitable')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0])
        parking = " ".join(response.xpath("//li[contains(.,'Parking')]/strong/text()").getall())
        if parking:
            if "oui" in parking.lower() or "1" in parking.lower():
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        terrace = response.xpath("//div[@class='info-content']//li[contains(.,'Terrasse')]/strong/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//div[@class='info-content']//li[contains(.,'Balcon')]/strong/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[@class='info-content']//li[contains(.,'Elevator')]/strong/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        energy_label = response.xpath("substring-after(//div[contains(@class,'bloc-energie')]/@class,'note-')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())
        swimming_pool = response.xpath("//div[@class='info-content']//li[contains(.,'Piscine')]/strong/text()").get()
        if swimming_pool:
            if "oui" in swimming_pool.lower() or "1" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
            else:
                item_loader.add_value("swimming_pool", False)
        description = " ".join(response.xpath("//div[@class='bloc-content']/div[contains(.,'Description')]/following-sibling::div//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//a[@class='photo-annonce']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        available_date = response.xpath("substring-after(//div[@class='bloc-content']//text()[contains(.,'DISPONIBLE LE ')],'DISPONIBLE LE ')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = response.xpath("//li[contains(.,'Loyer mensuel')]/strong/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))
        utilities = response.xpath("//li/span[contains(.,' charges')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ",""))
        script_map = response.xpath("//script[contains(.,'programCoordinates = [')]/text()").get()
        if script_map:
            latlng = script_map.split("programCoordinates = [")[1].split("]")[0]
            item_loader.add_value("longitude", latlng.split(",")[0].strip())
            item_loader.add_value("latitude", latlng.split(",")[1].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='profil-commercial']/div/p[1]/text()")
        item_loader.add_value("landlord_phone", "05.61.21.00.83")
        yield item_loader.load_item()