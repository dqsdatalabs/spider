# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'lemontorge_fr'   
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lemontorge.fr/advanced-search/?type=appartement&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lemontorge.fr/advanced-search/?type=maison&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='table-cell']//a[@class='hover-effect']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//ul[@class='pagination']//a[@rel='Next']/@href").get()
        if next_button: 
            yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Lemontorge_PySpider_france")
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("external_id", "//span[span[.='Référence : ']]/span[2]/text()")
        item_loader.add_xpath("rent_string", "//span[@class='item-price']/text()")
        item_loader.add_xpath("utilities", "//div[strong[.='Charges : ']]/label/text()")
        deposit = response.xpath("//div[strong[.='Dépôt de Garantie : ']]/label/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(",")[0])
        square_meters = response.xpath("//div[strong[.='Surface : ']]/label/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())

        description = "".join(response.xpath("//div[@id='description']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description)

        city = response.xpath("//li[@class='detail-city']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        zipcode = response.xpath("//li[@class='detail-zip']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        item_loader.add_xpath("floor", "//div[strong[.='Etage : ']]/label/text()")
        item_loader.add_xpath("address", "//address[@class='property-address']/text()")
        item_loader.add_xpath("bathroom_count", "//div[strong[.='Salle(s) de Bain(s) : ']]/label/text()")

        room_count = response.xpath("//div[strong[.='Chambres : ']]/label/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        energy_label = response.xpath("//h5[contains(.,'DPE :')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("DPE :")[1].split("(")[0])

        images = [x for x in response.xpath("//div[@class='gallery-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[strong[.='Garages : ']]/label/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath("//ul/li[contains(.,'Ascenseur')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        terrace = response.xpath("//ul/li[contains(.,'Terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        furnished = response.xpath("//ul/li[contains(.,'meublé')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        balcony = response.xpath("//ul/li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        landlord_name = response.xpath("//div[@class='media agent-media']//dd[i[@class='fa fa-user']]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone = response.xpath("//div[@class='media agent-media']//dd[span/i[@class='fa fa-phone']]//a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "04-76-87-89-41")
        item_loader.add_value("landlord_email", "agence@lemontorge.fr")
        available_date= response.xpath("//div[@id='description']/p//text()[contains(.,'DISPONIBLE LE') or contains(.,'Disponible le')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.upper().split(" LE")[-1], date_formats=["%m-%d-%Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        yield item_loader.load_item()