# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re
import dateparser

class MySpider(Spider):
    name = 'lachaumiere_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "http://www.la-chaumiere.fr/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=1&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&keywords=", "property_type": "apartment"},
	        {"url": "http://www.la-chaumiere.fr/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=UNIQUE&C_27=2&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&keywords=", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@id='listing_bien']/article[not(contains(.,'Loue'))]//a[./h2]/@href").extract():
            follow_url = response.urljoin(item.lstrip(".."))
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//ul//a[@class='page_suivante']/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "La_Chaumiere_PySpider_france")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("search_id=")[1].split("&")[0])
        
        title = response.xpath("//h1[@class='text-uppercase']/text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//span[contains(@class,'alur_location_ville')]//text()").get()
        if address:
            city = address.strip().split(" ")[-1]
            zipcode = address.strip().split(" ")[0]
            item_loader.add_value("address",address)
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)
        else:
            item_loader.add_value("address","MAINTENON")
            item_loader.add_value("city","MAINTENON")

        square_meters = response.xpath("//td[.='Surface']/following-sibling::td/text()").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", math.ceil(int(float(square_meters))))
        
        room_count = response.xpath("//td[contains(.,'Chambres')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//td[contains(.,'pièce')]/following-sibling::td/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
                
        bathroom_count = response.xpath("//td[contains(.,'Salle')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//div[contains(@class,'formatted')]/span[contains(.,'Loyer')]/text()").get()
        if rent:
            price = rent.split("€")[0].split("Loyer")[1].strip().replace("\xa0","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        else:
            rent = response.xpath("//div[contains(@class,'display')]//span[contains(.,'Loyer')]/text()").get()
            price = rent.split("€")[0].split("Loyer")[1].strip().replace("\xa0","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
            
        desc = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//a/@data-gallery/parent::a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor = response.xpath("//td[contains(.,'Etage')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        available_date = response.xpath("//td[contains(.,'Disponibi')]/following-sibling::td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//td[contains(.,'Garantie')]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("EUR")[0].strip())
        
        utilities = response.xpath("//td[contains(.,'sur charges')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("EUR")[0].strip())
        
        elevator = response.xpath("//td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if "Oui" in elevator:
                item_loader.add_value("elevator", True)
        
        parking = response.xpath("//td[contains(.,'parking')]/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        energy_label = response.xpath("//img[contains(@src,'DPE_')]/@src").get()
        if energy_label:
            energy_label = energy_label.split("DPE_")[1].split("_")[0]
            item_loader.add_value("energy_label", energy_label)
        
        item_loader.add_value("landlord_name", "La Chaumiere")
        item_loader.add_value("landlord_phone", "02.37.23.11.55")
        item_loader.add_value("landlord_email", "agency@la-chaumiere.fr")
        
        yield item_loader.load_item()