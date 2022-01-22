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
    name = 'cvimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://cvimmobilier.fr/advanced-search/?keyword=&status=location&type=appartement&bedrooms=&min-area=&min-price=&max-price=&bathrooms=&max-area=&min-price=&max-price=", 
                "property_type": "apartment"
            },
            {
                "url": "https://cvimmobilier.fr/advanced-search/?keyword=&status=location&type=duplex&bedrooms=&min-area=&min-price=&max-price=&bathrooms=&max-area=&min-price=&max-price=", 
                "property_type": "apartment"
            },
            {
                "url": "https://cvimmobilier.fr/advanced-search/?keyword=&status=location&type=maison&bedrooms=&min-area=&min-price=&max-price=&bathrooms=&max-area=&min-price=&max-price=", 
                "property_type": "house"
            },
            {
                "url": "https://cvimmobilier.fr/advanced-search/?keyword=&status=location&type=studio&bedrooms=&min-area=&min-price=&max-price=&bathrooms=&max-area=&min-price=&max-price=", 
                "property_type": "studio"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for url in response.xpath("//div[contains(@class,'body-right')]//a/@href").getall():
            yield Request(url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        next_button = response.xpath("//ul[@class='pagination']//a[@rel='Next']/@href").get()
        if next_button: 
            yield Request(response.urljoin(next_button), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cvimmobilier_PySpider_france")
        title = response.xpath("//div[@class='col-md-8']/h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        address = response.xpath("//div[@class='left-panel']/strong/text()").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", "".join(address.split(" ")[:-1]))
            item_loader.add_value("zipcode", address.split(" ")[-1])
    
        item_loader.add_xpath("bathroom_count", "//tr/td[.='Salle(s) de Bain(s)']/following-sibling::td[1]/text()")
        item_loader.add_xpath("rent_string", "//div[@class='left-panel']/p//strong/text()[contains(.,'€')]")
   
        description = " ".join(response.xpath("//div[@id='descriptionblockxs']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        external_id = response.xpath("//div[@class='left-panel']/p/text()[contains(.,'ref. ')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("ref. ")[-1].strip())
        floor = response.xpath("//tr/td[.='Etage']/following-sibling::td[1]/text()").get()
        if floor:
            item_loader.add_value("floor",floor.strip())
        
        available_date = response.xpath("//div[@id='descriptionblockxs']//text()[contains(.,'Disponible le ')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible le")[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        parking = response.xpath("//tr/td[.='Parking(s)']/following-sibling::td[1]/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        room_count = response.xpath("//tr/td[.='Chambres']/following-sibling::td[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//tr/td[.='Pièces']/following-sibling::td[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(" ")[0])
       
        energy_label = response.xpath("//h5/text()[contains(.,'DPE : ')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[-1].split("(")[0].strip())
        square_meters = response.xpath("//tr/td[.='Surface']/following-sibling::td[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        deposit = response.xpath("//tr/td[.='Dépôt de Garantie']/following-sibling::td[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//tr/td[.='Provision sur Charges']/following-sibling::td[1]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "CV IMMOBILIER")
        item_loader.add_value("landlord_phone", "02 32 40 22 28")
        item_loader.add_value("landlord_email", "cvimmobilier@wanadoo.fr")
        yield item_loader.load_item()