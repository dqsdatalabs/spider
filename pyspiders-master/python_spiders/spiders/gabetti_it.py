# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from enum import EnumMeta
import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'gabetti_it'
    external_source = "Gabetti_PySpider_italy"
    execution_type='testing'
    country='italy'
    locale='it'
    start_urls = ['https://www.gabetti.it/casa/affitto/provincia-rm']  # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "appartamento",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "casa-indipendente",
                    "rustico-cascina",
                    "villa"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=self.start_urls[0],
                    dont_filter=True,
                    callback=self.jump,
                    meta={'property_type': url.get('property_type'), "type": item}
                )
    
    def jump(self, response):
        url_type = response.meta.get('type')
        cities = response.xpath("//select[@title='Regione']/option")
        for i in cities:
            f_url = f"https://www.gabetti.it/casa/affitto/{i.xpath('.//@value').get()}/{url_type}"
            yield Request(
                f_url,
                callback=self.parse,
                meta={
                    "property_type": response.meta.get('property_type'),
                    "type": url_type,
                }
            )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        for item in response.xpath("//div[@id='real_estates_mini']//div[contains(@class,'real-estate-')]"):
            follow_url = response.urljoin(item.xpath("./div//a[contains(@class,'link')]/@href").get())
            yield Request(
                follow_url, 
                callback=self.populate_item, 
                meta={
                    "property_type": response.meta.get('property_type'),
                    "url_type": response.meta.get('type'),
                }
            )
        
        next_page = response.xpath("substring-before(//a[@rel='next']/@href,'#')").get()
        if next_page:
            yield Request(
                response.urljoin(next_page), 
                callback=self.parse, 
                meta={
                    "page": page+1, 
                    "property_type": response.meta.get('property_type'),
                    "url_type": response.meta.get('type'),
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("title", "//span[contains(@class,'box-desc')]/text()")
        
        city = response.xpath("(//div[span[contains(.,'citt')]]/span[2]/text())[1]").get()
        if city:
            item_loader.add_value("city", city)
        
        address = response.xpath("//div[span[contains(.,'indirizzo')]]/span[2]/text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            if city:
                item_loader.add_value("address", city)
                
        rent = response.xpath("//div[span[contains(.,'Prezzo')]]/span[2]/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        bathroom_count = response.xpath("//div[span[contains(.,'bagni')]]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_xpath("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//span[contains(@class,'square')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//span[contains(@class,'room')]/text()").get()
        if room_count and int(room_count)<50:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//span[contains(.,'camere')]/following-sibling::span/text()").get()
            item_loader.add_xpath("room_count", room_count)        

        description = response.xpath("//div[contains(@class,'__description')]/text()").get()
        if description:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', description.strip()))
        
        item_loader.add_xpath("external_id", "//div[span[contains(.,'codice')]]/span[2]/text()")
        
        elevator = response.xpath("//div[span[contains(.,'ascensore')]]/span[2]/text()").get()
        if elevator and "si" in elevator.lower():
            item_loader.add_value("elevator", True)
            
        utilities = response.xpath("//div[span[contains(.,'condominiali')]]/span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
            
        energy_label = response.xpath("//div[span[contains(.,'energetica')]]/span[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.lower())
        
        item_loader.add_xpath("latitude", "//@data-lat")
        item_loader.add_xpath("longitude", "//@data-lng")
        
        images = [x for x in response.xpath("//div[@class='image_list_main_image']//@src | //a[contains(@class,'real-estate-detail-slide__link')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = response.xpath("//dd[@class='agency-contacts__info-highlight agency-contacts__info-upcase']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Gabetti")
        landlord_email = response.xpath("//a/@href[contains(.,'mailto')]/parent::a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        landlord_phone = response.xpath("//a/@href[contains(.,'tel')]/parent::a/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
 
        yield item_loader.load_item()