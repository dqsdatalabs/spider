# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import itemloaders
from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'valorecasa_com'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Valorecasa_PySpider_italy"
    custom_settings = {
        "PROXY_ON": True
    }
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
    }
    def start_requests(self):
        start_urls = [
            {
                "url" : ["http://www.valorecasa.com/valore/italiano/ricerca_avanzata.asp?contratto=affitto"],
            } 
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(
                            url=item,
                            callback=self.parse,
                            headers=self.headers,
                        )
    def parse(self, response):
        for item in response.xpath("//div[@align='right']/strong"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item)    

        next_page = response.xpath("//td[@align='center']/a[contains(.,'Successiva')]/@href").get()
        if next_page:
            url = response.urljoin(next_page)
            yield Request(url, callback=self.parse)
        
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        property_type = response.xpath("//tr/td[contains(.,'Tipo Immobile')]/following-sibling::td[1]/text()").get()
        if property_type:   
            if property_type and "appartamento" in property_type.lower():  
                item_loader.add_value("property_type", "apartment")
            elif property_type and "attico" in property_type.lower():
                item_loader.add_value("property_type", "apartment")
            elif property_type and "casa indipendente" in property_type.lower():
                item_loader.add_value("property_type", "house")
            elif property_type and "villa" in property_type.lower():
                item_loader.add_value("property_type", "house")
            elif property_type and "villetta" in property_type.lower():
                item_loader.add_value("property_type", "house")
            else: 
                return
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split('=')[-1])
        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
        
        desc = "".join(response.xpath("//tr[@valign='top']/td/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc)

        city = response.xpath("(//td/b[contains(.,'Località')]/following-sibling::text())[1]").get()
        if city:
            item_loader.add_value("city", city.strip())
        zipcode = response.xpath("//b[contains(.,'Cod.')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(' ')[-1])
        address = response.xpath("(//td/b[contains(.,'Zona')]/following-sibling::text())[1]").get()
        if address:
            item_loader.add_value("address", zipcode.split(' ')[-1] + " " + city.strip() + ", " + address.strip())

        rent = response.xpath("//td/b[contains(.,'Prezzo')]/following-sibling::text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[-1].strip().split(',')[0])
        item_loader.add_value("currency", "EUR")
        square_meters = response.xpath("//td/b[contains(.,'Mq')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        room_count = response.xpath("//tr/td[contains(.,'Numero Vani')]/following-sibling::td[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//tr/td[contains(.,'Numero Bagni')]/following-sibling::td[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        images = [x for x in response.xpath("//td[1]/a//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        furnished = response.xpath("//tr/td[contains(.,'Arredato')]/following-sibling::td[1]/text()").get()
        if "si" in furnished.lower():
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr/td[contains(.,'Ascensore')]/following-sibling::td[1]/text()").get()
        if "si" in elevator.lower():
            item_loader.add_value("elevator", True)
        parking = response.xpath("//tr/td[contains(.,'Posti Auto')]/following-sibling::td[1]/text()").get()
        if "si" in parking.lower():
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "ValoreCASA")
        item_loader.add_value("landlord_phone", "080.5559956")
        item_loader.add_value("landlord_email", "immobiliare@valorecasa.com")
        
        yield item_loader.load_item()
